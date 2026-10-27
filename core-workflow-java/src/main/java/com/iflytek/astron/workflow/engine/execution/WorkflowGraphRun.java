package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.Getter;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 一次工作流运行（run）的调度状态：控制令牌队列 + 运行状态 + 最终输出。
 *
 * <p>它不执行节点、也不改变图，只负责三件事：发放令牌（issue）、按序消费令牌（next）、
 * 以及在无活可干时收敛终态（finishIfIdle）。真正的推进由 {@link WorkflowGraphExecutor#tick} 驱动。
 */
public final class WorkflowGraphRun {
    // 本次 run 的全部可变业务状态（变量池、节点执行记录、对话历史）
    @Getter
    private final WorkflowRunContext context;
    // 资源配额与时间窗口
    @Getter
    private final WorkflowExecutionLimits limits;
    @Getter
    private final long startedAt;
    @Getter
    private final long deadline;
    // 编译后的只读图定义
    private final WorkflowGraph graph;
    private final ArrayDeque<Token> pending = new ArrayDeque<>();
    private final WorkflowTokenScheduler scheduler;
    // 所有发出过的令牌：replay 时据此校验令牌是否存在、内容是否被篡改
    private final Map<String, Token> issued = new HashMap<>();
    // 已消费的令牌 ID，保证一条令牌最多驱动一次执行
    private final Set<String> consumed = new HashSet<>();
    // 终态用 volatile：推进线程与等待方可能分属不同线程
    private volatile Status status = Status.RUNNING;
    private Map<String, Object> outputs = Map.of();
    private Integer errorCode;
    private String error;
    // 推进闸门：同一时刻只允许一个线程推进
    private boolean advancing;
    // 是否已经到达 END
    private boolean endReached;

    // 创建即向 START 发放第一条令牌（无来源执行、无来源边）
    WorkflowGraphRun(WorkflowGraph graph, WorkflowRunContext context, WorkflowExecutionLimits limits, long startedAt, long deadline) {
        this.graph = graph;
        this.scheduler = new WorkflowTokenScheduler(graph);
        this.context = context;
        this.limits = limits;
        this.startedAt = startedAt;
        this.deadline = deadline;
        issue(graph.startNodeId(), null, null);
    }

    public Status status() {
        return status;
    }

    public synchronized Map<String, Object> outputs() {
        return outputs;
    }

    public synchronized Integer errorCode() {
        return errorCode;
    }

    public synchronized String error() {
        return error;
    }

    public synchronized List<Token> pending() {
        return List.copyOf(pending);
    }

    public synchronized Set<String> consumedTokenIds() {
        return Set.copyOf(consumed);
    }

    public synchronized List<Token> arrivals(String activationId) {
        return scheduler.arrivals(activationId);
    }

    /**
     * 把一条已发出的令牌重新放回待办队列（重试/恢复场景使用）。
     * 只接受本 run 发出过且内容完全一致的令牌，防止伪造或篡改。
     */
    public synchronized void replay(Token token) {
        if (token == null || !token.equals(issued.get(token.id()))) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "Only an issued control token can be replayed");
        }
        if (status == Status.RUNNING) {
            pending.addLast(token);
        }
    }

    WorkflowGraph graph() {
        return graph;
    }

    // 抢推进权：run 已结束或已有线程在推进时返回 false（不阻塞，交给调用方稍后重试）
    synchronized boolean beginAdvance() {
        if (status != Status.RUNNING || advancing) {
            return false;
        }
        advancing = true;
        return true;
    }

    // 释放推进权，必须在 finally 中调用，否则 run 会永久卡住
    synchronized void endAdvance() {
        advancing = false;
    }

    synchronized Token next() {
        pending.removeIf(token -> consumed.contains(token.id()));
        if (pending.isEmpty()) {
            return null;
        }
        WorkflowTokenScheduler.Selection selected = scheduler.select(List.copyOf(pending));
        selected.arrivals().forEach(token -> consumed.add(token.id()));
        pending.removeIf(token -> consumed.contains(token.id()));
        return selected.token();
    }

    // 发放新令牌：登记到 issued（供 replay 校验），并追加到待办队列尾部
    synchronized void issue(String nodeId, String sourceExecutionId, Integer edgeIndex) {
        Token token = new Token(UUID.randomUUID().toString(), nodeId, sourceExecutionId, edgeIndex);
        scheduler.register(token);
        issued.put(token.id(), token);
        pending.addLast(token);
    }

    synchronized void issueSuccessors(Token source, String executionId, List<WorkflowGraph.Transition> edges) {
        for (WorkflowGraph.Transition edge : edges) {
            Token token = new Token(UUID.randomUUID().toString(), edge.target(), executionId, edge.edgeIndex());
            scheduler.registerSuccessor(token, source);
            issued.put(token.id(), token);
            pending.addLast(token);
        }
    }

    // 记录 END 的输出快照；此时还不算成功，要等 finishIfIdle 确认确实空闲
    synchronized void reachedEnd(NodeRunResult result) {
        endReached = true;
        outputs = JsonSnapshot.copy(result.getOutputs());
    }

    /**
     * 空闲收敛：只有「无待办令牌 + 无在跑节点 + 无在途物理调用」时才判定终态。
     * 已到 END 则成功，否则说明控制流断了（STALLED）。
     */
    synchronized void finishIfIdle(boolean hasPendingExecutions, int physicalAttempts) {
        pending.removeIf(token -> consumed.contains(token.id()));
        if (status != Status.RUNNING || !pending.isEmpty() || hasPendingExecutions || physicalAttempts != 0) {
            return;
        }
        if (endReached) {
            status = Status.SUCCEEDED;
        } else {
            fail(new NodeCustomException(ErrorCode.WORKFLOW_CONTROL_FLOW_STALLED, "No runnable work remains before END"));
        }
    }

    // 失败收敛：记录错误码/信息并清空待办，之后不再接受推进
    synchronized void fail(NodeCustomException failure) {
        if (status != Status.RUNNING) {
            return;
        }
        status = Status.FAILED;
        errorCode = failure.getCode();
        error = failure.getMessage();
        pending.clear();
    }

    // run 的终态；离开 RUNNING 后 beginAdvance 会一直返回 false
    public enum Status {
        RUNNING, SUCCEEDED, FAILED
    }

    /**
     * 控制令牌：一次「请执行 targetNodeId」的调度许可。
     *
     * @param id                令牌唯一标识，同时充当节点激活的 activationId（重放幂等的基础）
     * @param targetNodeId      要激活的节点
     * @param sourceExecutionId 产生该令牌的上游执行 ID；初始令牌为 null
     * @param edgeIndex         触发它的边在 DSL edges 中的下标；非边产生时为 null
     */
    public record Token(String id, String targetNodeId, String sourceExecutionId, Integer edgeIndex) {
    }
}
