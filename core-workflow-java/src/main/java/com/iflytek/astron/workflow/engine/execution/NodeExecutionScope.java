package com.iflytek.astron.workflow.engine.execution;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.MsgTypeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeStatusEnum;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder.EngineContext;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation.AttemptStatus;
import com.iflytek.astron.workflow.engine.execution.NodeActivation.Status;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatItem;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatMessage;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.AsyncUtil;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * 单个节点「一次激活」的执行作用域。
 *
 * <p>它封装一次节点执行所需的全部上下文（入参快照、变量快照、对话历史、每次物理尝试的状态），
 * 并以 {@link NodeState} 向节点执行器暴露受控的副作用入口：事件回调、历史读写、输入存在性判断，
 * 以及可感知的取消/超时状态。
 *
 * <p>root 实例对应一次激活并持有真实状态；每次物理尝试（含重试）都会派生一个 child 实例，
 * 字段共享 root，只额外绑定自己的 attempt。
 */
@Slf4j
public final class NodeExecutionScope {
    // 所属运行的上下文，同时是本类所有状态变更的锁对象
    private final WorkflowRunContext run;
    // 节点定义副本，避免与图定义或其他执行互相污染
    private final Node node;
    @Getter
    private final String executionId;
    @Getter
    private final String activationId;
    private final int activationNo;
    private final Map<String, Object> inputs;
    private final Map<String, NodeActivation.InputOrigin> inputOrigins;
    // 激活时冻结的变量池与历史快照，保证一次执行内部看到一致的数据视图
    private final Map<String, Map<String, Object>> variables;
    private final List<ChatItem> initialHistory;
    private final Clock clock;
    // root 持有真实状态；child 通过这两个字段回指 root 与本次尝试
    private final NodeExecutionScope root;
    private final AttemptState attempt;
    // 一次激活可能经历多次物理尝试（重试），按发生顺序记录
    private final List<AttemptState> attempts = new ArrayList<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile Status status = Status.READY;
    private volatile AttemptState currentAttempt;
    private NodeRunResult result;
    private List<ChatItem> history;

    // root 作用域：由 NodeExecutionBridge.prepare 在一次激活时创建
    NodeExecutionScope(WorkflowRunContext run, Node node, String activationId, int activationNo,
                       Map<String, Object> inputs, Map<String, NodeActivation.InputOrigin> inputOrigins,
                       Map<String, Map<String, Object>> variables, Clock clock) {
        this.run = run;
        this.node = node;
        this.executionId = UUID.randomUUID().toString();
        this.activationId = activationId;
        this.activationNo = activationNo;
        this.inputs = JsonSnapshot.copy(inputs);
        this.inputOrigins = Map.copyOf(inputOrigins);
        this.variables = variables;
        this.initialHistory = run.history(node.getId());
        this.clock = clock;
        this.root = this;
        this.attempt = null;
    }

    // child 作用域：每次物理尝试派生一个，自身状态全部委托给 root
    private NodeExecutionScope(NodeExecutionScope root, Node node, AttemptState attempt) {
        this.run = root.run;
        this.node = node;
        this.executionId = root.executionId;
        this.activationId = root.activationId;
        this.activationNo = root.activationNo;
        this.inputs = root.inputs;
        this.inputOrigins = root.inputOrigins;
        this.variables = root.variables;
        this.initialHistory = root.initialHistory;
        this.clock = root.clock;
        this.root = root;
        this.attempt = attempt;
    }

    public Map<String, Object> inputs() {
        return JsonSnapshot.mutable(inputs);
    }

    /**
     * 是否还允许产生副作用：run 未超时、未取消、本激活处于运行中，
     * 且当前物理尝试仍在自身的超时窗口内。
     */
    public boolean isActive() {
        return run.executionAllowed() && !root.cancelled.get() && root.status == Status.RUNNING && (attempt == null || attempt.status == AttemptStatus.RUNNING
                && clock.millis() < attempt.deadline);
    }

    /**
     * 入参是否「真的有值」。只看 key 存在不够，上游节点没产出该字段时也应视为缺失。
     */
    public boolean inputPresent(String name) {
        NodeActivation.InputOrigin origin = inputOrigins.get(name);
        return origin == null ? inputs.containsKey(name) : origin.present();
    }

    /**
     * 发起一次物理尝试并同步等待结果：分配 attempt、按配置施加单次超时、执行节点；
     * 执行异常时交由 failure 回调转换成业务结果（例如走失败分支）。
     */
    public NodeRunResult invoke(NodeState state, RetryConfig retry, Invocation invocation,
                                BiFunction<NodeState, NodeCustomException, NodeRunResult> failure) {
        AttemptState current;
        NodeExecutionScope child;
        // 先补投上一次尝试积攒的终态事件，保证事件顺序与状态变化一致
        root.publishTerminals();
        synchronized (run) {
            // 已取消或已超时：不再执行，直接返回中断结果
            if (!isActive()) {
                return interrupted();
            }
            // 本次尝试的截止时间由节点重试配置决定（未配置则不设限）
            long startedAt = clock.millis();
            long timeout = retry != null && retry.timeOutEnabled() ? retry.toMillis() : Long.MAX_VALUE;
            long deadline = timeout > Long.MAX_VALUE - startedAt ? Long.MAX_VALUE : startedAt + timeout;
            current = new AttemptState(root.attempts.size() + 1, startedAt, deadline, initialHistory);
            root.attempts.add(current);
            root.currentAttempt = current;
            child = new NodeExecutionScope(root, copyNode(node), current);
            child.node.getExecutedCount().set(current.attemptNo);
        }
        // 每次尝试都基于激活时冻结的变量快照新建私有变量池
        VariablePool privatePool = pool();
        // 回调包一层门控：本尝试失效后，节点产生的消息与历史写入都会被静默丢弃
        WorkflowMsgCallback guarded = run.callback().guarded(child::gate, child::deferTerminal, child::isActive);
        NodeState call = new NodeState(child.node, privatePool, guarded, child);
        NodeRunResult returned;
        boolean timedOut = false;
        try {
            // 配了超时就限时执行，否则直接同步调用
            if (retry != null && retry.timeOutEnabled()) {
                returned = AsyncUtil.callWithTimeLimit(retry.toMillis(), TimeUnit.MILLISECONDS,
                        () -> child.withContext(call, invocation));
            } else {
                returned = child.withContext(call, invocation);
            }
            returned = JsonSnapshot.result(Objects.requireNonNull(returned, "Node executor returned no result"));
            synchronized (run) {
                // executor 正常返回也可能已越过时限，这里补判一次
                if (clock.millis() >= current.deadline) {
                    throw new TimeoutException("Node attempt deadline exceeded");
                }
                current.accepting = false;
            }
        } catch (Exception error) {
            // 超时与中断统一按超时处理，其余异常保留原始业务错误码
            timedOut = error instanceof TimeoutException || error instanceof InterruptedException;
            synchronized (run) {
                current.accepting = false;
                current.terminals.clear();
            }
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Throwable cause = error instanceof ExecutionException && error.getCause() != null ? error.getCause() : error;
            NodeCustomException business = timedOut ? new NodeCustomException(ErrorCode.TIMEOUT_ERROR)
                    : cause instanceof NodeCustomException custom ? custom
                    : new NodeCustomException(ErrorCode.NODE_RUN_ERROR, cause.getMessage());
            // 失败处理阶段用单独的门控，避免上一次尝试的迟到事件串台
            WorkflowMsgCallback control = run.callback().guarded(action -> controlGate(current, action),
                    action -> deferControlTerminal(current, action),
                    () -> controlActive(current));
            returned = failure.apply(new NodeState(copyNode(node), pool(), control, child), business);
        }
        synchronized (run) {
            // 执行期间被取消：结果作废，按中断返回
            if (!run.executionAllowed() || root.cancelled.get() || root.status != Status.RUNNING || current.status == AttemptStatus.CANCELLED) {
                return interrupted();
            }
            NodeRunResult fixed = JsonSnapshot.result(returned);
            fixed.setInputs(inputs);
            if (fixed.getStatus() == null) {
                fixed.setStatus(NodeExecStatusEnum.SUCCESS);
            }
            current.status = timedOut ? AttemptStatus.TIMED_OUT
                    : fixed.getStatus().isSuccess() ? AttemptStatus.SUCCEEDED : AttemptStatus.FAILED;
            current.finishedAt = clock.millis();
            current.errorCode = fixed.getError() == null ? null : fixed.getError().getCode();
            current.error = fixed.getError() == null ? null : fixed.getError().getMessage();
            current.control = false;
            root.history = current.historyTouched ? LlmChatHistory.copyHistory(current.history) : null;
            return fixed;
        }
    }

    // ===== 对话历史读写：供 LLM 节点维护多轮上下文，写入前校验 chatId/nodeId 归属 =====
    public void newChat(String chatId, String nodeId) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            if (attempt.history.size() >= 10) {
                attempt.history.removeFirst();
            }
            attempt.history.add(new ChatItem(chatId, nodeId));
            attempt.historyTouched = true;
        });
    }

    public void addMessage(String chatId, String nodeId, MsgTypeEnum role, String content) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            if (attempt.history.isEmpty()) {
                attempt.history.add(new ChatItem(chatId, nodeId));
            }
            ChatItem item = attempt.history.getLast();
            ChatMessage message = new ChatMessage(role, content);
            switch (role) {
                case USER, SYSTEM -> item.userInputs().add(message);
                case THINKING -> item.llmThinking().add(message);
                case ASSISTANT -> item.llmResponses().add(message);
            }
            attempt.historyTouched = true;
        });
    }

    public List<ChatItem> getHistory(String chatId, String nodeId, int count) {
        synchronized (run) {
            requireHistoryIdentity(chatId, nodeId);
            List<ChatItem> items = attempt == null ? initialHistory : attempt.history;
            return LlmChatHistory.copyHistory(items.subList(Math.max(0, items.size() - count), items.size()));
        }
    }

    public void clearHistory(String chatId, String nodeId) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            attempt.history.clear();
            attempt.historyTouched = true;
        });
    }

    // 交给节点执行器的 NodeState：绑定本作用域，回调已带门控
    NodeState state() {
        return new NodeState(node, pool(), run.callback().guarded(this::gate, this::isActive), this);
    }

    Node node() {
        return node;
    }

    Status status() {
        return root.status;
    }

    // READY -> RUNNING，由 NodeExecutionBridge 在 claim 成功后调用
    void start() {
        status = Status.RUNNING;
        node.setStatus(NodeStatusEnum.RUNNING);
    }

    // 取消本次激活：关闭所有尝试的副作用通道，并把在跑的尝试标记为已取消
    void cancel() {
        if (status != Status.READY && status != Status.RUNNING) {
            return;
        }
        cancelled.set(true);
        status = Status.CANCELLED;
        for (AttemptState current : attempts) {
            current.accepting = false;
            current.control = false;
            current.terminals.clear();
            if (current.status == AttemptStatus.RUNNING) {
                current.status = AttemptStatus.CANCELLED;
                current.finishedAt = clock.millis();
            }
        }
    }

    // 投递本尝试积攒的终态事件（节点完成/失败等）；单个事件投递失败不影响主流程
    void publishTerminals() {
        List<Runnable> terminal;
        synchronized (run) {
            if (attempts.isEmpty() || cancelled.get() || !run.executionAllowed()) {
                return;
            }
            terminal = new ArrayList<>(attempts.getLast().terminals);
            attempts.getLast().terminals.clear();
        }
        for (Runnable action : terminal) {
            if (cancelled.get() || !run.executionAllowed()) {
                break;
            }
            try {
                action.run();
            } catch (RuntimeException error) {
                log.warn("Cannot deliver committed node event for execution {}", executionId, error);
            }
        }
    }

    // 记录最终结果并收敛激活状态
    NodeRunResult complete(NodeRunResult returned) {
        result = JsonSnapshot.result(returned);
        result.setInputs(inputs);
        status = result.getStatus() == null || result.getStatus().isSuccess() ? Status.SUCCEEDED : Status.FAILED;
        node.setStatus(status == Status.SUCCEEDED ? NodeStatusEnum.SUCCESS : NodeStatusEnum.ERROR);
        return result;
    }

    // 收尾：把结果/历史写回 run（决定是否更新变量池），再投递终态事件
    void commit(NodeRunResult returned) {
        run.complete(root, returned, history);
        publishTerminals();
    }

    // 对外的不可变视图
    NodeActivation snapshot() {
        return new NodeActivation(run.getRunId(), executionId, activationId, node.getId(), activationNo, root.status,
                inputs, inputOrigins, result, attempts.stream().map(AttemptState::snapshot).toList());
    }

    static Node copyNode(Node source) {
        Node copy = new Node();
        copy.setId(source.getId());
        copy.setData(JSON.parseObject(JSON.toJSONString(source.getData()), NodeData.class));
        copy.init();
        return copy;
    }

    private VariablePool pool() {
        VariablePool pool = new VariablePool();
        pool.restore(variables);
        return pool;
    }

    // 绑定 ThreadLocal 的 EngineContext（flowId/chatId/sid/幂等键），并统计在途物理调用数
    private NodeRunResult withContext(NodeState state, Invocation invocation) throws Exception {
        if (!isActive()) {
            return interrupted();
        }
        EngineContext previous = EngineContextHolder.get();
        EngineContext context = new EngineContext();
        context.setFlowId(run.getFlowId());
        context.setChatId(run.getChatId());
        context.setSid(run.getSid());
        context.setCallback(state.callback());
        context.setIdempotencyKey(run.getRunId() + ":" + executionId);
        context.setNodeExecution(this);
        EngineContextHolder.set(context);
        run.enteredPhysicalAttempt();
        try {
            return invocation.call(state);
        } finally {
            run.exitedPhysicalAttempt();
            if (previous == null) {
                EngineContextHolder.remove();
            } else {
                EngineContextHolder.set(previous);
            }
        }
    }

    // 执行器直接抛异常时的兜底：把当前尝试记为 FAILED，并返回中断态结果
    NodeRunResult failedInvocation(NodeCustomException error) {
        synchronized (run) {
            if (!attempts.isEmpty()) {
                AttemptState current = attempts.getLast();
                if (current.status == AttemptStatus.RUNNING) {
                    current.status = AttemptStatus.FAILED;
                    current.finishedAt = clock.millis();
                    current.errorCode = error.getCode();
                    current.error = error.getMessage();
                    current.accepting = false;
                    current.control = false;
                    current.terminals.clear();
                }
            }
            NodeRunResult result = interrupted();
            result.setError(error);
            return result;
        }
    }

    // ===== 副作用门控：失效的尝试只能静默丢弃；终态事件先入队，提交后再统一投递 =====
    private void gate(Runnable action) {
        if (isActive() && (attempt == null || attempt.accepting)) {
            action.run();
        }
    }

    private void mutate(Runnable action) {
        synchronized (run) {
            gate(action);
        }
    }

    private void deferTerminal(Runnable action) {
        mutate(() -> attempt.terminals.add(action));
    }

    private boolean controlActive(AttemptState current) {
        return run.executionAllowed() && !root.cancelled.get() && root.status == Status.RUNNING && current.control && root.currentAttempt == current;
    }

    private void controlGate(AttemptState current, Runnable action) {
        if (controlActive(current)) {
            action.run();
        }
    }

    private void deferControlTerminal(AttemptState current, Runnable action) {
        synchronized (run) {
            if (controlActive(current)) {
                current.terminals.add(action);
            }
        }
    }

    private void requireHistoryIdentity(String chatId, String nodeId) {
        if (!run.getChatId().equals(chatId) || !node.getId().equals(nodeId)) {
            throw new IllegalArgumentException("History identity differs from node execution");
        }
    }

    private NodeRunResult interrupted() {
        NodeRunResult result = new NodeRunResult();
        result.setInputs(inputs);
        result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
        result.setError(new NodeCustomException(ErrorCode.INTERRUPTED_ERROR));
        return result;
    }

    @FunctionalInterface
    public interface Invocation {
        NodeRunResult call(NodeState state) throws Exception;
    }

    // 一次物理尝试（重试的第 N 次）的状态与事件缓冲
    private static final class AttemptState {
        private final String attemptId = UUID.randomUUID().toString();
        private final int attemptNo;
        private final long startedAt;
        private final long deadline;
        private final List<ChatItem> history;
        private final List<Runnable> terminals = new ArrayList<>();
        private volatile AttemptStatus status = AttemptStatus.RUNNING;
        private volatile boolean accepting = true;
        private volatile boolean control = true;
        private boolean historyTouched;
        private long finishedAt;
        private Integer errorCode;
        private String error;

        private AttemptState(int attemptNo, long startedAt, long deadline, List<ChatItem> history) {
            this.attemptNo = attemptNo;
            this.startedAt = startedAt;
            this.deadline = deadline;
            this.history = LlmChatHistory.copyHistory(history);
        }

        private NodeActivation.Attempt snapshot() {
            return new NodeActivation.Attempt(attemptId, attemptNo, status, startedAt, deadline, finishedAt, errorCode, error);
        }
    }
}
