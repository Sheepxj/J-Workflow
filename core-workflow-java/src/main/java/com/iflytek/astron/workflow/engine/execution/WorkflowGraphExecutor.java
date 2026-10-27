package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * 工作流执行器：图执行的驱动者与对外门面。
 *
 * <p>负责编译图、创建 run（{@link WorkflowGraphRun} + {@link WorkflowRunContext}）、
 * 在自旋循环里不断调用 {@link #tick} 推进节点，直到 run 收敛为成功或失败，
 * 并通过 callback 上报工作流开始/结束与节点事件。
 */
@Slf4j
@Component
public final class WorkflowGraphExecutor implements AutoCloseable {
    private final NodeExecutionBridge bridge;
    private final WorkflowGraphCompiler compiler;
    // 未显式指定时使用的默认配额
    private final WorkflowExecutionLimits defaults;
    private final Clock clock;
    // 节点执行线程池：tick 线程负责调度，节点实际跑在这里
    private final ExecutorService workers;

    @Autowired
    public WorkflowGraphExecutor(NodeExecutionBridge bridge,
                                 @Value("${workflow.control-flow.max-node-activations:100}") int maxNodeActivations,
                                 @Value("${workflow.control-flow.max-total-activations:10000}") int maxTotalActivations,
                                 @Value("${workflow.control-flow.max-duration-ms:300000}") long maxDurationMs) {
        this(bridge, new WorkflowExecutionLimits(maxNodeActivations, maxTotalActivations, maxDurationMs), Clock.systemUTC(),
                new ThreadPoolExecutor(4, 8, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256),
                        Thread.ofPlatform().daemon(true).name("workflow-node-", 0).factory(), new ThreadPoolExecutor.AbortPolicy()));
    }

    public WorkflowGraphExecutor(NodeExecutionBridge bridge, WorkflowExecutionLimits defaults, Clock clock, ExecutorService workers) {
        this.bridge = bridge;
        this.compiler = new WorkflowGraphCompiler(bridge);
        this.defaults = defaults;
        this.clock = clock;
        this.workers = workers;
    }

    // 仅编译/校验图，不启动运行；结果可缓存后重复 start
    public WorkflowGraph prepare(WorkflowDSL definition) {
        return compiler.compile(definition);
    }

    public WorkflowGraphRun start(WorkflowDSL definition, Map<String, Object> inputs, WorkflowMsgCallback callback,
                                  WorkflowExecutionLimits limits) {
        return start(prepare(definition), inputs, callback, limits);
    }

    /**
     * 创建一个 run：准备初始变量（START 节点的入参）、确定 chatId、计算截止时间。
     * 只建状态，不推进。
     */
    public WorkflowGraphRun start(WorkflowGraph graph, Map<String, Object> inputs, WorkflowMsgCallback callback,
                                  WorkflowExecutionLimits limits) {
        WorkflowExecutionLimits selected = limits == null ? defaults : limits;
        String runId = UUID.randomUUID().toString();
        // 图未指定会话时，用 runId 兜底，保证历史能落在一个独立会话里
        String chatId = graph.chatId() == null || graph.chatId().isBlank() ? runId : graph.chatId();
        VariablePool initial = new VariablePool();
        // 运行入参视为 START 节点的输出，后续节点通过引用它的字段取值
        initial.replaceNodeOutputs(graph.startNodeId(), inputs == null ? Map.of() : inputs);
        WorkflowRunContext context = new WorkflowRunContext(runId, graph.flowId(), chatId, initial, callback);
        long startedAt = clock.millis();
        long deadline;
        try {
            deadline = Math.addExact(startedAt, selected.maxDurationMs());
        } catch (ArithmeticException overflow) {
            deadline = Long.MAX_VALUE;
        }
        context.setExecutionDeadline(deadline, clock);
        return new WorkflowGraphRun(graph, context, selected, startedAt, deadline);
    }

    /**
     * 一站式执行：创建 run、推进到结束、把结果通过 callback 上报。
     */
    public WorkflowGraphRun execute(WorkflowDSL definition, Map<String, Object> inputs, WorkflowMsgCallback callback,
                                    WorkflowExecutionLimits limits) {
        WorkflowGraphRun run = start(definition, inputs, callback, limits);
        try {
            callback.onWorkflowStart();
            runToCompletion(run);
            NodeRunResult result = new NodeRunResult();
            result.setOutputs(run.outputs());
            if (run.status() == WorkflowGraphRun.Status.FAILED) {
                result.setError(new NodeCustomException(run.errorCode(), run.error()));
            }
            callback.onWorkflowEnd(result);
            return run;
        } finally {
            callback.finished();
        }
    }

    /**
     * 自旋推进直到 run 离开 RUNNING：每次 tick 若没推进（无活可干或被其他线程占用）就短暂让出。
     */
    public void runToCompletion(WorkflowGraphRun run) {
        while (run.status() == WorkflowGraphRun.Status.RUNNING) {
            if (!tick(run)) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            }
            if (Thread.currentThread().isInterrupted()) {
                run.fail(new NodeCustomException(ErrorCode.INTERRUPTED_ERROR, "Workflow caller interrupted"));
                break;
            }
        }
    }

    /**
     * 推进 run 一步：取一条控制令牌 -> 执行对应节点 -> 发放后继令牌。
     *
     * @return true 表示确实消费了一条令牌并执行了一个节点；false 表示本次没有推进
     *         （run 已结束、没有待办令牌、已有线程在推进，或本次推进失败并把 run 置为 FAILED）
     */
    public boolean tick(WorkflowGraphRun run) {
        // 抢推进闸门：同一 run 同时只允许一个线程推进，抢不到直接让出（不阻塞）
        if (!run.beginAdvance()) {
            return false;
        }
        String executionId = null;
        Future<NodeActivation> future = null;
        try {
            // 1. 整个 run 的截止时间校验，超时立即失败
            checkDuration(run);
            // 2. 取一条令牌；已消费的令牌会被跳过，队列为空说明当前没有可做的事
            WorkflowGraphRun.Token token = run.next();
            if (token == null) {
                // 没有待办时判定 run 是正常结束（到达 END）还是控制流停滞
                run.finishIfIdle(run.getContext().hasPendingExecutions(), run.getContext().physicalAttempts());
                return false;
            }
            Node node = run.graph().node(token.targetNodeId());
            WorkflowRunContext context = run.getContext();
            // 3. 预算校验：全局/单节点激活次数超限即失败，防止循环把执行次数打爆
            if (context.activationCount() >= run.getLimits().maxTotalActivations()
                    || context.activationCount(node.getId()) >= run.getLimits().maxNodeActivations()) {
                throw new NodeCustomException(ErrorCode.WORKFLOW_BUDGET_EXCEEDED, "Node activation limit exceeded at " + node.getId());
            }
            // 4. 建执行作用域：token.id 即 activationId（重放时复用同一作用域），同时固化入参快照与输入来源
            NodeActivation activation = bridge.prepare(context, token.id(), node);
            executionId = activation.executionId();
            checkDuration(run);
            String preparedId = executionId;
            // 5. 交给工作线程执行节点（内部含重试/单次超时控制），调用线程在此阻塞等待结果
            future = workers.submit(() -> bridge.execute(context, preparedId));
            NodeActivation completed = future.get(remaining(run), TimeUnit.MILLISECONDS);
            checkDuration(run);
            // 6. 按执行结果路由：正常出边发放新令牌，失败走向失败分支，END 则记录最终输出
            route(run, token, node, completed);
            // 7. 本步结束后若既无待办令牌、也无在跑节点、也无在途调用，即可收敛 run 的终态
            run.finishIfIdle(context.hasPendingExecutions(), context.physicalAttempts());
            return true;
        } catch (TimeoutException timeout) {
            // 等待节点完成期间整体超时：取消节点并终止 run
            stop(run, executionId, future);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_BUDGET_EXCEEDED, "Workflow duration exceeded"));
        } catch (InterruptedException interrupted) {
            // 调用线程被中断（如上游取消）：取消节点，并把中断标志还原给上层
            stop(run, executionId, future);
            Thread.currentThread().interrupt();
            run.fail(new NodeCustomException(ErrorCode.INTERRUPTED_ERROR, "Workflow caller interrupted"));
        } catch (NodeCustomException failure) {
            // 已带错误码的引擎内失败（超时、预算、路由错误等），直接落到 run 上
            stop(run, executionId, future);
            run.fail(failure);
        } catch (RejectedExecutionException rejected) {
            // 线程池队列已满或已关闭，节点无法被调度
            stop(run, executionId, future);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, "Node executor capacity exhausted"));
        } catch (ExecutionException | CancellationException failure) {
            // 工作线程抛异常或任务被取消，解包原始原因后转为 run 失败
            stop(run, executionId, future);
            Throwable cause = failure.getCause() == null ? failure : failure.getCause();
            run.fail(cause instanceof NodeCustomException custom ? custom
                    : new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, cause.getMessage()));
        } catch (RuntimeException failure) {
            // 兜底：把未预期异常也转成 run 失败，避免异常逃逸让 run 永久停在 RUNNING
            stop(run, executionId, future);
            log.error("Cannot advance workflow run {}", run.getContext().getRunId(), failure);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, failure.getMessage()));
        } finally {
            // 任何路径都必须释放推进闸门，否则该 run 再也无法推进
            run.endAdvance();
        }
        return false;
    }

    // 根据节点执行结果决定后继：成功走正常出边，失败走失败分支，END 则记录最终输出
    private void route(WorkflowGraphRun run, WorkflowGraphRun.Token token, Node node, NodeActivation activation) {
        NodeRunResult result = activation.result();
        if (activation.status() == NodeActivation.Status.CANCELLED || result == null) {
            throw new NodeCustomException(ErrorCode.INTERRUPTED_ERROR, "Node execution did not complete: " + node.getId());
        }
        NodeExecStatusEnum status = result.getStatus();
        boolean failedRoute = status == NodeExecStatusEnum.ERR_FAIL_CONDITION;
        if (status != NodeExecStatusEnum.SUCCESS && status != NodeExecStatusEnum.ERR_CODE_MSG && !failedRoute) {
            throw result.getError() == null ? new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR,
                    "Unsupported or unhandled node outcome: " + status) : result.getError();
        }
        if (node.getNodeType() == NodeTypeEnum.END) {
            if (failedRoute) {
                throw result.getError() == null ? new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR) : result.getError();
            }
            run.reachedEnd(result);
            return;
        }
        List<WorkflowGraph.Transition> candidates = run.graph().outgoing(node.getId()).stream()
                .filter(edge -> edge.failure() == failedRoute).toList();
        if (!failedRoute && node.getNodeType() == NodeTypeEnum.IF_ELSE) {
            candidates = candidates.stream().filter(edge -> edge.handle().equals(result.getSelectedHandle())).toList();
            if (candidates.isEmpty()) {
                throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "IF_ELSE did not select a declared outlet: " + node.getId());
            }
        }
        if (failedRoute && candidates.isEmpty()) {
            throw result.getError() == null ? new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, "Unhandled node failure") : result.getError();
        }
        if (candidates.isEmpty()) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_CONTROL_FLOW_STALLED, "No successor selected at " + node.getId());
        }
        run.issueSuccessors(token, activation.executionId(), candidates);
    }

    // 取消本次执行：先标记作用域不可再产生副作用，再中断工作线程
    private void stop(WorkflowGraphRun run, String executionId, Future<?> future) {
        if (executionId != null) {
            run.getContext().cancel(executionId);
        }
        if (future != null) {
            future.cancel(true);
        }
    }

    private void checkDuration(WorkflowGraphRun run) {
        if (clock.millis() >= run.getDeadline()) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_BUDGET_EXCEEDED, "Workflow duration exceeded");
        }
    }

    // 距离 run 截止还剩多少毫秒（至少 1，且顺带做一次超时校验）
    private long remaining(WorkflowGraphRun run) {
        checkDuration(run);
        return Math.max(1, run.getDeadline() - clock.millis());
    }

    // 关闭线程池并取消尚未开始的任务
    @Override
    public void close() {
        for (Runnable pending : workers.shutdownNow()) {
            if (pending instanceof Future<?> future) {
                future.cancel(false);
            }
        }
    }
}
