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

@Slf4j
@Component
public final class WorkflowGraphExecutor implements AutoCloseable {
    private final NodeExecutionBridge bridge;
    private final WorkflowGraphCompiler compiler;
    private final WorkflowExecutionLimits defaults;
    private final Clock clock;
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

    public WorkflowGraph prepare(WorkflowDSL definition) {
        return compiler.compile(definition);
    }

    public WorkflowGraphRun start(WorkflowDSL definition, Map<String, Object> inputs, WorkflowMsgCallback callback,
                                  WorkflowExecutionLimits limits) {
        return start(prepare(definition), inputs, callback, limits);
    }

    public WorkflowGraphRun start(WorkflowGraph graph, Map<String, Object> inputs, WorkflowMsgCallback callback,
                                  WorkflowExecutionLimits limits) {
        WorkflowExecutionLimits selected = limits == null ? defaults : limits;
        String runId = UUID.randomUUID().toString();
        String chatId = graph.chatId() == null || graph.chatId().isBlank() ? runId : graph.chatId();
        VariablePool initial = new VariablePool();
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

    public boolean tick(WorkflowGraphRun run) {
        if (!run.beginAdvance()) {
            return false;
        }
        String executionId = null;
        Future<NodeActivation> future = null;
        try {
            checkDuration(run);
            WorkflowGraphRun.Token token = run.next();
            if (token == null) {
                run.finishIfIdle(run.getContext().hasPendingExecutions(), run.getContext().physicalAttempts());
                return false;
            }
            Node node = run.graph().node(token.targetNodeId());
            WorkflowRunContext context = run.getContext();
            if (context.activationCount() >= run.getLimits().maxTotalActivations()
                    || context.activationCount(node.getId()) >= run.getLimits().maxNodeActivations()) {
                throw new NodeCustomException(ErrorCode.WORKFLOW_BUDGET_EXCEEDED, "Node activation limit exceeded at " + node.getId());
            }
            NodeActivation activation = bridge.prepare(context, token.id(), node);
            executionId = activation.executionId();
            checkDuration(run);
            String preparedId = executionId;
            future = workers.submit(() -> bridge.execute(context, preparedId));
            NodeActivation completed = future.get(remaining(run), TimeUnit.MILLISECONDS);
            checkDuration(run);
            route(run, node, completed);
            run.finishIfIdle(context.hasPendingExecutions(), context.physicalAttempts());
            return true;
        } catch (TimeoutException timeout) {
            stop(run, executionId, future);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_BUDGET_EXCEEDED, "Workflow duration exceeded"));
        } catch (InterruptedException interrupted) {
            stop(run, executionId, future);
            Thread.currentThread().interrupt();
            run.fail(new NodeCustomException(ErrorCode.INTERRUPTED_ERROR, "Workflow caller interrupted"));
        } catch (NodeCustomException failure) {
            stop(run, executionId, future);
            run.fail(failure);
        } catch (RejectedExecutionException rejected) {
            stop(run, executionId, future);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, "Node executor capacity exhausted"));
        } catch (ExecutionException | CancellationException failure) {
            stop(run, executionId, future);
            Throwable cause = failure.getCause() == null ? failure : failure.getCause();
            run.fail(cause instanceof NodeCustomException custom ? custom
                    : new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, cause.getMessage()));
        } catch (RuntimeException failure) {
            stop(run, executionId, future);
            log.error("Cannot advance workflow run {}", run.getContext().getRunId(), failure);
            run.fail(new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, failure.getMessage()));
        } finally {
            run.endAdvance();
        }
        return false;
    }

    private void route(WorkflowGraphRun run, Node node, NodeActivation activation) {
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
            if (candidates.size() != 1) {
                throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "IF_ELSE did not select a declared outlet: " + node.getId());
            }
        }
        if (failedRoute && candidates.isEmpty()) {
            throw result.getError() == null ? new NodeCustomException(ErrorCode.WORKFLOW_EXECUTION_ERROR, "Unhandled node failure") : result.getError();
        }
        if (candidates.size() > 1) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "Multiple successors selected at " + node.getId());
        }
        if (!candidates.isEmpty()) {
            WorkflowGraph.Transition selected = candidates.getFirst();
            run.issue(selected.target(), activation.executionId(), selected.edgeIndex());
        }
    }

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

    private long remaining(WorkflowGraphRun run) {
        checkDuration(run);
        return Math.max(1, run.getDeadline() - clock.millis());
    }

    @Override
    public void close() {
        for (Runnable pending : workers.shutdownNow()) {
            if (pending instanceof Future<?> future) {
                future.cancel(false);
            }
        }
    }
}
