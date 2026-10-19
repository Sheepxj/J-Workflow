package com.iflytek.astron.workflow.engine.v2.scheduler;

import com.iflytek.astron.workflow.engine.v2.binding.InputResolver;
import com.iflytek.astron.workflow.engine.v2.compiler.CompiledWorkflow;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.control.ConditionEvaluator;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.definition.LoopDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.NodeInvocation;
import com.iflytek.astron.workflow.engine.v2.executor.NodeTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.ControlToken;
import com.iflytek.astron.workflow.engine.v2.runtime.ExecutionAttempt;
import com.iflytek.astron.workflow.engine.v2.runtime.ExecutionScope;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.RunEvent;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.AttemptStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ScopeType;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.store.ExecutionStore;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkflowScheduler {
    private final WorkflowCompiler compiler;
    private final ExecutionStore store;
    private final Map<String, NodeTaskExecutor> executors;
    private final Executor workers;
    private final Clock clock;
    private final InputResolver resolver = new InputResolver();
    private final ConditionEvaluator conditions = new ConditionEvaluator();
    private final Map<String, CompiledWorkflow> compiledRuns = new ConcurrentHashMap<>();
    private final Map<String, PhysicalAttempt> physicalAttempts = new ConcurrentHashMap<>();

    public WorkflowScheduler(WorkflowCompiler compiler, ExecutionStore store, List<NodeTaskExecutor> executors,
                             Executor workers, Clock clock) {
        this.compiler = compiler;
        this.store = store;
        this.executors = new LinkedHashMap<>();
        executors.forEach(executor -> {
            if (this.executors.putIfAbsent(executor.nodeType(), executor) != null) {
                throw new IllegalArgumentException("Duplicate task executor: " + executor.nodeType());
            }
        });
        this.workers = workers;
        this.clock = clock;
    }

    public String start(WorkflowDefinition definition, Map<String, Object> inputs) {
        CompiledWorkflow compiled = compiler.compile(definition);
        WorkflowRun run = new WorkflowRun();
        run.setRunId(id());
        run.setDefinition(definition);
        run.setDefinitionHash(compiled.contentHash());
        run.setStartedAt(clock.millis());
        run.setInputs(JsonValues.snapshot(inputs));
        ExecutionScope root = scope(run, ScopeType.ROOT, null, null, 0);
        run.setRootScopeId(root.getScopeId());
        run.getTokens().add(new ControlToken(id(), null, compiled.startNodeId(), null, root.getScopeId(), null));
        event(run, "RUN_STARTED", null, null, root.getScopeId(), Map.of());
        store.create(run);
        compiledRuns.put(run.getRunId(), compiled);
        return run.getRunId();
    }

    public WorkflowRun get(String runId) {
        return store.get(runId);
    }

    public void tick(String runId) {
        List<NodeInvocation> dispatch = new ArrayList<>();
        store.update(runId, run -> {
            if (run.getStatus().terminal() || run.getStatus() == RunStatus.PAUSED) {
                return;
            }
            try {
                CompiledWorkflow graph = compiledRuns.computeIfAbsent(runId, key -> compiler.compile(run.getDefinition()));
                require(clock.millis() - run.getStartedAt() < graph.definition().limits().maxDurationMs(),
                        WorkflowError.BUDGET_EXCEEDED, "Workflow duration exceeded");
                expireAttempts(run, graph);
                if (run.getStatus().terminal()) {
                    return;
                }
                if (run.getStatus() == RunStatus.PAUSING) {
                    if (running(run) == null && !physicalAttempts.containsKey(runId)) {
                        run.setStatus(RunStatus.PAUSED);
                        event(run, "RUN_PAUSED", null, null, null, Map.of());
                    }
                    return;
                }
                if (running(run) != null || physicalAttempts.containsKey(runId)) {
                    return;
                }
                NodeExecution waiting = run.getExecutions().values().stream()
                        .filter(execution -> execution.getStatus() == ExecutionStatus.RETRY_WAIT).findFirst().orElse(null);
                if (waiting != null) {
                    if (waiting.getRetryAt() <= clock.millis()) {
                        prepareAttempt(run, graph, waiting, dispatch);
                    }
                    return;
                }
                if (!run.getTokens().isEmpty()) {
                    activate(run, graph, run.getTokens().getFirst(), dispatch);
                    return;
                }
                boolean closed = run.getScopes().values().stream()
                        .noneMatch(item -> item.getType() != ScopeType.ROOT && !item.isClosed());
                require(run.isEndReached() && closed, WorkflowError.CONTROL_FLOW_STALLED,
                        "No runnable work remains before a valid completion");
                run.setStatus(RunStatus.SUCCEEDED);
                event(run, "RUN_SUCCEEDED", null, null, null, run.getOutputs());
            } catch (WorkflowExecutionException e) {
                fail(run, e.getCode(), e.getMessage());
            }
        });
        dispatch.forEach(this::dispatch);
    }

    public void pause(String runId) {
        store.update(runId, run -> {
            if (run.getStatus() == RunStatus.RUNNING) {
                run.setStatus(RunStatus.PAUSING);
                event(run, "PAUSE_REQUESTED", null, null, null, Map.of());
            } else {
                require(run.getStatus() == RunStatus.PAUSING || run.getStatus() == RunStatus.PAUSED,
                        WorkflowError.INVALID_RUN_STATE, "Cannot pause terminal run");
            }
        });
    }

    public void resume(String runId) {
        store.update(runId, run -> {
            if (run.getStatus() == RunStatus.PAUSED) {
                run.setStatus(RunStatus.RUNNING);
                event(run, "RUN_RESUMED", null, null, null, Map.of());
            } else {
                require(run.getStatus() == RunStatus.RUNNING, WorkflowError.INVALID_RUN_STATE, "Run is not paused");
            }
        });
    }

    public void cancel(String runId) {
        store.update(runId, run -> {
            if (!run.getStatus().terminal()) {
                run.setStatus(RunStatus.CANCELLED);
                stopPending(run);
                event(run, "RUN_CANCELLED", null, null, null, Map.of());
            }
        });
    }

    public void complete(String runId, String executionId, String attemptId, Map<String, Object> outputs,
                          WorkflowError errorCode, String error) {
        physicalAttempts.computeIfPresent(runId, (key, physical) -> physical.id().equals(attemptId) ? null : physical);
        store.update(runId, run -> {
            NodeExecution execution = run.getExecutions().get(executionId);
            if (run.getStatus().terminal() || execution == null || execution.getStatus() != ExecutionStatus.RUNNING) {
                return;
            }
            ExecutionAttempt attempt = execution.getAttempts().getLast();
            if (!attempt.getAttemptId().equals(attemptId) || attempt.getStatus() != AttemptStatus.RUNNING) {
                return;
            }
            CompiledWorkflow graph = compiledRuns.computeIfAbsent(runId, key -> compiler.compile(run.getDefinition()));
            try {
                if (clock.millis() >= attempt.getDeadline()) {
                    failedAttempt(run, graph, execution, WorkflowError.ATTEMPT_TIMED_OUT, "Attempt timed out");
                } else if (errorCode != null) {
                    failedAttempt(run, graph, execution, errorCode, error);
                } else {
                    execution.setOutputs(JsonValues.snapshot(outputs));
                    execution.setStatus(ExecutionStatus.SUCCEEDED);
                    attempt.setStatus(AttemptStatus.SUCCEEDED);
                    attempt.setFinishedAt(clock.millis());
                    event(run, "NODE_SUCCEEDED", execution, attempt, execution.getScopeId(), execution.getOutputs());
                    route(run, graph, execution);
                }
            } catch (WorkflowExecutionException | IllegalArgumentException e) {
                fail(run, e instanceof WorkflowExecutionException workflow ? workflow.getCode() : WorkflowError.NODE_FAILED,
                        e.getMessage());
            }
        });
    }

    private void activate(WorkflowRun run, CompiledWorkflow graph, ControlToken token, List<NodeInvocation> dispatch) {
        if (run.getConsumedTokens().contains(token.tokenId())) {
            run.getTokens().removeFirst();
            return;
        }
        NodeDefinition node = graph.nodes().get(token.targetNodeId());
        if (node.breakpoint() && run.getPassedBreakpoints().add(token.tokenId())) {
            run.setStatus(RunStatus.PAUSING);
            event(run, "BREAKPOINT_REACHED", null, null, token.scopeId(), Map.of("nodeId", node.id()));
            return;
        }
        run.getTokens().removeFirst();
        run.getConsumedTokens().add(token.tokenId());
        if ("LOOP".equals(node.type())) {
            advanceLoop(run, graph, token);
            return;
        }
        ExecutionScope scope = run.getScopes().get(token.scopeId());
        require(!scope.isClosed() && !scope.getNodeExecutions().containsKey(node.id()),
                WorkflowError.INVALID_SCOPE, "Node activated more than once within one scope");
        require(run.getExecutions().size() < graph.definition().limits().maxNodeExecutions(),
                WorkflowError.BUDGET_EXCEEDED, "Node execution limit exceeded");
        NodeExecution execution = new NodeExecution();
        execution.setExecutionId(id());
        execution.setNodeId(node.id());
        execution.setScopeId(scope.getScopeId());
        execution.setActivationId(token.tokenId());
        Map<String, Object> inputs = resolver.resolve(node.inputBindings(), run, scope.getScopeId());
        execution.setInputSnapshot("START".equals(node.type()) && node.inputBindings().isEmpty() ? run.getInputs() : inputs);
        run.getExecutions().put(execution.getExecutionId(), execution);
        scope.getNodeExecutions().put(node.id(), execution.getExecutionId());
        prepareAttempt(run, graph, execution, dispatch);
    }

    private void prepareAttempt(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution,
                                 List<NodeInvocation> dispatch) {
        NodeDefinition node = graph.nodes().get(execution.getNodeId());
        ExecutionAttempt attempt = new ExecutionAttempt();
        attempt.setAttemptId(id());
        attempt.setAttemptNo(execution.getAttempts().size() + 1);
        attempt.setStartedAt(clock.millis());
        attempt.setDeadline(saturatingAdd(clock.millis(), node.retry().timeoutMs()));
        attempt.setStatus(AttemptStatus.RUNNING);
        execution.getAttempts().add(attempt);
        execution.setStatus(ExecutionStatus.RUNNING);
        AtomicBoolean cancelled = new AtomicBoolean();
        PhysicalAttempt physical = new PhysicalAttempt(attempt.getAttemptId(), cancelled);
        require(physicalAttempts.putIfAbsent(run.getRunId(), physical) == null,
                WorkflowError.INVALID_RUN_STATE, "Physical attempt already running");
        ExecutionScope scope = run.getScopes().get(execution.getScopeId());
        dispatch.add(new NodeInvocation(run.getRunId(), execution.getExecutionId(), attempt.getAttemptId(),
                attempt.getAttemptNo(), scope.getScopeId(), scope.getIterationNo(), node,
                JsonValues.snapshot(execution.getInputSnapshot()), attempt.getDeadline(), cancelled::get));
        event(run, "NODE_ATTEMPT_STARTED", execution, attempt, scope.getScopeId(), execution.getInputSnapshot());
    }

    private void dispatch(NodeInvocation invocation) {
        try {
            workers.execute(() -> {
                try {
                    if (invocation.cancelled().getAsBoolean()) {
                        complete(invocation.runId(), invocation.executionId(), invocation.attemptId(), Map.of(),
                                WorkflowError.NODE_FAILED, "Attempt cancelled before invocation");
                        return;
                    }
                    Map<String, Object> outputs;
                    if (List.of("START", "END", "XOR").contains(invocation.node().type())) {
                        outputs = invocation.inputs();
                    } else {
                        NodeTaskExecutor executor = executors.get(invocation.node().type());
                        require(executor != null, WorkflowError.NODE_FAILED, "Task executor not registered");
                        outputs = executor.execute(invocation);
                    }
                    complete(invocation.runId(), invocation.executionId(), invocation.attemptId(), outputs, null, null);
                } catch (Exception e) {
                    complete(invocation.runId(), invocation.executionId(), invocation.attemptId(), Map.of(),
                            e instanceof WorkflowExecutionException workflow ? workflow.getCode() : WorkflowError.NODE_FAILED,
                            e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
                }
            });
        } catch (RuntimeException e) {
            complete(invocation.runId(), invocation.executionId(), invocation.attemptId(), Map.of(),
                    WorkflowError.NODE_FAILED, "Task dispatch rejected");
        }
    }

    private void advanceLoop(WorkflowRun run, CompiledWorkflow graph, ControlToken token) {
        require(run.getControlTransitions() < graph.definition().limits().maxControlTransitions(),
                WorkflowError.BUDGET_EXCEEDED, "Control transition limit exceeded");
        run.setControlTransitions(run.getControlTransitions() + 1);
        LoopDefinition definition = graph.loopsByHeader().get(token.targetNodeId());
        ExecutionScope loop;
        if ("enter".equals(token.targetPort())) {
            loop = scope(run, ScopeType.LOOP, token.scopeId(), definition.loopId(), 0);
            loop.setLocalState(resolver.resolve(definition.initialState(), run, token.scopeId()));
        } else {
            ExecutionScope iteration = run.getScopes().get(token.scopeId());
            require(iteration.getType() == ScopeType.ITERATION && !iteration.isClosed(),
                    WorkflowError.INVALID_SCOPE, "Iteration already closed");
            loop = resolver.loopScope(run, token.scopeId(), definition.loopId());
            require(!loop.isClosed(), WorkflowError.INVALID_SCOPE, "Loop already closed");
            Map<String, Object> next = resolver.resolve(definition.nextState(), run, token.scopeId());
            loop.setLocalState(next);
            loop.setCompletedIterations(loop.getCompletedIterations() + 1);
            iteration.setClosed(true);
            event(run, "ITERATION_COMPLETED", null, null, iteration.getScopeId(), next);
        }
        if (!conditions.evaluate(definition.continueWhen(), loop.getLocalState())) {
            Map<String, Object> outputs = resolver.resolve(definition.outputBindings(), run, loop.getScopeId());
            run.getScopes().get(loop.getParentScopeId()).getLoopOutputs().put(definition.loopId(), outputs);
            loop.setClosed(true);
            emitPort(run, graph, definition.headerNodeId(), "exit", loop.getParentScopeId(), null);
            event(run, "LOOP_COMPLETED", null, null, loop.getScopeId(), outputs);
        } else {
            require(loop.getCompletedIterations() < definition.maxIterations(), WorkflowError.LOOP_LIMIT_EXCEEDED,
                    "Loop iteration limit exceeded: " + definition.loopId());
            ExecutionScope iteration = scope(run, ScopeType.ITERATION, loop.getScopeId(), definition.loopId(),
                    loop.getCompletedIterations() + 1);
            emitPort(run, graph, definition.headerNodeId(), "body", iteration.getScopeId(), null);
            event(run, "ITERATION_STARTED", null, null, iteration.getScopeId(), loop.getLocalState());
        }
    }

    private void route(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution) {
        NodeDefinition node = graph.nodes().get(execution.getNodeId());
        if ("END".equals(node.type())) {
            run.setEndReached(true);
            run.setOutputs(execution.getOutputs());
            return;
        }
        List<EdgeDefinition> edges = graph.outgoing().get(node.id());
        EdgeDefinition selected;
        if (node.routing() == NodeDefinition.Routing.EXCLUSIVE || "XOR".equals(node.type())) {
            List<EdgeDefinition> matching = edges.stream().filter(edge -> edge.condition() != null
                    && conditions.evaluate(edge.condition(), execution.getOutputs())).toList();
            require(matching.size() <= 1, WorkflowError.ROUTING_ERROR, "Multiple exclusive routes matched");
            selected = matching.isEmpty() ? edges.stream().filter(EdgeDefinition::defaultEdge).findFirst().orElseThrow()
                    : matching.getFirst();
        } else {
            selected = edges.getFirst();
        }
        emit(run, selected, execution.getScopeId(), execution.getExecutionId());
    }

    private void emitPort(WorkflowRun run, CompiledWorkflow graph, String nodeId, String port, String scopeId,
                           String causeExecutionId) {
        EdgeDefinition edge = graph.outgoing().get(nodeId).stream()
                .filter(item -> port.equals(item.sourcePort())).findFirst().orElseThrow();
        emit(run, edge, scopeId, causeExecutionId);
    }

    private void emit(WorkflowRun run, EdgeDefinition edge, String scopeId, String causeExecutionId) {
        run.getTokens().add(new ControlToken(id(), edge.id(), edge.target(), edge.targetPort(), scopeId, causeExecutionId));
    }

    private void expireAttempts(WorkflowRun run, CompiledWorkflow graph) {
        NodeExecution execution = running(run);
        if (execution != null && execution.getAttempts().getLast().getDeadline() <= clock.millis()) {
            PhysicalAttempt physical = physicalAttempts.get(run.getRunId());
            if (physical != null) {
                physical.cancelled().set(true);
            }
            failedAttempt(run, graph, execution, WorkflowError.ATTEMPT_TIMED_OUT, "Attempt timed out");
        }
    }

    private void failedAttempt(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution,
                                WorkflowError code, String message) {
        ExecutionAttempt attempt = execution.getAttempts().getLast();
        attempt.setStatus(code == WorkflowError.ATTEMPT_TIMED_OUT ? AttemptStatus.TIMED_OUT : AttemptStatus.FAILED);
        attempt.setFinishedAt(clock.millis());
        attempt.setError(message);
        event(run, "NODE_ATTEMPT_FAILED", execution, attempt, execution.getScopeId(), Map.of("code", code.name()));
        NodeDefinition node = graph.nodes().get(execution.getNodeId());
        if (execution.getAttempts().size() < node.retry().maxAttempts()) {
            execution.setStatus(ExecutionStatus.RETRY_WAIT);
            execution.setRetryAt(saturatingAdd(clock.millis(), node.retry().delayMs()));
        } else {
            execution.setStatus(ExecutionStatus.FAILED);
            execution.setErrorCode(code);
            execution.setError(message);
            fail(run, code, message);
        }
    }

    private void fail(WorkflowRun run, WorkflowError code, String message) {
        run.setStatus(RunStatus.FAILED);
        run.setErrorCode(code);
        run.setError(message);
        stopPending(run);
        event(run, "RUN_FAILED", null, null, null, Map.of("code", code.name(), "message", message));
    }

    private void stopPending(WorkflowRun run) {
        run.getTokens().clear();
        run.getExecutions().values().stream().filter(execution -> !execution.getStatus().terminal()).forEach(execution -> {
            execution.setStatus(ExecutionStatus.CANCELLED);
            if (!execution.getAttempts().isEmpty() && execution.getAttempts().getLast().getStatus() == AttemptStatus.RUNNING) {
                execution.getAttempts().getLast().setStatus(AttemptStatus.CANCELLED);
            }
        });
        PhysicalAttempt physical = physicalAttempts.get(run.getRunId());
        if (physical != null) {
            physical.cancelled().set(true);
        }
    }

    private ExecutionScope scope(WorkflowRun run, ScopeType type, String parent, String loopId, int iteration) {
        ExecutionScope scope = new ExecutionScope();
        scope.setScopeId(id());
        scope.setType(type);
        scope.setParentScopeId(parent);
        scope.setLoopId(loopId);
        scope.setIterationNo(iteration);
        run.getScopes().put(scope.getScopeId(), scope);
        return scope;
    }

    private NodeExecution running(WorkflowRun run) {
        return run.getExecutions().values().stream().filter(execution -> execution.getStatus() == ExecutionStatus.RUNNING)
                .findFirst().orElse(null);
    }

    private void event(WorkflowRun run, String type, NodeExecution execution, ExecutionAttempt attempt,
                         String scopeId, Map<String, Object> data) {
        ExecutionScope scope = scopeId == null ? null : run.getScopes().get(scopeId);
        run.getEvents().add(new RunEvent(id(), run.getEvents().size() + 1L, clock.millis(), type, run.getRunId(),
                execution == null ? null : execution.getNodeId(), execution == null ? null : execution.getExecutionId(),
                attempt == null ? null : attempt.getAttemptId(), scopeId, scope == null ? 0 : scope.getIterationNo(),
                JsonValues.snapshot(data)));
    }

    private long saturatingAdd(long value, long delta) {
        return value > Long.MAX_VALUE - delta ? Long.MAX_VALUE : value + delta;
    }

    private void require(boolean valid, WorkflowError code, String message) {
        if (!valid) {
            throw new WorkflowExecutionException(code, message);
        }
    }

    private String id() {
        return UUID.randomUUID().toString();
    }

    private record PhysicalAttempt(String id, AtomicBoolean cancelled) {
    }
}
