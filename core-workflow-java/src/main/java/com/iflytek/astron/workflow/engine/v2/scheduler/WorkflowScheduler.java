package com.iflytek.astron.workflow.engine.v2.scheduler;

import com.iflytek.astron.workflow.engine.v2.binding.InputResolver;
import com.iflytek.astron.workflow.engine.v2.compiler.CompiledWorkflow;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.control.ConditionEvaluator;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition.Outcome;
import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.definition.LoopDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.ParallelDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.NodeInvocation;
import com.iflytek.astron.workflow.engine.v2.executor.NodeTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.ControlToken;
import com.iflytek.astron.workflow.engine.v2.runtime.ExecutionAttempt;
import com.iflytek.astron.workflow.engine.v2.runtime.ExecutionScope;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.InputOrigin;
import com.iflytek.astron.workflow.engine.v2.runtime.ParallelRuntime;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    private final String ownerId = UUID.randomUUID().toString();
    private static final long LEASE_MILLIS = 30000;

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

    public Set<String> registeredNodeTypes() {
        return Set.copyOf(executors.keySet());
    }

    public void tick(String runId) {
        List<NodeInvocation> dispatch = new ArrayList<>();
        store.update(runId, run -> {
            if (run.getStatus().terminal() || run.getStatus() == RunStatus.PAUSED) {
                return;
            }
            try {
                if (!claimOwnership(run)) {
                    return;
                }
                CompiledWorkflow graph = compiledRuns.computeIfAbsent(runId, key -> compiler.compile(run.getDefinition()));
                require(graph.contentHash().equals(run.getDefinitionHash()), WorkflowError.INVALID_DEFINITION,
                        "Stored definition hash differs from the run definition");
                require(clock.millis() - run.getStartedAt() < graph.definition().limits().maxDurationMs(),
                        WorkflowError.BUDGET_EXCEEDED, "Workflow duration exceeded");
                expireAttempts(run, graph);
                if (run.getStatus().terminal()) {
                    return;
                }
                if (run.getStatus() == RunStatus.PAUSING) {
                    if (running(run) == null && physicalCount(runId) == 0) {
                        run.setStatus(RunStatus.PAUSED);
                        releaseOwnership(run);
                        event(run, "RUN_PAUSED", null, null, null, Map.of());
                    }
                    return;
                }
                if (physicalCount(runId) >= graph.definition().limits().maxConcurrentAttempts()) {
                    return;
                }
                NodeExecution ready = run.getExecutions().values().stream()
                        .filter(execution -> execution.getStatus() == ExecutionStatus.READY).findFirst().orElse(null);
                if (ready != null) {
                    dispatch.add(invocation(run, graph, ready, ready.getAttempts().getLast(), new AtomicBoolean()));
                    return;
                }
                NodeExecution waiting = run.getExecutions().values().stream()
                        .filter(execution -> execution.getStatus() == ExecutionStatus.RETRY_WAIT
                                && execution.getRetryAt() <= clock.millis()).findFirst().orElse(null);
                if (waiting != null) {
                    prepareAttempt(run, graph, waiting, dispatch);
                    return;
                }
                if (!run.getTokens().isEmpty()) {
                    activate(run, graph, run.getTokens().getFirst(), dispatch);
                    return;
                }
                if (physicalCount(runId) > 0 || run.getExecutions().values().stream()
                        .anyMatch(execution -> !execution.getStatus().terminal())) {
                    return;
                }
                boolean closed = run.getScopes().values().stream()
                        .noneMatch(item -> item.getType() != ScopeType.ROOT && !item.isClosed());
                require(run.isEndReached() && closed, WorkflowError.CONTROL_FLOW_STALLED,
                        "No runnable work remains before a valid completion");
                run.setStatus(RunStatus.SUCCEEDED);
                releaseOwnership(run);
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
                require(run.getExecutions().values().stream().noneMatch(item -> item.getStatus() == ExecutionStatus.RECOVERY_REQUIRED),
                        WorkflowError.RECOVERY_REQUIRED, "Resolve interrupted executions before resuming");
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
                releaseOwnership(run);
                stopPending(run);
                event(run, "RUN_CANCELLED", null, null, null, Map.of());
            }
        });
    }

    public void complete(String runId, String executionId, String attemptId, Map<String, Object> outputs,
                          WorkflowError errorCode, String error) {
        physicalAttempts.computeIfPresent(attemptId, (key, physical) -> physical.runId().equals(runId) ? null : physical);
        store.update(runId, run -> {
            NodeExecution execution = run.getExecutions().get(executionId);
            if (run.getStatus().terminal() || !ownerId.equals(run.getOwnerId()) || run.getLeaseUntil() <= clock.millis()
                    || execution == null || execution.getStatus() != ExecutionStatus.RUNNING) {
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
        if ((!"JOIN".equals(node.type()) || "joined".equals(token.targetPort()))
                && node.breakpoint() && run.getPassedBreakpoints().add(token.tokenId())) {
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
        if ("FORK".equals(node.type())) {
            fork(run, graph, token);
            return;
        }
        if ("JOIN".equals(node.type())) {
            if ("joined".equals(token.targetPort())) {
                ParallelRuntime parallel = run.getParallels().get(token.tokenId());
                require(parallel != null && parallel.isClosed(), WorkflowError.INVALID_SCOPE, "JOIN collection is not complete");
                activateBusiness(run, graph, token, dispatch, Map.of("branches", parallel.getArrivals()));
            } else {
                join(run, graph, token);
            }
            return;
        }
        activateBusiness(run, graph, token, dispatch, null);
    }

    private void activateBusiness(WorkflowRun run, CompiledWorkflow graph, ControlToken token,
                                    List<NodeInvocation> dispatch, Map<String, Object> suppliedInputs) {
        NodeDefinition node = graph.nodes().get(token.targetNodeId());
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
        execution.setCauseExecutionId(token.causeExecutionId());
        Map<String, Object> inputs = suppliedInputs == null ? resolver.resolve(node.inputBindings(), run, scope.getScopeId())
                : JsonValues.snapshot(suppliedInputs);
        execution.setInputSnapshot("START".equals(node.type()) && node.inputBindings().isEmpty() ? run.getInputs() : inputs);
        execution.setInputOrigins(suppliedInputs == null ? resolver.origins(node.inputBindings(), run, scope.getScopeId())
                : Map.of("branches", new InputOrigin("JOIN_COLLECTION", null, token.tokenId(), null, 0)));
        if ("START".equals(node.type()) && node.inputBindings().isEmpty()) {
            Map<String, InputOrigin> origins = new LinkedHashMap<>();
            run.getInputs().keySet().forEach(name -> origins.put(name,
                    new InputOrigin("RUN_INPUT", null, run.getRootScopeId(), name, 0)));
            execution.setInputOrigins(Map.copyOf(origins));
        }
        run.getExecutions().put(execution.getExecutionId(), execution);
        scope.getNodeExecutions().put(node.id(), execution.getExecutionId());
        prepareAttempt(run, graph, execution, dispatch);
    }

    private void prepareAttempt(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution,
                                 List<NodeInvocation> dispatch) {
        ExecutionAttempt attempt = new ExecutionAttempt();
        attempt.setAttemptId(id());
        attempt.setAttemptNo(execution.getAttempts().size() + 1);
        execution.getAttempts().add(attempt);
        execution.setStatus(ExecutionStatus.READY);
        dispatch.add(invocation(run, graph, execution, attempt, new AtomicBoolean()));
        event(run, "NODE_ATTEMPT_QUEUED", execution, attempt, execution.getScopeId(), Map.of());
    }

    private NodeInvocation invocation(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution,
                                       ExecutionAttempt attempt, AtomicBoolean cancelled) {
        NodeDefinition node = graph.nodes().get(execution.getNodeId());
        ExecutionScope scope = run.getScopes().get(execution.getScopeId());
        String runId = run.getRunId();
        String executionId = execution.getExecutionId();
        String attemptId = attempt.getAttemptId();
        return new NodeInvocation(runId, executionId, attemptId,
                attempt.getAttemptNo(), scope.getScopeId(), scope.getIterationNo(), node,
                JsonValues.snapshot(execution.getInputSnapshot()), attempt.getDeadline(), cancelled::get,
                (type, data) -> recordStream(runId, executionId, attemptId, type, data));
    }

    private void dispatch(NodeInvocation planned) {
        AtomicBoolean cancelled = new AtomicBoolean();
        PhysicalAttempt physical = new PhysicalAttempt(planned.attemptId(), planned.runId(), cancelled);
        if (physicalAttempts.putIfAbsent(planned.attemptId(), physical) != null) {
            return;
        }
        AtomicReference<NodeInvocation> claimed = new AtomicReference<>();
        try {
            store.update(planned.runId(), run -> {
                NodeExecution execution = run.getExecutions().get(planned.executionId());
                if (run.getStatus() != RunStatus.RUNNING || !ownerId.equals(run.getOwnerId())
                        || run.getLeaseUntil() <= clock.millis() || execution == null
                        || execution.getStatus() != ExecutionStatus.READY
                        || physicalCount(planned.runId()) > run.getDefinition().limits().maxConcurrentAttempts()
                        || run.getExecutions().values().stream().filter(item -> item.getStatus() == ExecutionStatus.RUNNING)
                                .count() >= run.getDefinition().limits().maxConcurrentAttempts()) {
                    return;
                }
                ExecutionAttempt attempt = execution.getAttempts().getLast();
                if (!attempt.getAttemptId().equals(planned.attemptId()) || attempt.getStatus() != AttemptStatus.READY) {
                    return;
                }
                attempt.setStartedAt(clock.millis());
                attempt.setDeadline(saturatingAdd(clock.millis(), planned.node().retry().timeoutMs()));
                attempt.setStatus(AttemptStatus.RUNNING);
                execution.setStatus(ExecutionStatus.RUNNING);
                claimed.set(invocation(run, compiledRuns.get(planned.runId()), execution, attempt, cancelled));
                event(run, "NODE_ATTEMPT_STARTED", execution, attempt, execution.getScopeId(), execution.getInputSnapshot());
            });
        } catch (RuntimeException e) {
            physicalAttempts.remove(planned.attemptId(), physical);
            throw e;
        }
        NodeInvocation invocation = claimed.get();
        if (invocation == null) {
            physicalAttempts.remove(planned.attemptId(), physical);
            return;
        }
        try {
            workers.execute(() -> {
                try {
                    if (invocation.cancelled().getAsBoolean()) {
                        complete(invocation.runId(), invocation.executionId(), invocation.attemptId(), Map.of(),
                                WorkflowError.NODE_FAILED, "Attempt cancelled before invocation");
                        return;
                    }
                    Map<String, Object> outputs;
                    if (List.of("START", "END", "XOR", "JOIN").contains(invocation.node().type())) {
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

    private void recordStream(String runId, String executionId, String attemptId, String type, Map<String, Object> data) {
        store.update(runId, run -> {
            NodeExecution execution = run.getExecutions().get(executionId);
            if (run.getStatus().terminal() || !ownerId.equals(run.getOwnerId()) || run.getLeaseUntil() <= clock.millis()
                    || execution == null || execution.getStatus() != ExecutionStatus.RUNNING) {
                return;
            }
            ExecutionAttempt attempt = execution.getAttempts().getLast();
            if (attempt.getAttemptId().equals(attemptId) && clock.millis() < attempt.getDeadline()) {
                event(run, "NODE_STREAM", execution, attempt, execution.getScopeId(),
                        Map.of("kind", type, "payload", JsonValues.snapshot(data)));
            }
        });
    }

    private void advanceLoop(WorkflowRun run, CompiledWorkflow graph, ControlToken token) {
        controlStep(run, graph);
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
            run.getScopes().get(loop.getParentScopeId()).getLoopOutputSources().put(definition.loopId(), loop.getScopeId());
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

    private void fork(WorkflowRun run, CompiledWorkflow graph, ControlToken token) {
        controlStep(run, graph);
        ParallelDefinition definition = graph.parallelsByFork().get(token.targetNodeId());
        ExecutionScope parent = run.getScopes().get(token.scopeId());
        require(parent != null && !parent.isClosed(), WorkflowError.INVALID_SCOPE, "FORK parent scope is closed");
        ExecutionScope group = scope(run, ScopeType.PARALLEL, parent.getScopeId(), parent.getLoopId(), parent.getIterationNo());
        ParallelRuntime parallel = new ParallelRuntime();
        parallel.setForkInstanceId(group.getScopeId());
        parallel.setForkNodeId(definition.forkNodeId());
        parallel.setJoinNodeId(definition.joinNodeId());
        parallel.setParentScopeId(parent.getScopeId());
        run.getParallels().put(group.getScopeId(), parallel);
        for (EdgeDefinition edge : graph.outgoing().get(definition.forkNodeId())) {
            ExecutionScope branch = scope(run, ScopeType.BRANCH, group.getScopeId(), parent.getLoopId(), parent.getIterationNo());
            branch.setBranchId(edge.sourcePort());
            parallel.getBranchScopes().put(edge.sourcePort(), branch.getScopeId());
            emit(run, edge, branch.getScopeId(), token.causeExecutionId());
        }
        event(run, "PARALLEL_STARTED", null, null, group.getScopeId(),
                Map.of("forkInstanceId", group.getScopeId(), "forkNodeId", definition.forkNodeId()));
    }

    private void join(WorkflowRun run, CompiledWorkflow graph, ControlToken token) {
        controlStep(run, graph);
        ExecutionScope branch = run.getScopes().get(token.scopeId());
        require(branch != null && branch.getType() == ScopeType.BRANCH, WorkflowError.INVALID_SCOPE,
                "JOIN arrival must belong to a branch scope");
        ParallelRuntime parallel = run.getParallels().get(branch.getParentScopeId());
        require(parallel != null && parallel.getJoinNodeId().equals(token.targetNodeId())
                && branch.getScopeId().equals(parallel.getBranchScopes().get(branch.getBranchId())),
                WorkflowError.INVALID_SCOPE, "JOIN does not belong to this fork instance");
        if (parallel.getArrivals().containsKey(branch.getBranchId())) {
            return;
        }
        Map<String, Object> outputs = new LinkedHashMap<>();
        Map<String, Object> errors = new LinkedHashMap<>();
        branch.getNodeExecutions().forEach((nodeId, executionId) -> {
            NodeExecution execution = run.getExecutions().get(executionId);
            require(execution.getStatus() == ExecutionStatus.SUCCEEDED
                    || execution.getStatus() == ExecutionStatus.FAILED && execution.isErrorHandled(), WorkflowError.INVALID_SCOPE,
                    "Branch has an uncommitted execution");
            if (execution.getStatus() == ExecutionStatus.SUCCEEDED) {
                outputs.put(nodeId, execution.getOutputs());
            } else {
                errors.put(nodeId, execution.getErrorOutputs());
            }
        });
        parallel.getArrivals().put(branch.getBranchId(), JsonValues.snapshot(
                Map.of("nodes", outputs, "errors", errors, "loops", branch.getLoopOutputs())));
        branch.setClosed(true);
        event(run, "BRANCH_ARRIVED", null, null, branch.getScopeId(),
                Map.of("forkInstanceId", parallel.getForkInstanceId(), "branchId", branch.getBranchId()));
        if (parallel.getArrivals().size() == parallel.getBranchScopes().size()) {
            parallel.setClosed(true);
            run.getScopes().get(parallel.getForkInstanceId()).setClosed(true);
            run.getTokens().add(new ControlToken(parallel.getForkInstanceId(), null, parallel.getJoinNodeId(),
                    "joined", parallel.getParentScopeId(), null));
            event(run, "PARALLEL_COMPLETED", null, null, parallel.getForkInstanceId(),
                    Map.of("forkInstanceId", parallel.getForkInstanceId()));
        }
    }

    private void controlStep(WorkflowRun run, CompiledWorkflow graph) {
        require(run.getControlTransitions() < graph.definition().limits().maxControlTransitions(),
                WorkflowError.BUDGET_EXCEEDED, "Control transition limit exceeded");
        run.setControlTransitions(run.getControlTransitions() + 1);
    }

    private void route(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution) {
        NodeDefinition node = graph.nodes().get(execution.getNodeId());
        if ("END".equals(node.type())) {
            run.setEndReached(true);
            run.setOutputs(execution.getOutputs());
            return;
        }
        List<EdgeDefinition> edges = graph.outgoing().get(node.id()).stream().filter(edge -> edge.on() == Outcome.SUCCESS).toList();
        emit(run, selectRoute(node, edges, execution.getOutputs()), execution.getScopeId(), execution.getExecutionId());
    }

    private EdgeDefinition selectRoute(NodeDefinition node, List<EdgeDefinition> edges, Map<String, Object> values) {
        EdgeDefinition selected;
        if (node.routing() == NodeDefinition.Routing.EXCLUSIVE || "XOR".equals(node.type())) {
            List<EdgeDefinition> matching = edges.stream().filter(edge -> edge.condition() != null
                    && conditions.evaluate(edge.condition(), values)).toList();
            require(matching.size() <= 1, WorkflowError.ROUTING_ERROR, "Multiple exclusive routes matched");
            selected = matching.isEmpty() ? edges.stream().filter(EdgeDefinition::defaultEdge).findFirst().orElseThrow()
                    : matching.getFirst();
        } else {
            selected = edges.getFirst();
        }
        return selected;
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
        for (NodeExecution execution : run.getExecutions().values()) {
            if (execution.getStatus() == ExecutionStatus.RUNNING && execution.getAttempts().getLast().getDeadline() <= clock.millis()) {
                PhysicalAttempt physical = physicalAttempts.get(execution.getAttempts().getLast().getAttemptId());
                if (physical != null) {
                    physical.cancelled().set(true);
                }
                failedAttempt(run, graph, execution, WorkflowError.ATTEMPT_TIMED_OUT, "Attempt timed out");
                if (run.getStatus().terminal()) {
                    return;
                }
            }
        }
    }

    private void failedAttempt(WorkflowRun run, CompiledWorkflow graph, NodeExecution execution,
                                WorkflowError code, String message) {
        message = message == null ? code.name() : message;
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
            execution.setErrorOutputs(JsonValues.snapshot(Map.of("error", Map.of("code", code.name(), "message", message))));
            event(run, "NODE_FAILED", execution, attempt, execution.getScopeId(), execution.getErrorOutputs());
            List<EdgeDefinition> errorEdges = graph.outgoing().get(node.id()).stream()
                    .filter(edge -> edge.on() == Outcome.FAILURE).toList();
            if (errorEdges.isEmpty()) {
                fail(run, code, message);
            } else {
                EdgeDefinition selected = selectRoute(node, errorEdges, execution.getErrorOutputs());
                execution.setErrorHandled(true);
                emit(run, selected, execution.getScopeId(), execution.getExecutionId());
            }
        }
    }

    private void fail(WorkflowRun run, WorkflowError code, String message) {
        message = message == null ? code.name() : message;
        run.setStatus(RunStatus.FAILED);
        releaseOwnership(run);
        run.setErrorCode(code);
        run.setError(message);
        stopPending(run);
        event(run, "RUN_FAILED", null, null, null, Map.of("code", code.name(), "message", message));
    }

    private void stopPending(WorkflowRun run) {
        run.getTokens().clear();
        run.getExecutions().values().stream().filter(execution -> !execution.getStatus().terminal()).forEach(execution -> {
            execution.setStatus(ExecutionStatus.CANCELLED);
            if (!execution.getAttempts().isEmpty()) {
                ExecutionAttempt attempt = execution.getAttempts().getLast();
                if (attempt.getStatus() == AttemptStatus.RUNNING || attempt.getStatus() == AttemptStatus.READY) {
                    attempt.setStatus(AttemptStatus.CANCELLED);
                    attempt.setFinishedAt(clock.millis());
                }
            }
        });
        physicalAttempts.values().stream().filter(physical -> physical.runId().equals(run.getRunId()))
                .forEach(physical -> physical.cancelled().set(true));
    }

    public void resolveInterrupted(String runId, String executionId, boolean retry, Map<String, Object> outputs) {
        store.update(runId, run -> {
            require(run.getStatus() == RunStatus.PAUSED, WorkflowError.INVALID_RUN_STATE, "Run must be paused");
            NodeExecution execution = run.getExecutions().get(executionId);
            require(execution != null && execution.getStatus() == ExecutionStatus.RECOVERY_REQUIRED,
                    WorkflowError.INVALID_RUN_STATE, "Execution does not need recovery");
            CompiledWorkflow graph = compiledRuns.computeIfAbsent(runId, key -> compiler.compile(run.getDefinition()));
            if (retry) {
                require(execution.getAttempts().size() < graph.nodes().get(execution.getNodeId()).retry().maxAttempts(),
                        WorkflowError.INVALID_RUN_STATE, "Attempt budget exhausted; resolve with known result or cancel");
                execution.setStatus(ExecutionStatus.RETRY_WAIT);
                execution.setRetryAt(clock.millis());
            } else {
                execution.setOutputs(JsonValues.snapshot(outputs));
                execution.setStatus(ExecutionStatus.SUCCEEDED);
                route(run, graph, execution);
            }
            event(run, "EXECUTION_RECOVERY_RESOLVED", execution, null, execution.getScopeId(), Map.of("retry", retry));
        });
    }

    private boolean claimOwnership(WorkflowRun run) {
        if (ownerId.equals(run.getOwnerId())) {
            run.setLeaseUntil(saturatingAdd(clock.millis(), LEASE_MILLIS));
            return true;
        }
        if (run.getOwnerId() != null && run.getLeaseUntil() > clock.millis()) {
            return false;
        }
        boolean interrupted = false;
        for (NodeExecution execution : run.getExecutions().values()) {
            if (execution.getStatus() != ExecutionStatus.RUNNING) {
                continue;
            }
            interrupted = true;
            ExecutionAttempt attempt = execution.getAttempts().getLast();
            attempt.setStatus(AttemptStatus.INTERRUPTED);
            attempt.setFinishedAt(clock.millis());
            attempt.setError("Previous scheduler lease expired before a result was committed");
            NodeDefinition node = run.getDefinition().nodes().stream()
                    .filter(item -> item.id().equals(execution.getNodeId())).findFirst().orElseThrow();
            boolean safe = Set.of("START", "END", "XOR", "MOCK", "JOIN").contains(node.type());
            String recovery = String.valueOf(node.config().getOrDefault("recovery", safe ? "RETRY" : "MANUAL"));
            if ("RETRY".equals(recovery) && execution.getAttempts().size() < node.retry().maxAttempts()) {
                execution.setStatus(ExecutionStatus.RETRY_WAIT);
                execution.setRetryAt(clock.millis());
            } else {
                execution.setStatus(ExecutionStatus.RECOVERY_REQUIRED);
                run.setStatus(RunStatus.PAUSED);
            }
            event(run, "EXECUTION_INTERRUPTED", execution, attempt, execution.getScopeId(), Map.of("recovery", recovery));
        }
        if (interrupted) {
            event(run, "RUN_RECOVERED", null, null, null, Map.of());
        }
        if (run.getStatus() == RunStatus.PAUSED) {
            releaseOwnership(run);
            return false;
        }
        run.setOwnerId(ownerId);
        run.setLeaseUntil(saturatingAdd(clock.millis(), LEASE_MILLIS));
        return true;
    }

    private void releaseOwnership(WorkflowRun run) {
        run.setOwnerId(null);
        run.setLeaseUntil(0);
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
        List<String> scopePath = new ArrayList<>();
        ExecutionScope ancestor = scope;
        while (ancestor != null) {
            scopePath.addFirst(ancestor.getScopeId());
            ancestor = ancestor.getParentScopeId() == null ? null : run.getScopes().get(ancestor.getParentScopeId());
        }
        run.getEvents().add(new RunEvent(id(), run.getEvents().size() + 1L, clock.millis(), type, run.getRunId(),
                execution == null ? null : execution.getNodeId(), execution == null ? null : execution.getExecutionId(),
                attempt == null ? null : attempt.getAttemptId(), scopeId, scope == null ? 0 : scope.getIterationNo(),
                JsonValues.snapshot(data), scopePath));
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

    private long physicalCount(String runId) {
        return physicalAttempts.values().stream().filter(attempt -> attempt.runId().equals(runId)).count();
    }

    private record PhysicalAttempt(String id, String runId, AtomicBoolean cancelled) {
    }
}
