package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.NodeInvocation;
import com.iflytek.astron.workflow.engine.v2.executor.NodeTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.ControlToken;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.AttemptStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowSchedulerTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final MutableClock clock = new MutableClock();
    private final InMemoryExecutionStore store = new InMemoryExecutionStore(mapper);
    private final List<NodeInvocation> calls = new ArrayList<>();

    @Test
    void executesThreeIterationsWithIndependentHistoryAndExportsFinalOutput() throws IOException {
        WorkflowScheduler engine = engine(Runnable::run, 3, false);
        String runId = start(engine, WorkflowCompilerTest.sample());
        pump(engine, runId, 30);
        WorkflowRun run = engine.get(runId);

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertEquals(8, run.getExecutions().size());
        assertEquals(List.of(1, 2, 3), calls.stream().filter(c -> c.node().id().equals("generate"))
                .map(NodeInvocation::iterationNo).toList());
        assertEquals("draft-3", run.getOutputs().get("result"));
        assertEquals(3, executions(run, "generate").stream().map(NodeExecution::getExecutionId).distinct().count());
        assertEquals(List.of("seed", "draft-1", "draft-2"), executions(run, "generate").stream()
                .map(execution -> execution.getInputSnapshot().get("draft")).toList());
        assertTrue(run.getExecutions().values().stream().allMatch(e -> e.getStatus() == ExecutionStatus.SUCCEEDED));
        assertEquals(1, run.getEvents().stream().filter(event -> event.type().equals("RUN_SUCCEEDED")).count());
    }

    @Test
    void retriesWithinEachIterationWithoutConsumingAnotherIteration() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(2)).putObject("retry")
                .put("maxAttempts", 2).put("delayMs", 0).put("timeoutMs", 1000);
        WorkflowScheduler engine = engine(Runnable::run, 3, true);
        String runId = start(engine, sample);
        pump(engine, runId, 40);
        WorkflowRun run = engine.get(runId);

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        List<NodeExecution> generations = executions(run, "generate");
        assertEquals(3, generations.size());
        assertTrue(generations.stream().allMatch(e -> e.getAttempts().size() == 2));
        assertEquals(6, calls.stream().filter(call -> call.node().id().equals("generate")).count());
        assertEquals(calls.get(0).inputs(), calls.get(1).inputs());
        assertEquals(calls.get(0).executionId(), calls.get(1).executionId());
        assertNotEquals(calls.get(0).attemptId(), calls.get(1).attemptId());
        assertEquals(calls.get(0).idempotencyKey("generate"), calls.get(1).idempotencyKey("generate"));
    }

    @Test
    void permitsSuccessOnTheLastAllowedIterationButFailsIfAnotherIsRequired() throws IOException {
        WorkflowScheduler succeeds = engine(Runnable::run, 5, false);
        String first = start(succeeds, WorkflowCompilerTest.sample());
        pump(succeeds, first, 40);
        assertEquals(RunStatus.SUCCEEDED, succeeds.get(first).getStatus());

        WorkflowScheduler fails = engine(Runnable::run, 6, false);
        String second = start(fails, WorkflowCompilerTest.sample());
        pump(fails, second, 40);
        assertEquals(RunStatus.FAILED, fails.get(second).getStatus());
        assertEquals(WorkflowError.LOOP_LIMIT_EXCEEDED, fails.get(second).getErrorCode());
        assertEquals(5, executions(fails.get(second), "generate").size());
    }

    @Test
    void supportsZeroIterationsWithAnExplicitInitialOutput() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ObjectNode initial = (ObjectNode) sample.withArray("loops").get(0).get("initialState").get("passed");
        initial.put("value", true);
        WorkflowScheduler engine = engine(Runnable::run, 3, false);
        String runId = start(engine, sample);
        pump(engine, runId, 10);

        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals("seed", engine.get(runId).getOutputs().get("result"));
        assertTrue(executions(engine.get(runId), "generate").isEmpty());
    }

    @Test
    void ignoresDuplicateTokensAndDuplicateCompletionEvents() throws IOException {
        WorkflowScheduler engine = engine(Runnable::run, 1, false);
        String runId = start(engine, WorkflowCompilerTest.sample());
        store.update(runId, run -> {
            ControlToken token = run.getTokens().getFirst();
            run.getTokens().add(token);
        });
        pump(engine, runId, 15);
        WorkflowRun before = engine.get(runId);
        NodeExecution execution = executions(before, "generate").getFirst();
        engine.complete(runId, execution.getExecutionId(), execution.getAttempts().getFirst().getAttemptId(),
                Map.of("text", "incorrect"), null, null);

        assertEquals(4, engine.get(runId).getExecutions().size());
        assertEquals(before.getEvents(), engine.get(runId).getEvents());
        assertEquals("draft-1", engine.get(runId).getOutputs().get("result"));
    }

    @Test
    void doesNotReadAPreviousIterationsMissingOutput() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(3).get("inputBindings").get("text")).put("path", "missing");
        WorkflowScheduler engine = engine(Runnable::run, 3, false);
        String runId = start(engine, sample);
        pump(engine, runId, 15);

        assertEquals(WorkflowError.INPUT_UNAVAILABLE, engine.get(runId).getErrorCode());
        assertEquals(RunStatus.FAILED, engine.get(runId).getStatus());
    }

    @Test
    void pauseLetsTheCurrentCallCommitWithoutStartingItsSuccessor() throws IOException {
        Queue<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(work::add, 3, false);
        String runId = start(engine, WorkflowCompilerTest.sample());
        engine.tick(runId);
        engine.pause(runId);
        engine.pause(runId);
        work.remove().run();
        engine.tick(runId);

        assertEquals(RunStatus.PAUSED, engine.get(runId).getStatus());
        assertEquals(1, engine.get(runId).getExecutions().size());
        assertFalse(engine.get(runId).getTokens().isEmpty());
        assertTrue(work.isEmpty());
        engine.resume(runId);
        engine.resume(runId);
        for (int index = 0; index < 30; index++) {
            engine.tick(runId);
            if (!work.isEmpty()) {
                work.remove().run();
            }
        }
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
    }

    @Test
    void breakpointPausesOncePerActivationAndCanBeContinued() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(2)).put("breakpoint", true);
        WorkflowScheduler engine = engine(Runnable::run, 1, false);
        String runId = start(engine, sample);
        pump(engine, runId, 10);

        assertEquals(RunStatus.PAUSED, engine.get(runId).getStatus());
        assertTrue(executions(engine.get(runId), "generate").isEmpty());
        engine.resume(runId);
        pump(engine, runId, 15);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
    }

    @Test
    void timedOutCallsKeepPhysicalCapacityUntilTheyFinishAndTheirLateResultIsIgnored() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(0)).putObject("retry")
                .put("maxAttempts", 2).put("delayMs", 50).put("timeoutMs", 100);
        Queue<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(work::add, 1, false);
        String runId = start(engine, sample);
        engine.tick(runId);
        clock.advance(101);
        engine.tick(runId);
        clock.advance(50);
        engine.tick(runId);

        NodeExecution start = executions(engine.get(runId), "start").getFirst();
        assertEquals(ExecutionStatus.RETRY_WAIT, start.getStatus());
        assertEquals(1, start.getAttempts().size());
        assertEquals(1, work.size());
        work.remove().run();
        engine.tick(runId);
        start = executions(engine.get(runId), "start").getFirst();
        assertEquals(2, start.getAttempts().size());
        assertEquals(AttemptStatus.TIMED_OUT, start.getAttempts().getFirst().getStatus());
        work.remove().run();
        assertEquals(ExecutionStatus.SUCCEEDED, executions(engine.get(runId), "start").getFirst().getStatus());
    }

    @Test
    void enforcesTotalExecutionAndTimeBudgets() throws IOException {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.get("limits")).put("maxNodeExecutions", 2);
        WorkflowScheduler engine = engine(Runnable::run, 3, false);
        String first = start(engine, sample);
        pump(engine, first, 15);
        assertEquals(WorkflowError.BUDGET_EXCEEDED, engine.get(first).getErrorCode());
        assertEquals(2, engine.get(first).getExecutions().size());

        String second = start(engine, WorkflowCompilerTest.sample());
        clock.advance(300001);
        engine.tick(second);
        assertEquals(WorkflowError.BUDGET_EXCEEDED, engine.get(second).getErrorCode());
    }

    @Test
    void cancellationSignalsTheActiveInvocationAndRejectsItsCompletion() throws IOException {
        Queue<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(work::add, 1, false);
        String runId = start(engine, WorkflowCompilerTest.sample());
        engine.tick(runId);
        work.remove().run();
        engine.tick(runId);
        engine.tick(runId);
        engine.cancel(runId);
        work.remove().run();

        assertEquals(RunStatus.CANCELLED, engine.get(runId).getStatus());
        assertTrue(calls.isEmpty());
        assertTrue(engine.get(runId).getTokens().isEmpty());
    }

    @Test
    void snapshotsReturnedToCallersCannotChangeTheRun() throws IOException {
        WorkflowScheduler engine = engine(Runnable::run, 1, false);
        String runId = start(engine, WorkflowCompilerTest.sample());
        WorkflowRun snapshot = engine.get(runId);
        snapshot.getTokens().clear();
        snapshot.getInputs().put("text", "changed");

        pump(engine, runId, 15);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals("seed", engine.get(runId).getInputs().get("text"));
    }

    @Test
    void runsALongChainWithoutRecursiveScheduling() throws IOException {
        ObjectNode tree = mapper.createObjectNode().put("dslVersion", "2").put("definitionId", "long-chain").put("version", 1);
        tree.putObject("limits").put("maxNodeExecutions", 2500).put("maxDurationMs", 300000);
        var nodes = tree.putArray("nodes");
        var edges = tree.putArray("edges");
        for (int index = 0; index < 2000; index++) {
            nodes.addObject().put("id", "n" + index).put("type", index == 0 ? "START" : index == 1999 ? "END" : "MOCK");
            if (index > 0) {
                edges.addObject().put("id", "e" + index).put("source", "n" + (index - 1)).put("target", "n" + index);
            }
        }
        AtomicInteger count = new AtomicInteger();
        NodeTaskExecutor passThrough = new NodeTaskExecutor() {
            public String nodeType() { return "MOCK"; }
            public Map<String, Object> execute(NodeInvocation invocation) {
                count.incrementAndGet();
                return Map.of();
            }
        };
        WorkflowScheduler engine = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")), store,
                List.of(passThrough), Runnable::run, clock);
        String runId = start(engine, tree);
        pump(engine, runId, 2001);

        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals(1998, count.get());
    }

    private WorkflowScheduler engine(Executor workers, int passesAt, boolean retryGenerate) {
        NodeTaskExecutor mock = new NodeTaskExecutor() {
            public String nodeType() { return "MOCK"; }
            public Map<String, Object> execute(NodeInvocation invocation) {
                calls.add(invocation);
                if (invocation.node().id().equals("generate")) {
                    if (retryGenerate && invocation.attemptNo() == 1) {
                        throw new IllegalStateException("Retry this call");
                    }
                    return Map.of("text", "draft-" + invocation.iterationNo());
                }
                return Map.of("passed", invocation.iterationNo() >= passesAt);
            }
        };
        return new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")), store, List.of(mock), workers, clock);
    }

    private String start(WorkflowScheduler engine, ObjectNode sample) throws IOException {
        return engine.start(mapper.treeToValue(sample, WorkflowDefinition.class), Map.of("text", "seed"));
    }

    private void pump(WorkflowScheduler engine, String runId, int ticks) {
        for (int index = 0; index < ticks; index++) {
            engine.tick(runId);
        }
    }

    private List<NodeExecution> executions(WorkflowRun run, String nodeId) {
        return run.getExecutions().values().stream().filter(execution -> execution.getNodeId().equals(nodeId)).toList();
    }

    private static class MutableClock extends Clock {
        private long now = 1000000;
        void advance(long millis) { now += millis; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return Instant.ofEpochMilli(now); }
    }
}
