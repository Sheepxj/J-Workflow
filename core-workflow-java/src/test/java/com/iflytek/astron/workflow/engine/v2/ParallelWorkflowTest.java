package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.ControlToken;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.ParallelRuntime;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.ExecutionStore;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import com.iflytek.astron.workflow.engine.v2.store.JdbcExecutionStore;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelWorkflowTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final AdjustableClock clock = new AdjustableClock();
    private final InMemoryExecutionStore store = new InMemoryExecutionStore(mapper);
    private final WorkflowCompiler compiler = new WorkflowCompiler(mapper, Set.of("MOCK"));

    @Test
    void dispatchesBothBranchesAndJoinsOnceRegardlessOfCompletionOrder() throws Exception {
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(store, work::add);
        String runId = start(engine, parallel());
        startBoth(engine, runId, work);
        assertEquals(2, work.size());
        assertEquals(2, engine.get(runId).getExecutions().values().stream()
                .filter(execution -> execution.getStatus() == ExecutionStatus.RUNNING).count());

        work.removeLast().run();
        engine.tick(runId);
        assertEquals(1, engine.get(runId).getParallels().values().iterator().next().getArrivals().size());
        assertEquals(0, executions(engine, runId, "join").size());
        work.remove().run();
        drain(engine, runId, work, 20);

        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals(Map.of("left", "L", "right", "R"), engine.get(runId).getOutputs());
        assertEquals(1, executions(engine, runId, "join").size());
        assertTrue(engine.get(runId).getParallels().values().iterator().next().isClosed());
        NodeExecution right = executions(engine, runId, "right").getFirst();
        engine.complete(runId, right.getExecutionId(), right.getAttempts().getLast().getAttemptId(), Map.of("value", "wrong"), null, null);
        assertEquals(Map.of("left", "L", "right", "R"), engine.get(runId).getOutputs());
    }

    @Test
    void singleCapacityStillExecutesLogicalParallelBranchesWithoutDeadlock() throws Exception {
        ObjectNode definition = parallel();
        ((ObjectNode) definition.get("limits")).put("maxConcurrentAttempts", 1);
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(store, work::add);
        String runId = start(engine, definition);
        startBoth(engine, runId, work);

        assertEquals(1, work.size());
        drain(engine, runId, work, 30);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
    }

    @Test
    void pauseWaitsForAllActiveBranchesAndPreservesTheirPendingJoinArrivals() throws Exception {
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(store, work::add);
        String runId = start(engine, parallel());
        startBoth(engine, runId, work);
        engine.pause(runId);
        work.removeLast().run();
        engine.tick(runId);
        assertEquals(RunStatus.PAUSING, engine.get(runId).getStatus());
        work.remove().run();
        engine.tick(runId);
        assertEquals(RunStatus.PAUSED, engine.get(runId).getStatus());
        assertEquals(0, executions(engine, runId, "join").size());
        assertEquals(2, engine.get(runId).getTokens().size());

        engine.resume(runId);
        drain(engine, runId, work, 25);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals(1, executions(engine, runId, "join").size());
    }

    @Test
    void persistedPartialJoinSurvivesAStoreAndSchedulerReplacement() throws Exception {
        var dataSource = new DriverManagerDataSource("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        ExecutionStore persisted = new JdbcExecutionStore(dataSource, mapper, true);
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler first = engine(persisted, work::add);
        String runId = start(first, parallel());
        startBoth(first, runId, work);
        work.removeLast().run();
        first.tick(runId);
        first.pause(runId);
        work.remove().run();
        first.tick(runId);
        assertEquals(RunStatus.PAUSED, first.get(runId).getStatus());
        assertEquals(1, first.get(runId).getParallels().values().iterator().next().getArrivals().size());

        WorkflowScheduler restored = engine(new JdbcExecutionStore(dataSource, mapper, false), Runnable::run);
        restored.resume(runId);
        for (int index = 0; index < 20; index++) {
            restored.tick(runId);
        }
        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals(1, executions(restored, runId, "left").size());
        assertEquals(1, executions(restored, runId, "right").size());
        assertEquals(1, executions(restored, runId, "join").size());
    }

    @Test
    void aRetryDelayInOneBranchDoesNotBlockTheOtherBranchFromJoining() throws Exception {
        ObjectNode definition = parallel();
        ((ObjectNode) definition.withArray("nodes").get(2).get("config")).put("failAttempts", 1);
        ((ObjectNode) definition.withArray("nodes").get(2)).putObject("retry")
                .put("maxAttempts", 2).put("delayMs", 1000).put("timeoutMs", 60000);
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler engine = engine(store, work::add);
        String runId = start(engine, definition);
        startBoth(engine, runId, work);
        work.remove().run();
        work.remove().run();
        engine.tick(runId);
        assertEquals(1, engine.get(runId).getParallels().values().iterator().next().getArrivals().size());
        assertTrue(work.isEmpty());
        assertEquals(ExecutionStatus.RETRY_WAIT, executions(engine, runId, "left").getFirst().getStatus());
        clock.advance(1000);
        drain(engine, runId, work, 25);

        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals(2, executions(engine, runId, "left").getFirst().getAttempts().size());
        assertEquals(1, executions(engine, runId, "right").getFirst().getAttempts().size());
    }

    @Test
    void eachLoopIterationCreatesANewForkInstanceAndCannotReuseAnOlderArrival() throws Exception {
        WorkflowScheduler engine = engine(store, Runnable::run);
        String runId = start(engine, parallelLoop());
        boolean injected = false;
        for (int index = 0; index < 100; index++) {
            engine.tick(runId);
            var run = engine.get(runId);
            if (!injected && run.getParallels().size() == 2) {
                ParallelRuntime previous = run.getParallels().values().iterator().next();
                String oldScope = previous.getBranchScopes().values().iterator().next();
                store.update(runId, state -> state.getTokens().add(new ControlToken(UUID.randomUUID().toString(),
                        null, "join", null, oldScope, null)));
                injected = true;
            }
        }

        assertTrue(injected);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals("draft-3", engine.get(runId).getOutputs().get("result"));
        assertEquals(3, engine.get(runId).getParallels().size());
        assertEquals(3, executions(engine, runId, "join").size());
        assertTrue(engine.get(runId).getParallels().values().stream().allMatch(parallel -> parallel.isClosed()
                && parallel.getArrivals().size() == 2));
    }

    @Test
    void rejectsOverlappingBranchesBypassRoutesAndUnpairedGateways() throws Exception {
        ObjectNode overlap = parallel();
        ((ObjectNode) overlap.withArray("edges").get(3)).put("target", "right");
        reject(overlap);
        ObjectNode bypass = parallel();
        ((ObjectNode) bypass.withArray("edges").get(3)).put("target", "end");
        reject(bypass);
        ObjectNode unpaired = parallel();
        unpaired.withArray("parallels").removeAll();
        reject(unpaired);
        ObjectNode duplicatePort = parallel();
        ((ObjectNode) duplicatePort.withArray("edges").get(2)).put("sourcePort", "left");
        reject(duplicatePort);
    }

    @Test
    void aLoopInsideOneBranchExportsOnlyToThatBranchAndItsPairedJoin() throws Exception {
        ObjectNode definition = WorkflowCompilerTest.sample();
        ((ObjectNode) definition.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) definition.withArray("nodes").get(3)).putObject("config").put("operation", "VALIDATE");
        ((ObjectNode) definition.get("limits")).put("maxConcurrentAttempts", 2);
        ((ObjectNode) definition.withArray("edges").get(0)).put("target", "fork").remove("targetPort");
        ((ObjectNode) definition.withArray("edges").get(4)).put("target", "join");
        definition.withArray("nodes").addObject().put("id", "fork").put("type", "FORK");
        definition.withArray("nodes").addObject().put("id", "join").put("type", "JOIN");
        definition.withArray("nodes").addObject().put("id", "right").put("type", "MOCK");
        definition.withArray("edges").addObject().put("id", "fl").put("source", "fork").put("target", "loop")
                .put("sourcePort", "left").put("targetPort", "enter");
        definition.withArray("edges").addObject().put("id", "fr").put("source", "fork").put("target", "right").put("sourcePort", "right");
        definition.withArray("edges").addObject().put("id", "rj").put("source", "right").put("target", "join");
        definition.withArray("edges").addObject().put("id", "je").put("source", "join").put("target", "end");
        definition.putArray("parallels").addObject().put("forkNodeId", "fork").put("joinNodeId", "join")
                .putObject("branches").put("left", "loop").put("right", "right");
        ((ObjectNode) definition.withArray("nodes").get(4)).putObject("inputBindings").putObject("result")
                .put("source", "NODE_OUTPUT").put("nodeId", "join").put("path", "branches.left.loops.refine.result");
        WorkflowScheduler engine = engine(store, Runnable::run);
        String runId = start(engine, definition);
        for (int index = 0; index < 60; index++) {
            engine.tick(runId);
        }

        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals("draft-3", engine.get(runId).getOutputs().get("result"));
        assertEquals(3, executions(engine, runId, "generate").size());
        assertEquals(1, executions(engine, runId, "join").size());
    }

    @Test
    void joinBreakpointPausesOnceAfterAllBranchesHaveArrived() throws Exception {
        ObjectNode definition = parallel();
        ((ObjectNode) definition.withArray("nodes").get(4)).put("breakpoint", true);
        WorkflowScheduler engine = engine(store, Runnable::run);
        String runId = start(engine, definition);
        for (int index = 0; index < 20; index++) {
            engine.tick(runId);
        }
        assertEquals(RunStatus.PAUSED, engine.get(runId).getStatus());
        assertEquals(2, engine.get(runId).getParallels().values().iterator().next().getArrivals().size());
        assertTrue(executions(engine, runId, "join").isEmpty());
        engine.resume(runId);
        for (int index = 0; index < 10; index++) {
            engine.tick(runId);
        }
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
        assertEquals(1, engine.get(runId).getEvents().stream().filter(event -> event.type().equals("BREAKPOINT_REACHED")).count());
    }

    static ObjectNode parallel() throws Exception {
        return (ObjectNode) new ObjectMapper().readTree("""
                {
                  "dslVersion":"2", "definitionId":"parallel", "version":1,
                  "limits":{"maxNodeExecutions":100,"maxDurationMs":300000,"maxConcurrentAttempts":2},
                  "nodes":[
                    {"id":"start","type":"START"},
                    {"id":"fork","type":"FORK"},
                    {"id":"left","type":"MOCK","config":{"operation":"CONSTANT","outputs":{"value":"L"}}},
                    {"id":"right","type":"MOCK","config":{"operation":"CONSTANT","outputs":{"value":"R"}}},
                    {"id":"join","type":"JOIN"},
                    {"id":"end","type":"END","inputBindings":{
                      "left":{"source":"NODE_OUTPUT","nodeId":"join","path":"branches.left.nodes.left.value"},
                      "right":{"source":"NODE_OUTPUT","nodeId":"join","path":"branches.right.nodes.right.value"}
                    }}
                  ],
                  "edges":[
                    {"id":"s","source":"start","target":"fork"},
                    {"id":"fl","source":"fork","target":"left","sourcePort":"left"},
                    {"id":"fr","source":"fork","target":"right","sourcePort":"right"},
                    {"id":"lj","source":"left","target":"join"},
                    {"id":"rj","source":"right","target":"join"},
                    {"id":"je","source":"join","target":"end"}
                  ],
                  "parallels":[{"forkNodeId":"fork","joinNodeId":"join","branches":{"left":"left","right":"right"}}]
                }
                """);
    }

    static ObjectNode parallelLoop() throws Exception {
        ObjectNode definition = parallel();
        ((ObjectNode) definition.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) definition.withArray("nodes").get(3)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) definition.withArray("nodes").get(5)).putObject("inputBindings").putObject("result")
                .put("source", "LOOP_OUTPUT").put("loopId", "refine").put("path", "result");
        definition.withArray("nodes").addObject().put("id", "loop").put("type", "LOOP");
        definition.withArray("nodes").addObject().put("id", "check").put("type", "MOCK")
                .putObject("config").put("operation", "VALIDATE").put("passesOnIteration", 3);
        ((ObjectNode) definition.withArray("edges").get(0)).put("target", "loop").put("targetPort", "enter");
        ((ObjectNode) definition.withArray("edges").get(5)).put("target", "check");
        definition.withArray("edges").addObject().put("id", "body").put("source", "loop").put("sourcePort", "body").put("target", "fork");
        definition.withArray("edges").addObject().put("id", "back").put("source", "check").put("target", "loop")
                .put("targetPort", "continue").put("edgeType", "BACK").put("loopId", "refine");
        definition.withArray("edges").addObject().put("id", "exit").put("source", "loop").put("sourcePort", "exit").put("target", "end");
        ObjectNode loop = definition.putArray("loops").addObject().put("loopId", "refine").put("headerNodeId", "loop").put("maxIterations", 3);
        loop.putArray("bodyNodeIds").add("fork").add("left").add("right").add("join").add("check");
        loop.putObject("initialState").putObject("passed").put("source", "LITERAL").put("value", false);
        loop.putObject("continueWhen").put("operator", "EQUALS").put("statePath", "passed").put("value", false);
        ObjectNode next = loop.putObject("nextState");
        next.putObject("passed").put("source", "NODE_OUTPUT").put("nodeId", "check").put("scope", "CURRENT_ITERATION").put("path", "passed");
        next.putObject("result").put("source", "NODE_OUTPUT").put("nodeId", "join").put("scope", "CURRENT_ITERATION")
                .put("path", "branches.left.nodes.left.text");
        loop.putObject("outputBindings").putObject("result").put("source", "LOOP_STATE").put("path", "result");
        return definition;
    }

    private WorkflowScheduler engine(ExecutionStore stateStore, Executor workers) {
        return new WorkflowScheduler(compiler, stateStore, List.of(new MockTaskExecutor()), workers, clock);
    }

    private String start(WorkflowScheduler engine, ObjectNode definition) throws Exception {
        return engine.start(mapper.treeToValue(definition, WorkflowDefinition.class), Map.of("text", "seed"));
    }

    private void startBoth(WorkflowScheduler engine, String runId, Queue<Runnable> work) {
        engine.tick(runId);
        work.remove().run();
        engine.tick(runId);
        engine.tick(runId);
        engine.tick(runId);
    }

    private void drain(WorkflowScheduler engine, String runId, Queue<Runnable> work, int ticks) {
        for (int index = 0; index < ticks; index++) {
            engine.tick(runId);
            while (!work.isEmpty()) {
                work.remove().run();
            }
        }
    }

    private List<NodeExecution> executions(WorkflowScheduler engine, String runId, String nodeId) {
        return engine.get(runId).getExecutions().values().stream().filter(execution -> execution.getNodeId().equals(nodeId)).toList();
    }

    private void reject(ObjectNode definition) {
        assertThrows(WorkflowExecutionException.class, () -> compiler.compile(mapper.treeToValue(definition, WorkflowDefinition.class)));
    }

    private static class AdjustableClock extends Clock {
        private long now = 1000000;
        void advance(long millis) { now += millis; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return Instant.ofEpochMilli(now); }
    }
}
