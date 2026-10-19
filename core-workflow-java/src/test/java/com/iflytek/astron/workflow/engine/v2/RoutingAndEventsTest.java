package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.NodeInvocation;
import com.iflytek.astron.workflow.engine.v2.executor.NodeTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingAndEventsTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void exclusiveRoutingRunsOnlyItsSelectedOrDefaultBranch() throws Exception {
        for (boolean passed : List.of(true, false)) {
            ObjectNode definition = decision();
            ((ObjectNode) definition.withArray("nodes").get(1).get("config").get("outputs")).put("passed", passed);
            WorkflowRun run = execute(definition);
            assertEquals(RunStatus.SUCCEEDED, run.getStatus());
            assertTrue(run.getExecutions().values().stream().anyMatch(node -> node.getNodeId().equals(passed ? "accepted" : "fallback")));
            assertTrue(run.getExecutions().values().stream().noneMatch(node -> node.getNodeId().equals(passed ? "fallback" : "accepted")));
        }
    }

    @Test
    void ambiguousConditionsFailInsteadOfChoosingByEdgeOrder() throws Exception {
        ObjectNode definition = decision();
        ObjectNode duplicate = definition.withArray("edges").get(1).deepCopy();
        duplicate.put("id", "ambiguous").put("target", "fallback");
        definition.withArray("edges").add(duplicate);

        WorkflowRun run = execute(definition);

        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(WorkflowError.ROUTING_ERROR, run.getErrorCode());
        assertEquals(2, run.getExecutions().size());
    }

    @Test
    void conditionTypeMismatchIsAnExplicitFailure() throws Exception {
        ObjectNode definition = decision();
        ((ObjectNode) definition.withArray("nodes").get(1).get("config").get("outputs")).put("passed", "true");
        assertEquals(WorkflowError.CONDITION_ERROR, execute(definition).getErrorCode());
    }

    @Test
    void controlTransitionBudgetTerminatesALoopBeforeAnotherRound() throws Exception {
        ObjectNode definition = WorkflowCompilerTest.sample();
        ((ObjectNode) definition.get("limits")).put("maxControlTransitions", 1);
        ((ObjectNode) definition.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) definition.withArray("nodes").get(3)).putObject("config").put("operation", "VALIDATE");
        WorkflowRun run = execute(definition);

        assertEquals(WorkflowError.BUDGET_EXCEEDED, run.getErrorCode());
        assertEquals(1, run.getExecutions().values().stream().filter(node -> node.getNodeId().equals("generate")).count());
    }

    @Test
    void streamChunksFromAPreviousAttemptCannotMixWithTheRetry() throws Exception {
        ObjectNode definition = (ObjectNode) mapper.readTree("""
                {"dslVersion":"2","definitionId":"stream","version":1,
                 "nodes":[{"id":"start","type":"START"},
                   {"id":"task","type":"MOCK","retry":{"maxAttempts":2,"delayMs":0,"timeoutMs":1000}},
                   {"id":"end","type":"END"}],
                 "edges":[{"id":"s","source":"start","target":"task"},
                   {"id":"e","source":"task","target":"end"}]}
                """);
        List<NodeInvocation> calls = new ArrayList<>();
        NodeTaskExecutor task = new NodeTaskExecutor() {
            public String nodeType() { return "MOCK"; }
            public Map<String, Object> execute(NodeInvocation invocation) {
                calls.add(invocation);
                if (invocation.attemptNo() == 1) {
                    throw new IllegalStateException("Retry");
                }
                invocation.eventSink().accept("TEXT", Map.of("text", "current"));
                return Map.of("value", 42);
            }
        };
        ArrayDeque<Runnable> work = new ArrayDeque<>();
        WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")),
                new InMemoryExecutionStore(mapper), List.of(task), work::add, clock);
        String runId = scheduler.start(mapper.treeToValue(definition, WorkflowDefinition.class), Map.of());
        scheduler.tick(runId);
        work.remove().run();
        scheduler.tick(runId);
        work.remove().run();
        scheduler.tick(runId);
        calls.getFirst().eventSink().accept("TEXT", Map.of("text", "stale"));
        work.remove().run();
        for (int index = 0; index < 5; index++) {
            scheduler.tick(runId);
            if (!work.isEmpty()) { work.remove().run(); }
        }
        calls.getLast().eventSink().accept("TEXT", Map.of("text", "after-completion"));
        var events = scheduler.get(runId).getEvents().stream().filter(event -> event.type().equals("NODE_STREAM")).toList();

        assertEquals(RunStatus.SUCCEEDED, scheduler.get(runId).getStatus());
        assertEquals(1, events.size());
        assertEquals(calls.getLast().attemptId(), events.getFirst().attemptId());
        assertEquals(Map.of("kind", "TEXT", "payload", Map.of("text", "current")), events.getFirst().data());
    }

    private WorkflowRun execute(ObjectNode definition) throws Exception {
        WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")),
                new InMemoryExecutionStore(mapper), List.of(new MockTaskExecutor()), Runnable::run, clock);
        String runId = scheduler.start(mapper.treeToValue(definition, WorkflowDefinition.class), Map.of("text", "seed"));
        for (int index = 0; index < 30; index++) { scheduler.tick(runId); }
        return scheduler.get(runId);
    }

    private ObjectNode decision() throws Exception {
        return (ObjectNode) mapper.readTree("""
                {"dslVersion":"2","definitionId":"decision","version":1,
                 "nodes":[{"id":"start","type":"START"},
                   {"id":"decision","type":"MOCK","routing":"EXCLUSIVE","config":{"operation":"CONSTANT","outputs":{"passed":true}}},
                   {"id":"accepted","type":"MOCK"},{"id":"fallback","type":"MOCK"},{"id":"end","type":"END"}],
                 "edges":[{"id":"s","source":"start","target":"decision"},
                   {"id":"yes","source":"decision","target":"accepted","condition":{"operator":"EQUALS","output":"passed","value":true}},
                   {"id":"no","source":"decision","target":"fallback","default":true},
                   {"id":"a","source":"accepted","target":"end"},{"id":"b","source":"fallback","target":"end"}]}
                """);
    }
}
