package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureRoutingTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void exhaustedAttemptsKeepTheirFailureAndActivateTheExplicitHandlerOnce() throws Exception {
        WorkflowRun run = execute(pipeline(2));
        var failed = run.getExecutions().values().stream().filter(node -> node.getNodeId().equals("work")).findFirst().orElseThrow();
        var handler = run.getExecutions().values().stream().filter(node -> node.getNodeId().equals("handler")).findFirst().orElseThrow();

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertEquals(ExecutionStatus.FAILED, failed.getStatus());
        assertTrue(failed.isErrorHandled());
        assertEquals(2, failed.getAttempts().size());
        assertTrue(failed.getOutputs().isEmpty());
        assertEquals("NODE_FAILED", handler.getInputSnapshot().get("failure"));
        assertEquals(1, run.getEvents().stream().filter(event -> event.type().equals("NODE_FAILED")).count());
    }

    @Test
    void successfulExecutionNeverActivatesItsErrorBranch() throws Exception {
        WorkflowRun run = execute(pipeline(0));

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertTrue(run.getExecutions().values().stream().noneMatch(node -> node.getNodeId().equals("handler")));
        assertEquals(3, run.getExecutions().size());
    }

    @Test
    void errorDataCannotBeReadAsASuccessfulOutput() throws Exception {
        ObjectNode definition = pipeline(2);
        ((ObjectNode) definition.withArray("nodes").get(2).get("inputBindings").get("failure")).put("source", "NODE_OUTPUT");
        WorkflowRun run = execute(definition);

        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(WorkflowError.INPUT_UNAVAILABLE, run.getErrorCode());
    }

    @Test
    void handledBranchFailureCanStillReachItsPairedJoin() throws Exception {
        ObjectNode definition = ParallelWorkflowTest.parallel();
        ((ObjectNode) definition.withArray("nodes").get(2).get("config")).put("failAttempts", 1);
        definition.withArray("nodes").addObject().put("id", "handler").put("type", "MOCK")
                .putObject("config").put("operation", "CONSTANT").putObject("outputs").put("value", "H");
        definition.withArray("edges").addObject().put("id", "failure").put("source", "left").put("target", "handler").put("on", "FAILURE");
        definition.withArray("edges").addObject().put("id", "handled").put("source", "handler").put("target", "join");
        ((ObjectNode) definition.withArray("nodes").get(5).get("inputBindings").get("left"))
                .put("path", "branches.left.nodes.handler.value");
        WorkflowRun run = execute(definition);

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertEquals(Map.of("left", "H", "right", "R"), run.getOutputs());
        var arrival = run.getParallels().values().iterator().next().getArrivals().get("left");
        assertTrue(((Map<?, ?>) arrival.get("errors")).containsKey("left"));
        assertTrue(!((Map<?, ?>) arrival.get("nodes")).containsKey("left"));
    }

    @Test
    void unhandledFailureTerminatesWithoutFollowingTheSuccessEdge() throws Exception {
        ObjectNode definition = pipeline(2);
        definition.withArray("nodes").remove(2);
        definition.withArray("edges").remove(3);
        definition.withArray("edges").remove(2);
        WorkflowRun run = execute(definition);

        assertEquals(RunStatus.FAILED, run.getStatus());
        assertEquals(WorkflowError.NODE_FAILED, run.getErrorCode());
        assertTrue(run.getExecutions().values().stream().noneMatch(node -> node.getNodeId().equals("end")));
    }

    private WorkflowRun execute(ObjectNode definition) throws Exception {
        WorkflowScheduler engine = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")),
                new InMemoryExecutionStore(mapper), List.of(new MockTaskExecutor()), Runnable::run, Clock.systemUTC());
        String runId = engine.start(mapper.treeToValue(definition, WorkflowDefinition.class), Map.of());
        for (int index = 0; index < 50; index++) {
            engine.tick(runId);
        }
        return engine.get(runId);
    }

    private ObjectNode pipeline(int failures) throws Exception {
        ObjectNode definition = (ObjectNode) mapper.readTree("""
                {
                  "dslVersion":"2", "definitionId":"failure-handler", "version":1,
                  "nodes":[
                    {"id":"start","type":"START"},
                    {"id":"work","type":"MOCK","config":{"operation":"CONSTANT","outputs":{"value":7}},
                     "retry":{"maxAttempts":2,"delayMs":0,"timeoutMs":1000}},
                    {"id":"handler","type":"MOCK","inputBindings":{
                      "failure":{"source":"NODE_ERROR","nodeId":"work","path":"error.code"}
                    }},
                    {"id":"end","type":"END"}
                  ],
                  "edges":[
                    {"id":"s","source":"start","target":"work"},
                    {"id":"ok","source":"work","target":"end"},
                    {"id":"error","source":"work","target":"handler","on":"FAILURE"},
                    {"id":"handled","source":"handler","target":"end"}
                  ]
                }
                """);
        ((ObjectNode) definition.withArray("nodes").get(1).get("config")).put("failAttempts", failures);
        return definition;
    }
}
