package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ScopeType;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedLoopTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final WorkflowCompiler compiler = new WorkflowCompiler(mapper, Set.of("MOCK"));

    @Test
    void reentersTheInnerLoopWithIndependentStateForEachOuterIteration() throws Exception {
        var store = new InMemoryExecutionStore(mapper);
        var scheduler = new WorkflowScheduler(compiler, store, List.of(new MockTaskExecutor()), Runnable::run,
                Clock.systemUTC());
        String runId = scheduler.start(mapper.treeToValue(nested(), WorkflowDefinition.class), Map.of("text", "seed"));
        for (int tick = 0; tick < 80; tick++) {
            scheduler.tick(runId);
        }
        var run = scheduler.get(runId);

        assertEquals(RunStatus.SUCCEEDED, run.getStatus());
        assertEquals("draft-3", run.getOutputs().get("result"));
        assertEquals(6, run.getExecutions().values().stream().filter(e -> e.getNodeId().equals("generate")).count());
        assertEquals(2, run.getExecutions().values().stream().filter(e -> e.getNodeId().equals("generate")
                && "seed".equals(e.getInputSnapshot().get("draft"))).count());
        assertEquals(2, run.getScopes().values().stream().filter(scope -> scope.getType() == ScopeType.LOOP
                && scope.getLoopId().equals("refine")).count());
        assertTrue(run.getScopes().values().stream().filter(scope -> scope.getType() != ScopeType.ROOT)
                .allMatch(scope -> scope.isClosed()));
    }

    @Test
    void rejectsPartialOverlapAndCrossLevelJumps() throws Exception {
        ObjectNode sample = nested();
        ((ObjectNode) sample.withArray("loops").get(1)).withArray("bodyNodeIds").add("generate");
        assertThrows(WorkflowExecutionException.class,
                () -> compiler.compile(mapper.treeToValue(sample, WorkflowDefinition.class)));

        ObjectNode crossing = nested();
        ((ObjectNode) crossing.withArray("edges").get(2)).put("target", "outerCheck");
        assertThrows(WorkflowExecutionException.class,
                () -> compiler.compile(mapper.treeToValue(crossing, WorkflowDefinition.class)));
    }

    static ObjectNode nested() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) sample.withArray("nodes").get(3)).putObject("config")
                .put("operation", "VALIDATE").put("passesOnIteration", 3);
        sample.withArray("nodes").addObject().put("id", "outer").put("type", "LOOP");
        ObjectNode check = sample.withArray("nodes").addObject().put("id", "outerCheck").put("type", "MOCK");
        check.putObject("config").put("operation", "VALIDATE").put("passesOnIteration", 2);
        ((ObjectNode) sample.withArray("nodes").get(4).get("inputBindings").get("result")).put("loopId", "outerLoop");
        ((ObjectNode) sample.withArray("edges").get(0)).put("target", "outer");
        ((ObjectNode) sample.withArray("edges").get(4)).put("target", "outerCheck");
        sample.withArray("edges").addObject().put("id", "outerBody").put("source", "outer")
                .put("sourcePort", "body").put("target", "loop").put("targetPort", "enter");
        sample.withArray("edges").addObject().put("id", "outerBack").put("source", "outerCheck")
                .put("target", "outer").put("targetPort", "continue").put("edgeType", "BACK").put("loopId", "outerLoop");
        sample.withArray("edges").addObject().put("id", "outerExit").put("source", "outer")
                .put("sourcePort", "exit").put("target", "end");
        ObjectNode loop = mapper.createObjectNode().put("loopId", "outerLoop").put("headerNodeId", "outer").put("maxIterations", 2);
        loop.putArray("bodyNodeIds").add("loop").add("outerCheck");
        ObjectNode initial = loop.putObject("initialState");
        initial.putObject("passed").put("source", "LITERAL").put("value", false);
        initial.putObject("result").put("source", "RUN_INPUT").put("path", "text");
        loop.putObject("continueWhen").put("operator", "EQUALS").put("statePath", "passed").put("value", false);
        ObjectNode next = loop.putObject("nextState");
        next.putObject("passed").put("source", "NODE_OUTPUT").put("nodeId", "outerCheck")
                .put("scope", "CURRENT_ITERATION").put("path", "passed");
        next.putObject("result").put("source", "LOOP_OUTPUT").put("loopId", "refine").put("path", "result");
        loop.putObject("outputBindings").putObject("result").put("source", "LOOP_STATE").put("path", "result");
        sample.withArray("loops").add(loop);
        return sample;
    }
}
