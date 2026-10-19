package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkflowExamplesTest {
    @ParameterizedTest
    @ValueSource(strings = {"structured-loop.json", "parallel.json"})
    void committedRequestExamplesExecuteSuccessfully(String file) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var request = mapper.readTree(Path.of("examples", file).toFile());
        WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")),
                new InMemoryExecutionStore(mapper), List.of(new MockTaskExecutor()), Runnable::run, Clock.systemUTC());
        WorkflowDefinition definition = mapper.treeToValue(request.get("definition"), WorkflowDefinition.class);
        String runId = scheduler.start(definition, Map.of("text", request.get("inputs").get("text").asText()));
        for (int index = 0; index < 100; index++) {
            scheduler.tick(runId);
        }

        assertEquals(RunStatus.SUCCEEDED, scheduler.get(runId).getStatus());
        assertEquals(file.equals("parallel.json") ? Map.of("left", "L", "right", "R") : Map.of("result", "draft-3"),
                scheduler.get(runId).getOutputs());
    }
}
