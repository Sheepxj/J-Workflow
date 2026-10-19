package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryTransactionTest {
    @Test
    void failingTransitionDoesNotPublishPartialChangesOrAdvanceTheRevision() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InMemoryExecutionStore store = new InMemoryExecutionStore(mapper);
        WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")), store,
                List.of(new MockTaskExecutor()), Runnable::run, Clock.systemUTC());
        String runId = scheduler.start(mapper.treeToValue(WorkflowCompilerTest.sample(), WorkflowDefinition.class), Map.of("text", "seed"));
        long revision = store.get(runId).getRevision();

        assertThrows(IllegalStateException.class, () -> store.update(runId, run -> {
            run.getTokens().clear();
            run.getInputs().put("text", "corrupted");
            run.setStatus(RunStatus.SUCCEEDED);
            throw new IllegalStateException("Transition failed");
        }));

        assertEquals(revision, store.get(runId).getRevision());
        assertEquals(1, store.get(runId).getTokens().size());
        assertEquals("seed", store.get(runId).getInputs().get("text"));
        assertEquals(RunStatus.RUNNING, store.get(runId).getStatus());
    }
}
