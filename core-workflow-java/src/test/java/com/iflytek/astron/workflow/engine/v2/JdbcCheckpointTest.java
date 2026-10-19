package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.AttemptStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.JdbcExecutionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.nio.file.Path;
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

class JdbcCheckpointTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DriverManagerDataSource dataSource = new DriverManagerDataSource(
            "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
    private final AdjustableClock clock = new AdjustableClock();

    @Test
    void aQueuedAttemptIsDurableAndCanBeDispatchedWithoutConsumingARetry() throws Exception {
        JdbcExecutionStore firstStore = new JdbcExecutionStore(dataSource, mapper, true);
        Queue<Runnable> abandoned = new ArrayDeque<>();
        WorkflowScheduler first = engine(firstStore, abandoned::add);
        String runId = first.start(definition(false), Map.of("text", "seed"));
        first.tick(runId);
        firstStore.update(runId, run -> {
            NodeExecution pending = run.getExecutions().values().iterator().next();
            pending.setStatus(ExecutionStatus.READY);
            pending.getAttempts().getLast().setStatus(AttemptStatus.READY);
            pending.getAttempts().getLast().setStartedAt(0);
            pending.getAttempts().getLast().setDeadline(0);
        });
        clock.advance(30001);
        WorkflowScheduler restored = engine(new JdbcExecutionStore(dataSource, mapper, false), Runnable::run);
        pump(restored, runId);

        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals(1, restored.get(runId).getExecutions().values().iterator().next().getAttempts().size());
    }

    @Test
    void checkpointSurvivesClosingAndReopeningTheDatabase(@TempDir Path directory) throws Exception {
        String url = "jdbc:h2:file:" + directory.resolve("checkpoint").toAbsolutePath().toString().replace('\\', '/')
                + ";MODE=MySQL";
        JdbcExecutionStore firstStore = new JdbcExecutionStore(new DriverManagerDataSource(url, "sa", ""), mapper, true);
        WorkflowScheduler first = engine(firstStore, Runnable::run);
        String runId = first.start(definition(false), Map.of("text", "seed"));
        first.tick(runId);
        first.pause(runId);
        first.tick(runId);

        JdbcExecutionStore reopened = new JdbcExecutionStore(new DriverManagerDataSource(url, "sa", ""), mapper, false);
        WorkflowScheduler restored = engine(reopened, Runnable::run);
        assertEquals(RunStatus.PAUSED, restored.get(runId).getStatus());
        restored.resume(runId);
        pump(restored, runId);
        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals("draft-3", restored.get(runId).getOutputs().get("result"));
    }

    @Test
    void resumesAPersistedPausedLoopWithANewStoreAndScheduler() throws Exception {
        JdbcExecutionStore firstStore = new JdbcExecutionStore(dataSource, mapper, true);
        WorkflowScheduler first = engine(firstStore, Runnable::run);
        String runId = first.start(definition(false), Map.of("text", "seed"));
        first.tick(runId);
        first.tick(runId);
        first.tick(runId);
        first.pause(runId);
        first.tick(runId);
        String committedExecutionId = first.get(runId).getExecutions().values().stream()
                .filter(execution -> "generate".equals(execution.getNodeId())).findFirst().orElseThrow().getExecutionId();

        JdbcExecutionStore restoredStore = new JdbcExecutionStore(dataSource, mapper, false);
        WorkflowScheduler restored = engine(restoredStore, Runnable::run);
        assertEquals(RunStatus.PAUSED, restored.get(runId).getStatus());
        restored.resume(runId);
        pump(restored, runId);

        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals("draft-3", restored.get(runId).getOutputs().get("result"));
        assertTrue(restored.get(runId).getExecutions().containsKey(committedExecutionId));
        assertEquals(3, restored.get(runId).getExecutions().values().stream()
                .filter(execution -> "generate".equals(execution.getNodeId())).count());
    }

    @Test
    void recoversAnUncommittedDispatchAfterLeaseExpiryWithoutAcceptingTheOldOwnersResult() throws Exception {
        JdbcExecutionStore firstStore = new JdbcExecutionStore(dataSource, mapper, true);
        Queue<Runnable> abandonedWork = new ArrayDeque<>();
        WorkflowScheduler first = engine(firstStore, abandonedWork::add);
        String runId = first.start(definition(true), Map.of("text", "seed"));
        first.tick(runId);
        String executionId = first.get(runId).getExecutions().keySet().iterator().next();
        WorkflowScheduler restored = engine(new JdbcExecutionStore(dataSource, mapper, false), Runnable::run);
        restored.tick(runId);
        assertEquals(1, restored.get(runId).getExecutions().get(executionId).getAttempts().size());

        clock.advance(30001);
        restored.tick(runId);
        abandonedWork.remove().run();
        pump(restored, runId);

        NodeExecution execution = restored.get(runId).getExecutions().get(executionId);
        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals(2, execution.getAttempts().size());
        assertEquals(AttemptStatus.INTERRUPTED, execution.getAttempts().getFirst().getStatus());
        assertEquals(AttemptStatus.SUCCEEDED, execution.getAttempts().getLast().getStatus());
        assertEquals(1, restored.get(runId).getEvents().stream()
                .filter(event -> event.type().equals("NODE_SUCCEEDED") && executionId.equals(event.executionId())).count());
    }

    @Test
    void unknownResultCanBeResolvedManuallyWithoutRepeatingTheExternalCall() throws Exception {
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, mapper, true);
        Queue<Runnable> abandoned = new ArrayDeque<>();
        WorkflowScheduler first = engine(store, abandoned::add);
        String runId = first.start(definition(false), Map.of("text", "seed"));
        first.tick(runId);
        clock.advance(30001);
        WorkflowScheduler restored = engine(new JdbcExecutionStore(dataSource, mapper, false), Runnable::run);
        restored.tick(runId);
        NodeExecution interrupted = restored.get(runId).getExecutions().values().iterator().next();

        assertEquals(RunStatus.PAUSED, restored.get(runId).getStatus());
        assertEquals(ExecutionStatus.RECOVERY_REQUIRED, interrupted.getStatus());
        assertThrows(WorkflowExecutionException.class, () -> restored.resume(runId));
        restored.resolveInterrupted(runId, interrupted.getExecutionId(), false, Map.of("text", "seed"));
        restored.resume(runId);
        pump(restored, runId);

        assertEquals(RunStatus.SUCCEEDED, restored.get(runId).getStatus());
        assertEquals(1, restored.get(runId).getExecutions().get(interrupted.getExecutionId()).getAttempts().size());
    }

    @Test
    void transactionRollsBackBothProgressAndProducedTokens() throws Exception {
        JdbcExecutionStore store = new JdbcExecutionStore(dataSource, mapper, true);
        WorkflowScheduler engine = engine(store, Runnable::run);
        String runId = engine.start(definition(false), Map.of("text", "seed"));
        long revision = store.get(runId).getRevision();
        assertThrows(IllegalStateException.class, () -> store.update(runId, run -> {
            run.getTokens().clear();
            run.setStatus(RunStatus.SUCCEEDED);
            throw new IllegalStateException("Injected failure before commit");
        }));

        assertEquals(revision, store.get(runId).getRevision());
        assertEquals(1, store.get(runId).getTokens().size());
        assertEquals(RunStatus.RUNNING, store.get(runId).getStatus());
        pump(engine, runId);
        assertEquals(RunStatus.SUCCEEDED, engine.get(runId).getStatus());
    }

    private WorkflowDefinition definition(boolean allowRecoveryRetry) throws Exception {
        ObjectNode sample = WorkflowCompilerTest.sample();
        ((ObjectNode) sample.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
        ((ObjectNode) sample.withArray("nodes").get(3)).putObject("config")
                .put("operation", "VALIDATE").put("passesOnIteration", 3);
        if (allowRecoveryRetry) {
            ((ObjectNode) sample.withArray("nodes").get(0)).putObject("retry")
                    .put("maxAttempts", 2).put("delayMs", 0).put("timeoutMs", 60000);
        }
        return mapper.treeToValue(sample, WorkflowDefinition.class);
    }

    private WorkflowScheduler engine(JdbcExecutionStore store, Executor workers) {
        return new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")), store,
                List.of(new MockTaskExecutor()), workers, clock);
    }

    private void pump(WorkflowScheduler engine, String runId) {
        for (int index = 0; index < 40; index++) {
            engine.tick(runId);
        }
    }

    private static class AdjustableClock extends Clock {
        private long now = 1000000;
        void advance(long millis) { now += millis; }
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return Instant.ofEpochMilli(now); }
    }
}
