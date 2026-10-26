package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.WorkflowExecutionLimits;
import com.iflytek.astron.workflow.engine.execution.WorkflowGraphRun;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowGraphBudgetTest {
    @Test
    void anEndlessBusinessLoopStopsBeforeCreatingAnActivationBeyondTheNodeLimit() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            harness.responses("RETRY");
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("branch-cycle"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(2, 50, 5000));

            assertBudgetFailure(run);
            assertEquals(2, harness.modelCalls.get());
            assertEquals(2, run.getContext().activationCount("spark-llm::generate"));
            assertEquals(5, run.getContext().activationCount());
            assertEquals(0, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void aTotalActivationLimitCountsStartAndDecisionNodes() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            harness.responses("RETRY");
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("branch-cycle"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(10, 3, 5000));

            assertBudgetFailure(run);
            assertEquals(3, run.getContext().activationCount());
            assertEquals(1, harness.modelCalls.get());
        }
    }

    @Test
    void expirationBeforeDispatchDoesNotCreateOrCallAnyNode() throws Exception {
        TestClock clock = new TestClock();
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(clock, Executors.newVirtualThreadPerTaskExecutor())) {
            WorkflowGraphRun run = harness.executor.start(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(10, 20, 100));
            clock.advance(100);

            harness.executor.tick(run);

            assertBudgetFailure(run);
            assertEquals(0, run.getContext().activationCount());
            assertTrue(run.pending().isEmpty());
        }
    }

    @Test
    void aResultReturnedAfterTheAbsoluteDeadlineCannotCommit() throws Exception {
        TestClock clock = new TestClock();
        NodeExecutor work = mockWork((state, inputs) -> {
            clock.advance(100);
            return success("late");
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(clock, Executors.newVirtualThreadPerTaskExecutor(), work)) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(10, 20, 100));

            assertBudgetFailure(run);
            assertFalse(run.getContext().variables().containsKey("mock::work"));
            assertEquals(NodeActivation.Status.CANCELLED, run.getContext().executions().getLast().status());
            assertEquals(0, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void expirationBetweenBufferedNodeEndAndCommitCannotPublishAnUncommittedSuccess() throws Exception {
        TestClock clock = new TestClock();
        NodeExecutor work = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            public NodeRunResult execute(NodeState state) {
                NodeRunResult result = super.execute(state);
                clock.advance(100);
                return result;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return success("uncommitted");
            }
        };
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(clock, Executors.newVirtualThreadPerTaskExecutor(), work)) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(10, 20, 100));

            assertBudgetFailure(run);
            assertFalse(run.getContext().variables().containsKey("mock::work"));
            assertFalse(harness.frames.stream().anyMatch(frame -> "mock::work".equals(frame.getWorkflowStep().getNode().getId())
                    && "stop".equals(frame.getWorkflowStep().getNode().getFinishReason())));
        }
    }

    @Test
    void workflowDeadlineDoesNotWaitForANonCooperativeBusinessCallAndDropsItsLateOutput() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        NodeExecutor work = mockWork((state, inputs) -> {
            entered.countDown();
            awaitIgnoringInterrupt(release);
            return success("late");
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(work);
             ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            WorkflowGraphRun run = harness.executor.start(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(10, 20, 1000));
            Future<?> running = callers.submit(() -> harness.executor.runToCompletion(run));
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                running.get(3, TimeUnit.SECONDS);
                assertBudgetFailure(run);
                assertEquals(1, run.getContext().physicalAttempts());
                assertFalse(run.getContext().hasPendingExecutions());
            } finally {
                release.countDown();
            }
            awaitPhysicalDrain(run);
            assertFalse(run.getContext().variables().containsKey("mock::work"));
            assertEquals(NodeActivation.Status.CANCELLED, run.getContext().executions().getLast().status());
        }
    }

    @Test
    void retriesShareAnActivationAndEndWaitsForTheOldPhysicalAttemptToExit() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        NodeExecutor work = mockWork((state, inputs) -> {
            if (attempts.incrementAndGet() == 1) {
                firstEntered.countDown();
                awaitIgnoringInterrupt(release);
                return success("old");
            }
            return success("current");
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(work)) {
            WorkflowDSL definition = harness.fixture("sequential");
            RetryConfig retry = new RetryConfig();
            retry.setShouldRetry(true);
            retry.setMaxRetries(1);
            retry.setTimeout(0.1f);
            retry.setErrorStrategy(0);
            retry.setCustomOutput(Map.of());
            definition.findNodeById("mock::work").getData().setRetryConfig(retry);
            WorkflowGraphRun run = harness.executor.start(definition, Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(1, 3, 5000));
            try {
                assertTrue(harness.executor.tick(run));
                assertTrue(harness.executor.tick(run));
                assertTrue(firstEntered.await(1, TimeUnit.SECONDS));
                assertTrue(harness.executor.tick(run));
                assertEquals(WorkflowGraphRun.Status.RUNNING, run.status());
                assertEquals(1, run.getContext().physicalAttempts());
                assertTrue(run.pending().isEmpty());
                assertEquals(3, run.getContext().activationCount());
                assertEquals(2, run.getContext().executions().get(1).attempts().size());
                assertEquals(NodeActivation.AttemptStatus.TIMED_OUT, run.getContext().executions().get(1).attempts().getFirst().status());
                assertEquals("Result: current", run.outputs().get("content"));
            } finally {
                release.countDown();
            }
            awaitPhysicalDrain(run);
            harness.executor.runToCompletion(run);
            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals("current", run.getContext().variables().get("mock::work").get("output"));
        }
    }

    @Test
    void workerRejectionCancelsThePreparedActivationInsteadOfLeavingItReady() throws Exception {
        ExecutorService workers = Executors.newSingleThreadExecutor();
        workers.shutdown();
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(Clock.systemUTC(), workers)) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.WORKFLOW_EXECUTION_ERROR.getCode(), run.errorCode());
            assertFalse(run.getContext().hasPendingExecutions());
            assertEquals(NodeActivation.Status.CANCELLED, run.getContext().executions().getFirst().status());
        }
    }

    @Test
    void limitsMustBePositive() {
        assertThrows(NodeCustomException.class, () -> new WorkflowExecutionLimits(0, 1, 1));
        assertThrows(NodeCustomException.class, () -> new WorkflowExecutionLimits(1, 0, 1));
        assertThrows(NodeCustomException.class, () -> new WorkflowExecutionLimits(1, 1, 0));
    }

    private void assertBudgetFailure(WorkflowGraphRun run) {
        assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
        assertEquals(ErrorCode.WORKFLOW_BUDGET_EXCEEDED.getCode(), run.errorCode());
        assertTrue(run.pending().isEmpty());
    }

    private static NodeExecutor mockWork(Work work) {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return work.call(state, inputs);
            }
        };
    }

    private static NodeRunResult success(String output) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        result.setOutputs(Map.of("output", output));
        return result;
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
        try {
            while (latch.getCount() != 0 && System.nanoTime() < deadline) {
                try {
                    latch.await(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (latch.getCount() != 0) {
                throw new IllegalStateException("Test latch timed out");
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void awaitPhysicalDrain(WorkflowGraphRun run) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (run.getContext().physicalAttempts() != 0 && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(2);
        }
        assertEquals(0, run.getContext().physicalAttempts());
    }

    @FunctionalInterface
    private interface Work {
        NodeRunResult call(NodeState state, Map<String, Object> inputs);
    }

    private static final class TestClock extends Clock {
        private final AtomicLong now = new AtomicLong(1000);

        void advance(long millis) {
            now.addAndGet(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(now.get());
        }

        @Override
        public long millis() {
            return now.get();
        }
    }
}
