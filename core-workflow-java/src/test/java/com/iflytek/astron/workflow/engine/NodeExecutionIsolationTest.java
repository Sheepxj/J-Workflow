package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.NodeExecutionBridge;
import com.iflytek.astron.workflow.engine.execution.WorkflowRunContext;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class NodeExecutionIsolationTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private WorkflowMsgCallback callback;
    private final CountDownLatch release = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = spy(new WorkflowMsgCallback("p1", (type, data) -> { }, EndNodeOutputModeEnum.VARIABLE_MODE,
                new ArrayBlockingQueue<>(64), new ConcurrentLinkedQueue<>(), streams));
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        callback.finished();
        streams.destroy();
    }

    @Test
    void nonCooperativeTimedOutCallCannotOverwriteTheRetryOrEmitLateEvents() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch lateFinished = new CountDownLatch(1);
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                if (calls.incrementAndGet() == 1) {
                    awaitIgnoringInterrupt(release);
                    state.variablePool().set(state.node().getId(), "injected", "late");
                    state.callback().onNodeProcess(0, state.node().getId(), "work", "late-stream", "");
                    return output("late");
                }
                return output("accepted");
            }

            @Override
            protected void successResponse(NodeState state, NodeRunResult result) {
                super.successResponse(state, result);
                if ("late".equals(result.getOutputs().get("output"))) {
                    lateFinished.countDown();
                }
            }
        };
        Node node = node();
        node.getData().setRetryConfig(retry(1, 0.15F));
        WorkflowRunContext run = run();

        NodeActivation record = new NodeExecutionBridge(List.of(executor)).execute(run, node);
        assertEquals(NodeActivation.Status.SUCCEEDED, record.status());
        assertEquals(List.of(NodeActivation.AttemptStatus.TIMED_OUT, NodeActivation.AttemptStatus.SUCCEEDED),
                record.attempts().stream().map(NodeActivation.Attempt::status).toList());
        release.countDown();
        assertTrue(lateFinished.await(2, TimeUnit.SECONDS));

        assertEquals(Map.of("output", "accepted"), run.variables().get("mock::work"));
        verify(callback, times(1)).onNodeEnd(eq("mock::work"), anyString(), any());
        verify(callback, times(1)).onNodeInterrupt(anyString(), any(), eq("mock::work"), anyString(), anyInt(), anyString(), eq(false));
        verify(callback, never()).onNodeProcess(anyInt(), anyString(), anyString(), eq("late-stream"), anyString());
        assertEquals(record.attempts(), run.execution(record.executionId()).attempts());
    }

    @Test
    void cancellationRejectsTheOldInvocationAfterAnotherActivationCommits() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch lateFinished = new CountDownLatch(1);
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                if (calls.incrementAndGet() == 1) {
                    entered.countDown();
                    awaitIgnoringInterrupt(release);
                    return output("cancelled-output");
                }
                return output("new-output");
            }

            @Override
            protected void successResponse(NodeState state, NodeRunResult result) {
                super.successResponse(state, result);
                if ("cancelled-output".equals(result.getOutputs().get("output"))) {
                    lateFinished.countDown();
                }
            }
        };
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        WorkflowRunContext run = run();
        NodeActivation prepared = bridge.prepare(run, "first", node());
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<NodeActivation> first = workers.submit(() -> bridge.execute(run, prepared.executionId()));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            run.cancel(prepared.executionId());
            NodeActivation accepted = bridge.execute(run, node());
            release.countDown();
            assertTrue(lateFinished.await(2, TimeUnit.SECONDS));

            assertEquals(NodeActivation.Status.CANCELLED, first.get(2, TimeUnit.SECONDS).status());
            assertEquals(NodeActivation.AttemptStatus.CANCELLED, run.execution(prepared.executionId()).attempts().getFirst().status());
            assertEquals(NodeActivation.Status.SUCCEEDED, accepted.status());
            assertEquals(Map.of("output", "new-output"), run.variables().get("mock::work"));
            verify(callback, times(1)).onNodeEnd(eq("mock::work"), anyString(), any());
        }
    }

    @Test
    void terminalPresentationHappensAfterTheBusinessResultIsPublished() throws Exception {
        AtomicReference<WorkflowRunContext> current = new AtomicReference<>();
        List<Boolean> observations = new CopyOnWriteArrayList<>();
        WorkflowMsgCallback observing = spy(new WorkflowMsgCallback("observe", (type, data) -> { },
                EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(64), new ConcurrentLinkedQueue<>(), streams));
        doAnswer(invocation -> {
            observations.add("done".equals(current.get().variables().get("mock::work").get("output")));
            return invocation.callRealMethod();
        }).when(observing).onNodeEnd(anyString(), anyString(), any());
        WorkflowRunContext observed = new WorkflowRunContext("observed", "flow", "chat", new VariablePool(), observing);
        current.set(observed);
        AbstractNodeExecutor executor = simple();
        try {
            NodeActivation result = new NodeExecutionBridge(List.of(executor)).execute(observed, node());
            assertEquals(NodeActivation.Status.SUCCEEDED, result.status());
            assertEquals(List.of(true), observations);
        } finally {
            observing.finished();
        }
    }

    @Test
    void cancellationUnblocksAProducerWhenTheStreamQueueIsFull() throws Exception {
        CountDownLatch consumerEntered = new CountDownLatch(1);
        CountDownLatch producerEntered = new CountDownLatch(1);
        WorkflowMsgCallback blocked = new WorkflowMsgCallback("blocked", (type, data) -> {
            consumerEntered.countDown();
            awaitIgnoringInterrupt(release);
        }, EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(1), new ConcurrentLinkedQueue<>(), streams);
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                producerEntered.countDown();
                for (int index = 0; index < 8; index++) {
                    state.callback().onNodeProcess(0, state.node().getId(), "work", "chunk-" + index, "");
                }
                return output("done");
            }
        };
        WorkflowRunContext run = new WorkflowRunContext("blocked", "flow", "chat", new VariablePool(), blocked);
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        NodeActivation prepared = bridge.prepare(run, "blocked", node());
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<NodeActivation> executing = workers.submit(() -> bridge.execute(run, prepared.executionId()));
            try {
                assertTrue(consumerEntered.await(2, TimeUnit.SECONDS));
                assertTrue(producerEntered.await(2, TimeUnit.SECONDS));
                workers.submit(() -> run.cancel(prepared.executionId())).get(2, TimeUnit.SECONDS);
                assertEquals(NodeActivation.Status.CANCELLED, executing.get(2, TimeUnit.SECONDS).status());
                assertFalse(run.variables().containsKey("mock::work"));
            } finally {
                release.countDown();
                blocked.finished();
            }
        }
    }

    @Test
    void aBackpressuredStreamConsumerCanReadRunStateWithoutDeadlockingTheProducer() throws Exception {
        CountDownLatch allowRead = new CountDownLatch(1);
        CountDownLatch queued = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        AtomicReference<WorkflowRunContext> current = new AtomicReference<>();
        WorkflowMsgCallback reading = new WorkflowMsgCallback("reading", (type, data) -> {
            if (delivered.getAndIncrement() == 0) {
                awaitIgnoringInterrupt(allowRead);
            }
            current.get().variables();
        }, EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(1), new ConcurrentLinkedQueue<>(), streams);
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                state.callback().onNodeProcess(0, state.node().getId(), "work", "first", "");
                queued.countDown();
                state.callback().onNodeProcess(0, state.node().getId(), "work", "second", "");
                return output("done");
            }
        };
        WorkflowRunContext run = new WorkflowRunContext("reading", "flow", "chat", new VariablePool(), reading);
        current.set(run);
        Node definition = node();
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<NodeActivation> executing = workers.submit(() -> new NodeExecutionBridge(List.of(executor)).execute(run, definition));
            try {
                assertTrue(queued.await(2, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> executing.get(100, TimeUnit.MILLISECONDS));
                allowRead.countDown();
                assertEquals(NodeActivation.Status.SUCCEEDED, executing.get(2, TimeUnit.SECONDS).status());
                assertEquals(Map.of("output", "done"), run.variables().get("mock::work"));
            } finally {
                allowRead.countDown();
                reading.abort();
            }
        }
    }

    private AbstractNodeExecutor simple() {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return output("done");
            }
        };
    }

    private Node node() throws Exception {
        return LegacyFixtureSupport.load("sequential").findNodeById("mock::work");
    }

    private WorkflowRunContext run() {
        VariablePool variables = new VariablePool();
        variables.set("node-start::s", "text", "seed");
        return new WorkflowRunContext("run", "flow", "chat", variables, callback);
    }

    private RetryConfig retry(int retries, float timeout) {
        RetryConfig config = new RetryConfig();
        config.setShouldRetry(true);
        config.setMaxRetries(retries);
        config.setTimeout(timeout);
        config.setErrorStrategy(0);
        config.setRetryInterval(0F);
        return config;
    }

    private NodeRunResult output(String value) {
        NodeRunResult result = new NodeRunResult();
        result.setOutputs(Map.of("output", value));
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        return result;
    }

    private void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    if (!latch.await(4, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Test latch timed out");
                    }
                    return;
                } catch (InterruptedException error) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
