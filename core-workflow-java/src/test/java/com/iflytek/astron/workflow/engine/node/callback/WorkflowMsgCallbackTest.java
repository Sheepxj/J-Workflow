package com.iflytek.astron.workflow.engine.node.callback;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.domain.callbacks.ChatCallBackStreamResult;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.StreamCallback;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowMsgCallbackTest {

    private SimpleMeterRegistry registry;
    private WorkflowStreamVirtualThreadExecutor executor;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        executor = new WorkflowStreamVirtualThreadExecutor(new WorkflowMetrics(registry));
    }

    @AfterEach
    void tearDown() {
        executor.destroy();
        registry.close();
    }

    @Test
    void consumesOnDedicatedVirtualThreadAndExitsAfterFinish() throws Exception {
        RecordingCallback client = new RecordingCallback();
        WorkflowMsgCallback callback = callback(client);

        callback.onWorkflowStart();
        assertTrue(client.eventReceived.await(2, TimeUnit.SECONDS));
        callback.finished();

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertEquals(List.of("stream", "finished"), client.operations);
        assertTrue(client.eventThread.isVirtual());
        assertTrue(client.eventThread.getName().startsWith("workflow-stream-vt-"));
        assertEquals(0, registry.get("workflow.stream.virtual.threads").gauge().value());
    }

    @Test
    void consumerExitsWhenClientDisconnects() throws Exception {
        RecordingCallback client = new RecordingCallback();
        WorkflowMsgCallback callback = callback(client);

        client.active.set(false);

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(callback.isActive());
    }

    @Test
    void consumerExitsWhenClientTimesOut() throws Exception {
        RecordingCallback client = new RecordingCallback();
        WorkflowMsgCallback callback = callback(client);

        client.active.set(false);
        callback.onWorkflowStart();

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(callback.isActive());
    }

    @Test
    void consumerExitsWhenForwardingFails() throws Exception {
        RecordingCallback client = new RecordingCallback();
        client.failOnCallback.set(true);
        WorkflowMsgCallback callback = callback(client);

        callback.onWorkflowStart();

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertTrue(client.aborted.get());
        assertFalse(callback.isActive());
    }

    @Test
    void abortIsIdempotentAndStopsConsumer() throws Exception {
        RecordingCallback client = new RecordingCallback();
        WorkflowMsgCallback callback = callback(client);

        callback.abort();
        callback.abort();

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertTrue(client.aborted.get());
    }

    private WorkflowMsgCallback callback(StreamCallback client) {
        ArrayBlockingQueue<LLMGenerate> streamQueue = new ArrayBlockingQueue<>(4);
        Queue<ChatCallBackStreamResult> orderedQueue = new ConcurrentLinkedQueue<>();
        return new WorkflowMsgCallback(
                "sid",
                client,
                EndNodeOutputModeEnum.DIRECT_MODE,
                streamQueue,
                orderedQueue,
                executor
        );
    }

    private static final class RecordingCallback implements StreamCallback {

        private final List<String> operations = new CopyOnWriteArrayList<>();
        private final CountDownLatch eventReceived = new CountDownLatch(1);
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicBoolean aborted = new AtomicBoolean();
        private final AtomicBoolean failOnCallback = new AtomicBoolean();
        private volatile Thread eventThread;

        @Override
        public void callback(String eventType, Object data) {
            if (failOnCallback.get()) {
                throw new IllegalStateException("send failed");
            }
            eventThread = Thread.currentThread();
            operations.add(eventType);
            eventReceived.countDown();
        }

        @Override
        public void finished() {
            operations.add("finished");
        }

        @Override
        public void abort() {
            aborted.set(true);
            active.set(false);
        }

        @Override
        public boolean isActive() {
            return active.get();
        }
    }
}
