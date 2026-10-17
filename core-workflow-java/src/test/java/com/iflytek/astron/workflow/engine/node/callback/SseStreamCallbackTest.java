package com.iflytek.astron.workflow.engine.node.callback;

import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.util.SseVirtualThreadExecutor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SseStreamCallbackTest {

    private SimpleMeterRegistry registry;
    private SseVirtualThreadExecutor executor;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        executor = new SseVirtualThreadExecutor(new WorkflowMetrics(registry));
    }

    @AfterEach
    void tearDown() {
        executor.destroy();
        registry.close();
    }

    @Test
    void sendsAndCompletesOnOneVirtualThreadInOrder() throws Exception {
        RecordingSseEmitter emitter = new RecordingSseEmitter(3);
        SseStreamCallback callback = new SseStreamCallback(emitter, executor);

        callback.callback("first", "one");
        callback.callback("second", "two");
        callback.callback("third", "three");
        callback.finished();
        callback.complete();

        assertTrue(emitter.awaitCompletion());
        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertEquals(3, emitter.sendCount.get());
        assertEquals(1, emitter.maxConcurrentSends.get());
        assertTrue(emitter.threads.stream().allMatch(Thread::isVirtual));
        assertTrue(emitter.threads.stream().allMatch(thread -> thread.getName().startsWith("sse-vt-")));
        assertEquals(1, emitter.threads.stream().map(Thread::threadId).distinct().count());
        assertEquals(List.of("send", "send", "send", "complete"), emitter.operations);
        assertEquals(0, gauge("sse.connections.active"));
        assertEquals(0, gauge("sse.virtual.threads"));
    }

    @Test
    void sendsQueuedEventsBeforeCompletingWithError() throws Exception {
        RecordingSseEmitter emitter = new RecordingSseEmitter(1);
        SseStreamCallback callback = new SseStreamCallback(emitter, executor);
        IllegalStateException failure = new IllegalStateException("failed");

        callback.callback("error", "failed");
        callback.completeWithError(failure);

        assertTrue(emitter.awaitCompletion());
        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertSame(failure, emitter.error);
        assertEquals(List.of("send", "error"), emitter.operations);
    }

    @Test
    void completionTimeoutAndErrorCallbacksStopWriter() throws Exception {
        assertLifecycleSignalStopsWriter(LifecycleSignal.COMPLETION);
        assertLifecycleSignalStopsWriter(LifecycleSignal.TIMEOUT);
        assertLifecycleSignalStopsWriter(LifecycleSignal.ERROR);
    }

    @Test
    void abortIsIdempotentAndInterruptsWriter() throws Exception {
        RecordingSseEmitter emitter = new RecordingSseEmitter(0);
        SseStreamCallback callback = new SseStreamCallback(emitter, executor);

        callback.abort();
        callback.abort();

        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(callback.isActive());
        assertEquals(0, gauge("sse.connections.active"));
        assertEquals(0, gauge("sse.virtual.threads"));
    }

    @Test
    void fullQueueBackpressuresProducerAndDisconnectReleasesIt() throws Exception {
        BlockingSseEmitter emitter = new BlockingSseEmitter();
        SseStreamCallback callback = new SseStreamCallback(emitter, executor, 1);
        callback.callback("first", "one");
        assertTrue(emitter.sendStarted.await(2, TimeUnit.SECONDS));
        callback.callback("second", "two");

        Thread producer = Thread.ofPlatform().start(() -> callback.callback("third", "three"));
        waitUntil(() -> counter("sse.queue.backpressure") > 0, 2, TimeUnit.SECONDS);
        assertTrue(producer.isAlive());

        emitter.fireTimeout();
        producer.join(2000);
        emitter.releaseSend.countDown();

        assertFalse(producer.isAlive());
        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(callback.isActive());
    }

    private void assertLifecycleSignalStopsWriter(LifecycleSignal signal) throws Exception {
        RecordingSseEmitter emitter = new RecordingSseEmitter(0);
        SseStreamCallback callback = new SseStreamCallback(emitter, executor);
        signal.fire(emitter);
        assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(callback.isActive());
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    private double counter(String name) {
        return registry.get(name).counter().count();
    }

    private void waitUntil(Check check, long timeout, TimeUnit unit) throws Exception {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (!check.evaluate() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(check.evaluate());
    }

    private enum LifecycleSignal {
        COMPLETION {
            @Override
            void fire(RecordingSseEmitter emitter) {
                emitter.fireCompletion();
            }
        },
        TIMEOUT {
            @Override
            void fire(RecordingSseEmitter emitter) {
                emitter.fireTimeout();
            }
        },
        ERROR {
            @Override
            void fire(RecordingSseEmitter emitter) {
                emitter.fireError(new IOException("disconnected"));
            }
        };

        abstract void fire(RecordingSseEmitter emitter);
    }

    @FunctionalInterface
    private interface Check {
        boolean evaluate();
    }

    private static class RecordingSseEmitter extends SseEmitter {

        private final AtomicInteger sendCount = new AtomicInteger();
        private final AtomicInteger activeSends = new AtomicInteger();
        private final AtomicInteger maxConcurrentSends = new AtomicInteger();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final List<String> operations = new CopyOnWriteArrayList<>();
        private final CountDownLatch sends;
        private final CountDownLatch completed = new CountDownLatch(1);
        private volatile Throwable error;
        private volatile Runnable completionCallback;
        private volatile Runnable timeoutCallback;
        private volatile Consumer<Throwable> errorCallback;

        private RecordingSseEmitter(int expectedSends) {
            sends = new CountDownLatch(expectedSends);
        }

        @Override
        public void send(SseEventBuilder builder) throws IOException {
            int active = activeSends.incrementAndGet();
            maxConcurrentSends.accumulateAndGet(active, Math::max);
            threads.add(Thread.currentThread());
            operations.add("send");
            sendCount.incrementAndGet();
            activeSends.decrementAndGet();
            sends.countDown();
        }

        @Override
        public synchronized void complete() {
            threads.add(Thread.currentThread());
            operations.add("complete");
            completed.countDown();
        }

        @Override
        public synchronized void completeWithError(Throwable error) {
            threads.add(Thread.currentThread());
            operations.add("error");
            this.error = error;
            completed.countDown();
        }

        @Override
        public void onCompletion(Runnable callback) {
            completionCallback = callback;
        }

        @Override
        public void onTimeout(Runnable callback) {
            timeoutCallback = callback;
        }

        @Override
        public void onError(Consumer<Throwable> callback) {
            errorCallback = callback;
        }

        private boolean awaitCompletion() throws InterruptedException {
            return sends.await(2, TimeUnit.SECONDS) && completed.await(2, TimeUnit.SECONDS);
        }

        void fireCompletion() {
            completionCallback.run();
        }

        void fireTimeout() {
            timeoutCallback.run();
        }

        void fireError(Throwable failure) {
            errorCallback.accept(failure);
        }
    }

    private static final class BlockingSseEmitter extends RecordingSseEmitter {

        private final CountDownLatch sendStarted = new CountDownLatch(1);
        private final CountDownLatch releaseSend = new CountDownLatch(1);

        private BlockingSseEmitter() {
            super(0);
        }

        @Override
        public void send(SseEventBuilder builder) throws IOException {
            sendStarted.countDown();
            try {
                releaseSend.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }
}
