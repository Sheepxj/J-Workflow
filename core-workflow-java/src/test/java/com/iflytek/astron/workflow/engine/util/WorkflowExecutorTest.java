package com.iflytek.astron.workflow.engine.util;

import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.SseStreamCallback;
import com.iflytek.astron.workflow.exception.WorkflowOverloadedException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkflowExecutorTest {

    @Test
    void workflowRunsOnNamedPlatformThread() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        WorkflowMetrics metrics = new WorkflowMetrics(registry);
        WorkflowExecutor workflowExecutor = new WorkflowExecutor(registry, metrics);
        SseVirtualThreadExecutor sseExecutor = new SseVirtualThreadExecutor(metrics);
        SseStreamCallback callback = new SseStreamCallback(new SseEmitter(), sseExecutor);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Thread> executingThread = new AtomicReference<>();

        try {
            workflowExecutor.execute(callback, () -> {
                executingThread.set(Thread.currentThread());
                completed.countDown();
            });
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            assertTrue(executingThread.get().getName().startsWith("workflow-"));
            assertFalse(executingThread.get().isVirtual());
        } finally {
            callback.abort();
            workflowExecutor.destroy();
            sseExecutor.destroy();
            registry.close();
        }
    }

    @Test
    void rejectionReturns503AndDoesNotLeakSseWriters() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        WorkflowMetrics metrics = new WorkflowMetrics(registry);
        ThreadPoolExecutor saturatedPool = new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.AbortPolicy());
        WorkflowExecutor workflowExecutor = new WorkflowExecutor(saturatedPool, registry, metrics);
        SseVirtualThreadExecutor sseExecutor = new SseVirtualThreadExecutor(metrics);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        SseStreamCallback acceptedCallback = new SseStreamCallback(new SseEmitter(), sseExecutor);
        List<SseStreamCallback> rejectedCallbacks = new ArrayList<>();

        try {
            workflowExecutor.execute(acceptedCallback, () -> {
                blockerStarted.countDown();
                try {
                    releaseBlocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(blockerStarted.await(2, TimeUnit.SECONDS));

            for (int index = 0; index < 100; index++) {
                SseStreamCallback rejectedCallback = new SseStreamCallback(new SseEmitter(), sseExecutor);
                rejectedCallbacks.add(rejectedCallback);
                WorkflowOverloadedException failure = assertThrows(
                        WorkflowOverloadedException.class,
                        () -> workflowExecutor.execute(rejectedCallback, () -> {
                        }));
                assertTrue(failure.getCause() instanceof java.util.concurrent.RejectedExecutionException);
            }

            for (SseStreamCallback callback : rejectedCallbacks) {
                assertTrue(callback.awaitTermination(2, TimeUnit.SECONDS));
                assertFalse(callback.isActive());
            }
            assertEquals(100, registry.get("workflow.executor.rejected").counter().count());
            ResponseStatus responseStatus = WorkflowOverloadedException.class.getAnnotation(ResponseStatus.class);
            assertEquals(HttpStatus.SERVICE_UNAVAILABLE, responseStatus.value());
            assertEquals(1, registry.get("sse.connections.active").gauge().value());
            assertEquals(1, registry.get("sse.virtual.threads").gauge().value());
        } finally {
            releaseBlocker.countDown();
            acceptedCallback.abort();
            acceptedCallback.awaitTermination(2, TimeUnit.SECONDS);
            workflowExecutor.destroy();
            sseExecutor.destroy();
            registry.close();
        }
    }
}
