package com.iflytek.astron.workflow.engine.util;

import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
public class SseVirtualThreadExecutor implements DisposableBean {

    private final ExecutorService executorService = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("sse-vt-", 1).factory());
    private final WorkflowMetrics metrics;

    public SseVirtualThreadExecutor(WorkflowMetrics metrics) {
        this.metrics = metrics;
    }

    public Future<?> submit(Runnable task) {
        return executorService.submit(() -> {
            metrics.sseThreadStarted();
            try {
                task.run();
            } finally {
                metrics.sseThreadFinished();
            }
        });
    }

    public void connectionOpened() {
        metrics.sseConnectionOpened();
    }

    public void connectionClosed() {
        metrics.sseConnectionClosed();
    }

    public void backpressure() {
        metrics.sseBackpressure();
    }

    @Override
    public void destroy() {
        executorService.shutdownNow();
    }
}
