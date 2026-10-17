package com.iflytek.astron.workflow.engine.util;

import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
public class WorkflowStreamVirtualThreadExecutor implements DisposableBean {

    private final ExecutorService executorService = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("workflow-stream-vt-", 1).factory());
    private final WorkflowMetrics metrics;

    public WorkflowStreamVirtualThreadExecutor(WorkflowMetrics metrics) {
        this.metrics = metrics;
    }

    public Future<?> submit(Runnable task) {
        return executorService.submit(() -> {
            metrics.workflowStreamThreadStarted();
            try {
                task.run();
            } finally {
                metrics.workflowStreamThreadFinished();
            }
        });
    }

    @Override
    public void destroy() {
        executorService.shutdownNow();
    }
}
