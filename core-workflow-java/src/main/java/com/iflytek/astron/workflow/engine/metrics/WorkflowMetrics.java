package com.iflytek.astron.workflow.engine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class WorkflowMetrics {

    private final AtomicInteger activeWorkflows = new AtomicInteger();
    private final AtomicInteger activeSseConnections = new AtomicInteger();
    private final AtomicInteger activeWorkflowStreamThreads = new AtomicInteger();
    private final AtomicInteger activeSseThreads = new AtomicInteger();
    private final Counter rejectedWorkflows;
    private final Counter sseBackpressure;

    public WorkflowMetrics(MeterRegistry registry) {
        Gauge.builder("workflow.executions.active", activeWorkflows, AtomicInteger::get).register(registry);
        Gauge.builder("sse.connections.active", activeSseConnections, AtomicInteger::get).register(registry);
        Gauge.builder("workflow.stream.virtual.threads", activeWorkflowStreamThreads, AtomicInteger::get)
                .register(registry);
        Gauge.builder("sse.virtual.threads", activeSseThreads, AtomicInteger::get).register(registry);
        rejectedWorkflows = Counter.builder("workflow.executor.rejected").register(registry);
        sseBackpressure = Counter.builder("sse.queue.backpressure").register(registry);
    }

    public void workflowStarted() {
        activeWorkflows.incrementAndGet();
    }

    public void workflowFinished() {
        activeWorkflows.decrementAndGet();
    }

    public void workflowRejected() {
        rejectedWorkflows.increment();
    }

    public void sseConnectionOpened() {
        activeSseConnections.incrementAndGet();
    }

    public void sseConnectionClosed() {
        activeSseConnections.decrementAndGet();
    }

    public void workflowStreamThreadStarted() {
        activeWorkflowStreamThreads.incrementAndGet();
    }

    public void workflowStreamThreadFinished() {
        activeWorkflowStreamThreads.decrementAndGet();
    }

    public void sseThreadStarted() {
        activeSseThreads.incrementAndGet();
    }

    public void sseThreadFinished() {
        activeSseThreads.decrementAndGet();
    }

    public void sseBackpressure() {
        sseBackpressure.increment();
    }
}
