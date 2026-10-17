package com.iflytek.astron.workflow.engine.util;

import com.alibaba.ttl.threadpool.TtlExecutors;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.SseStreamCallback;
import com.iflytek.astron.workflow.exception.WorkflowOverloadedException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class WorkflowExecutor implements DisposableBean {

    private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int MAXIMUM_POOL_SIZE = Math.max(CORE_POOL_SIZE, 50);

    private final ThreadPoolExecutor threadPoolExecutor;
    private final ExecutorService executorService;
    private final WorkflowMetrics metrics;

    @Autowired
    public WorkflowExecutor(MeterRegistry registry, WorkflowMetrics metrics) {
        this(new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAXIMUM_POOL_SIZE,
                0,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                workflowThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()), registry, metrics);
    }

    WorkflowExecutor(ThreadPoolExecutor threadPoolExecutor, MeterRegistry registry, WorkflowMetrics metrics) {
        this.threadPoolExecutor = threadPoolExecutor;
        this.executorService = TtlExecutors.getTtlExecutorService(threadPoolExecutor);
        this.metrics = metrics;
        Gauge.builder("workflow.executor.active", threadPoolExecutor, ThreadPoolExecutor::getActiveCount)
                .register(registry);
        Gauge.builder("workflow.executor.pool.size", threadPoolExecutor, ThreadPoolExecutor::getPoolSize)
                .register(registry);
        Gauge.builder("workflow.executor.maximum", threadPoolExecutor, ThreadPoolExecutor::getMaximumPoolSize)
                .register(registry);
    }

    public void execute(SseStreamCallback callback, Runnable task) {
        try {
            executorService.execute(() -> {
                metrics.workflowStarted();
                try {
                    task.run();
                } finally {
                    metrics.workflowFinished();
                }
            });
        } catch (RejectedExecutionException e) {
            metrics.workflowRejected();
            callback.abort();
            throw new WorkflowOverloadedException(e);
        }
    }

    int activeCount() {
        return threadPoolExecutor.getActiveCount();
    }

    @Override
    public void destroy() {
        executorService.shutdownNow();
    }

    private static ThreadFactory workflowThreadFactory() {
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        AtomicInteger threadNumber = new AtomicInteger(1);
        return task -> {
            Thread thread = defaultFactory.newThread(task);
            thread.setDaemon(true);
            thread.setName("workflow-" + threadNumber.getAndIncrement());
            return thread;
        };
    }
}
