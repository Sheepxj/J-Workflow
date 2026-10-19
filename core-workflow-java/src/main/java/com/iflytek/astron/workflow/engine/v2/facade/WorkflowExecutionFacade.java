package com.iflytek.astron.workflow.engine.v2.facade;

import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.ExecutionStore;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class WorkflowExecutionFacade implements AutoCloseable {
    private final WorkflowScheduler scheduler;
    private final ExecutionStore store;
    private final ScheduledExecutorService timer;

    public WorkflowExecutionFacade(WorkflowScheduler scheduler, ExecutionStore store) {
        this.scheduler = scheduler;
        this.store = store;
        this.timer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon(true).name("workflow-v2-timer").factory());
        timer.scheduleWithFixedDelay(this::advance, 10, 10, TimeUnit.MILLISECONDS);
    }

    public String start(WorkflowDefinition definition, Map<String, Object> inputs) {
        return scheduler.start(definition, inputs);
    }

    public WorkflowRun get(String runId) {
        return scheduler.get(runId);
    }

    public void pause(String runId) {
        scheduler.pause(runId);
    }

    public void resume(String runId) {
        scheduler.resume(runId);
    }

    public void cancel(String runId) {
        scheduler.cancel(runId);
    }

    public void resolveInterrupted(String runId, String executionId, boolean retry, Map<String, Object> outputs) {
        scheduler.resolveInterrupted(runId, executionId, retry, outputs);
    }

    private void advance() {
        try {
            for (String runId : store.activeRunIds()) {
                try {
                    scheduler.tick(runId);
                } catch (RuntimeException e) {
                    log.error("Cannot advance workflow run {}", runId, e);
                }
            }
        } catch (RuntimeException e) {
            log.error("Cannot list active workflow runs", e);
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
