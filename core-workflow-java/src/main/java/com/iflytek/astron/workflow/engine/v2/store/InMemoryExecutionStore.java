package com.iflytek.astron.workflow.engine.v2.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class InMemoryExecutionStore implements ExecutionStore {
    private final Map<String, WorkflowRun> runs = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;

    public InMemoryExecutionStore(ObjectMapper mapper) {
        this.mapper = mapper.copy();
    }

    @Override
    public void create(WorkflowRun run) {
        if (runs.putIfAbsent(run.getRunId(), copy(run)) != null) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_RUN_STATE, "Run already exists");
        }
    }

    @Override
    public WorkflowRun get(String runId) {
        WorkflowRun run = find(runId);
        synchronized (run) {
            return copy(run);
        }
    }

    @Override
    public void update(String runId, Consumer<WorkflowRun> transition) {
        WorkflowRun run = find(runId);
        synchronized (run) {
            transition.accept(run);
            run.setRevision(run.getRevision() + 1);
        }
    }

    @Override
    public Set<String> activeRunIds() {
        Set<String> active = new HashSet<>();
        runs.forEach((id, run) -> {
            synchronized (run) {
                if (run.getStatus() == RunStatus.RUNNING || run.getStatus() == RunStatus.PAUSING) {
                    active.add(id);
                }
            }
        });
        return Set.copyOf(active);
    }

    private WorkflowRun find(String runId) {
        WorkflowRun run = runs.get(runId);
        if (run == null) {
            throw new WorkflowExecutionException(WorkflowError.RUN_NOT_FOUND, "Run not found: " + runId);
        }
        return run;
    }

    private WorkflowRun copy(WorkflowRun run) {
        return mapper.convertValue(run, WorkflowRun.class);
    }
}
