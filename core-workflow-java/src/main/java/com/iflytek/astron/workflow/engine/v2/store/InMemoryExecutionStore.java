package com.iflytek.astron.workflow.engine.v2.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class InMemoryExecutionStore implements ExecutionStore {
    private final Map<String, AtomicReference<WorkflowRun>> runs = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;

    public InMemoryExecutionStore(ObjectMapper mapper) {
        this.mapper = mapper.copy().addMixIn(WorkflowRun.class, RuntimeCopy.class);
    }

    @Override
    public void create(WorkflowRun run) {
        if (runs.putIfAbsent(run.getRunId(), new AtomicReference<>(copy(run))) != null) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_RUN_STATE, "Run already exists");
        }
    }

    @Override
    public WorkflowRun get(String runId) {
        AtomicReference<WorkflowRun> state = find(runId);
        synchronized (state) {
            return copy(state.get());
        }
    }

    @Override
    public void update(String runId, Consumer<WorkflowRun> transition) {
        AtomicReference<WorkflowRun> state = find(runId);
        synchronized (state) {
            WorkflowRun previous = state.get();
            WorkflowRun candidate = copy(previous);
            transition.accept(candidate);
            candidate.setRevision(previous.getRevision() + 1);
            state.set(candidate);
        }
    }

    @Override
    public Set<String> activeRunIds() {
        Set<String> active = new HashSet<>();
        runs.forEach((id, state) -> {
            synchronized (state) {
                WorkflowRun run = state.get();
                if (run.getStatus() == RunStatus.RUNNING || run.getStatus() == RunStatus.PAUSING) {
                    active.add(id);
                }
            }
        });
        return Set.copyOf(active);
    }

    private AtomicReference<WorkflowRun> find(String runId) {
        AtomicReference<WorkflowRun> run = runs.get(runId);
        if (run == null) {
            throw new WorkflowExecutionException(WorkflowError.RUN_NOT_FOUND, "Run not found: " + runId);
        }
        return run;
    }

    private WorkflowRun copy(WorkflowRun run) {
        WorkflowRun copy = mapper.convertValue(run, WorkflowRun.class);
        copy.setDefinition(run.getDefinition());
        return copy;
    }

    private abstract static class RuntimeCopy {
        @JsonIgnore
        public abstract WorkflowDefinition getDefinition();
    }
}
