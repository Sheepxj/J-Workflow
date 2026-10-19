package com.iflytek.astron.workflow.engine.v2.store;

import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;

import java.util.Set;
import java.util.function.Consumer;

public interface ExecutionStore {
    void create(WorkflowRun run);

    WorkflowRun get(String runId);

    void update(String runId, Consumer<WorkflowRun> transition);

    Set<String> activeRunIds();
}
