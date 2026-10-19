package com.iflytek.astron.workflow.engine.v2.runtime;

import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.AttemptStatus;
import lombok.Data;

@Data
public class ExecutionAttempt {
    private String attemptId;
    private int attemptNo;
    private AttemptStatus status = AttemptStatus.READY;
    private long startedAt;
    private long deadline;
    private long finishedAt;
    private String error;
}
