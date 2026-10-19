package com.iflytek.astron.workflow.engine.v2.runtime;

import lombok.Getter;

@Getter
public class WorkflowExecutionException extends RuntimeException {
    private final WorkflowError code;

    public WorkflowExecutionException(WorkflowError code, String message) {
        super(message);
        this.code = code;
    }
}
