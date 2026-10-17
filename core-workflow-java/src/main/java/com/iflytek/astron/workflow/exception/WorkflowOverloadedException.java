package com.iflytek.astron.workflow.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class WorkflowOverloadedException extends RuntimeException {

    public WorkflowOverloadedException(Throwable cause) {
        super("Workflow execution capacity is exhausted", cause);
    }
}
