package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;

public record WorkflowExecutionLimits(int maxNodeActivations, int maxTotalActivations, long maxDurationMs) {
    public WorkflowExecutionLimits {
        if (maxNodeActivations <= 0 || maxTotalActivations <= 0 || maxDurationMs <= 0) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Workflow execution limits must be positive");
        }
    }

    public static WorkflowExecutionLimits defaults() {
        return new WorkflowExecutionLimits(100, 10000, 300000);
    }
}
