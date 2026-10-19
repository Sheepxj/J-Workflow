package com.iflytek.astron.workflow.engine.v2.definition;

public record ExecutionLimits(int maxNodeExecutions, long maxDurationMs, int maxControlTransitions) {
    public ExecutionLimits {
        if (maxControlTransitions == 0) {
            maxControlTransitions = 10000;
        }
    }

    public static ExecutionLimits defaults() {
        return new ExecutionLimits(1000, 300000, 10000);
    }
}
