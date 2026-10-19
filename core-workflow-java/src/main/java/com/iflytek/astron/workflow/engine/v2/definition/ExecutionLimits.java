package com.iflytek.astron.workflow.engine.v2.definition;

public record ExecutionLimits(int maxNodeExecutions, long maxDurationMs, Integer maxControlTransitions,
                               Integer maxConcurrentAttempts) {
    public ExecutionLimits {
        if (maxControlTransitions == null) {
            maxControlTransitions = 10000;
        }
        if (maxConcurrentAttempts == null) {
            maxConcurrentAttempts = 1;
        }
    }

    public static ExecutionLimits defaults() {
        return new ExecutionLimits(1000, 300000, 10000, 1);
    }
}
