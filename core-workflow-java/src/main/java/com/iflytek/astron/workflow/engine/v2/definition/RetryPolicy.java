package com.iflytek.astron.workflow.engine.v2.definition;

public record RetryPolicy(int maxAttempts, long delayMs, long timeoutMs) {
    public static RetryPolicy defaults() {
        return new RetryPolicy(1, 0, 60000);
    }
}
