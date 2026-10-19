package com.iflytek.astron.workflow.engine.v2.executor;

import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

public record NodeInvocation(String runId, String executionId, String attemptId, int attemptNo,
                             String scopeId, int iterationNo, NodeDefinition node, Map<String, Object> inputs,
                             long deadline, BooleanSupplier cancelled, BiConsumer<String, Map<String, Object>> eventSink) {
    public String idempotencyKey(String effectName) {
        return runId + ":" + executionId + ":" + effectName;
    }
}
