package com.iflytek.astron.workflow.engine.v2.executor;

import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class MockTaskExecutor implements NodeTaskExecutor {
    @Override
    public String nodeType() {
        return "MOCK";
    }

    @Override
    public Map<String, Object> execute(NodeInvocation invocation) throws InterruptedException {
        Map<String, Object> config = invocation.node().config();
        long delay = ((Number) config.getOrDefault("delayMs", 0)).longValue();
        if (delay < 0 || delay > 60000) {
            throw new IllegalArgumentException("MOCK delayMs must be between 0 and 60000");
        }
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        while (System.nanoTime() < until) {
            if (invocation.cancelled().getAsBoolean()) {
                throw new WorkflowExecutionException(WorkflowError.NODE_FAILED, "Mock invocation cancelled");
            }
            Thread.sleep(Math.min(50, Math.max(1, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime()))));
        }
        int failAttempts = ((Number) config.getOrDefault("failAttempts", 0)).intValue();
        if (invocation.attemptNo() <= failAttempts) {
            throw new WorkflowExecutionException(WorkflowError.NODE_FAILED, "Configured mock attempt failure");
        }
        String operation = String.valueOf(config.getOrDefault("operation", "ECHO"));
        return switch (operation) {
            case "ECHO" -> invocation.inputs();
            case "GENERATE" -> Map.of("text", "draft-" + invocation.iterationNo());
            case "VALIDATE" -> Map.of("passed", invocation.iterationNo()
                    >= ((Number) config.getOrDefault("passesOnIteration", 3)).intValue());
            case "CONSTANT" -> {
                Object value = config.get("outputs");
                if (!(value instanceof Map<?, ?> map)) {
                    throw new IllegalArgumentException("MOCK CONSTANT requires outputs object");
                }
                Map<String, Object> outputs = new LinkedHashMap<>();
                map.forEach((key, item) -> outputs.put((String) key, item));
                yield JsonValues.snapshot(outputs);
            }
            default -> throw new IllegalArgumentException("Unknown MOCK operation: " + operation);
        };
    }
}
