package com.iflytek.astron.workflow.engine.v2.executor;

import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;

import java.util.LinkedHashMap;
import java.util.Map;

public final class MockTaskExecutor implements NodeTaskExecutor {
    @Override
    public String nodeType() {
        return "MOCK";
    }

    @Override
    public Map<String, Object> execute(NodeInvocation invocation) {
        Map<String, Object> config = invocation.node().config();
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
