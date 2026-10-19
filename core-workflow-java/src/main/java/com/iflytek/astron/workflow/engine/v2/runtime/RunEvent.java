package com.iflytek.astron.workflow.engine.v2.runtime;

import java.util.Map;
import java.util.List;

public record RunEvent(String eventId, long sequence, long timestamp, String type, String runId,
                       String nodeId, String executionId, String attemptId, String scopeId, int iterationNo,
                       Map<String, Object> data, List<String> scopePath) {
    public RunEvent {
        scopePath = scopePath == null ? List.of() : List.copyOf(scopePath);
    }
}
