package com.iflytek.astron.workflow.engine.v2.runtime;

import java.util.Map;

public record RunEvent(String eventId, long sequence, long timestamp, String type, String runId,
                       String nodeId, String executionId, String attemptId, String scopeId, int iterationNo,
                       Map<String, Object> data) {
}
