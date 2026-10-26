package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;

import java.util.List;
import java.util.Map;

public record NodeActivation(String runId, String executionId, String activationId, String nodeId, int activationNo,
                             Status status, Map<String, Object> inputSnapshot, Map<String, InputOrigin> inputOrigins, NodeRunResult result,
                             List<Attempt> attempts) {
    public NodeActivation {
        inputSnapshot = JsonSnapshot.copy(inputSnapshot);
        inputOrigins = Map.copyOf(inputOrigins);
        result = result == null ? null : JsonSnapshot.result(result);
        attempts = List.copyOf(attempts);
    }

    @Override
    public NodeRunResult result() {
        return result == null ? null : JsonSnapshot.result(result);
    }

    public enum Status {
        READY, RUNNING, SUCCEEDED, FAILED, CANCELLED
    }

    public enum AttemptStatus {
        RUNNING, SUCCEEDED, FAILED, TIMED_OUT, CANCELLED
    }

    public record Attempt(String attemptId, int attemptNo, AttemptStatus status, long startedAt,
                          long deadline, long finishedAt, Integer errorCode, String error) {
    }

    public record InputOrigin(String source, String nodeId, String path, String executionId, boolean present) {
    }
}
