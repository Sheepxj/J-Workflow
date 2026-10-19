package com.iflytek.astron.workflow.engine.v2.runtime;

import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class NodeExecution {
    private String executionId;
    private String nodeId;
    private String scopeId;
    private String activationId;
    private ExecutionStatus status = ExecutionStatus.READY;
    private Map<String, Object> inputSnapshot = Map.of();
    private Map<String, Object> outputs = Map.of();
    private List<ExecutionAttempt> attempts = new ArrayList<>();
    private long retryAt;
    private WorkflowError errorCode;
    private String error;
}
