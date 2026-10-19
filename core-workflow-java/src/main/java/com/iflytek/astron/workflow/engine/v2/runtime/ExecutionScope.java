package com.iflytek.astron.workflow.engine.v2.runtime;

import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ScopeType;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ExecutionScope {
    private String scopeId;
    private String parentScopeId;
    private ScopeType type;
    private String loopId;
    private String branchId;
    private int iterationNo;
    private int completedIterations;
    private boolean closed;
    private Map<String, Object> localState = Map.of();
    private Map<String, String> nodeExecutions = new HashMap<>();
    private Map<String, Map<String, Object>> loopOutputs = new HashMap<>();
    private Map<String, String> loopOutputSources = new HashMap<>();
}
