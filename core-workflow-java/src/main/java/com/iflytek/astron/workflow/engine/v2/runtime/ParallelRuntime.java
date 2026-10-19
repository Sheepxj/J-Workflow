package com.iflytek.astron.workflow.engine.v2.runtime;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class ParallelRuntime {
    private String forkInstanceId;
    private String forkNodeId;
    private String joinNodeId;
    private String parentScopeId;
    private boolean closed;
    private Map<String, String> branchScopes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> arrivals = new LinkedHashMap<>();
}
