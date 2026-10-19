package com.iflytek.astron.workflow.engine.v2.runtime;

import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class WorkflowRun {
    private String runId;
    private WorkflowDefinition definition;
    private String definitionHash;
    private RunStatus status = RunStatus.RUNNING;
    private long revision;
    private String ownerId;
    private long leaseUntil;
    private long startedAt;
    private int controlTransitions;
    private String rootScopeId;
    private Map<String, Object> inputs = Map.of();
    private Map<String, Object> outputs = Map.of();
    private Map<String, NodeExecution> executions = new LinkedHashMap<>();
    private Map<String, ExecutionScope> scopes = new LinkedHashMap<>();
    private Map<String, ParallelRuntime> parallels = new LinkedHashMap<>();
    private List<ControlToken> tokens = new ArrayList<>();
    private Set<String> consumedTokens = new HashSet<>();
    private Set<String> passedBreakpoints = new HashSet<>();
    private List<RunEvent> events = new ArrayList<>();
    private Map<String, Object> metadata = new HashMap<>();
    private WorkflowError errorCode;
    private String error;
    private boolean endReached;
}
