package com.iflytek.astron.workflow.engine.v2.binding;

import com.iflytek.astron.workflow.engine.v2.definition.InputBinding;
import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.runtime.ExecutionScope;
import com.iflytek.astron.workflow.engine.v2.runtime.NodeExecution;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ExecutionStatus;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.ScopeType;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InputResolver {
    private static final Pattern SEGMENT = Pattern.compile("^([^\\[\\]]+)((?:\\[\\d+\\])*)$");
    private static final Pattern INDEX = Pattern.compile("\\[(\\d+)\\]");

    public Map<String, Object> resolve(Map<String, InputBinding> bindings, WorkflowRun run, String scopeId) {
        Map<String, Object> result = new LinkedHashMap<>();
        bindings.forEach((name, binding) -> result.put(name, resolve(binding, run, scopeId)));
        return JsonValues.snapshot(result);
    }

    private Object resolve(InputBinding binding, WorkflowRun run, String scopeId) {
        ExecutionScope scope = run.getScopes().get(scopeId);
        Object source = switch (binding.source()) {
            case LITERAL -> binding.value();
            case RUN_INPUT -> run.getInputs();
            case LOOP_STATE -> loopScope(run, scopeId, binding.loopId()).getLocalState();
            case LOOP_OUTPUT -> {
                if (!scope.getLoopOutputs().containsKey(binding.loopId())) {
                    throw unavailable("Loop output unavailable: " + binding.loopId());
                }
                yield scope.getLoopOutputs().get(binding.loopId());
            }
            case NODE_OUTPUT -> {
                if ("CURRENT_ITERATION".equals(binding.scope()) && scope.getType() != ScopeType.ITERATION) {
                    throw unavailable("CURRENT_ITERATION used outside an iteration");
                }
                if ("PARENT".equals(binding.scope())) {
                    scope = run.getScopes().get(scope.getParentScopeId());
                    if (scope != null && scope.getType() == ScopeType.LOOP) {
                        scope = run.getScopes().get(scope.getParentScopeId());
                    }
                }
                String executionId = scope == null ? null : scope.getNodeExecutions().get(binding.nodeId());
                NodeExecution execution = executionId == null ? null : run.getExecutions().get(executionId);
                if (execution == null || execution.getStatus() != ExecutionStatus.SUCCEEDED) {
                    throw unavailable("Node output unavailable in requested scope: " + binding.nodeId());
                }
                yield execution.getOutputs();
            }
        };
        return binding.source() == InputBinding.Source.LITERAL ? source : path(source, binding.path());
    }

    public ExecutionScope loopScope(WorkflowRun run, String scopeId, String loopId) {
        ExecutionScope scope = run.getScopes().get(scopeId);
        while (scope != null) {
            if (scope.getType() == ScopeType.LOOP && (loopId == null || Objects.equals(loopId, scope.getLoopId()))) {
                return scope;
            }
            scope = scope.getParentScopeId() == null ? null : run.getScopes().get(scope.getParentScopeId());
        }
        throw unavailable("Loop state unavailable: " + loopId);
    }

    public Object path(Object source, String path) {
        if (path == null || path.isBlank()) {
            return source;
        }
        Object current = source;
        for (String part : path.split("\\.", -1)) {
            Matcher segment = SEGMENT.matcher(part);
            if (!segment.matches() || !(current instanceof Map<?, ?> map) || !map.containsKey(segment.group(1))) {
                throw unavailable("Missing or invalid value path: " + path);
            }
            current = map.get(segment.group(1));
            Matcher indexes = INDEX.matcher(segment.group(2));
            while (indexes.find()) {
                int index;
                try {
                    index = Integer.parseInt(indexes.group(1));
                } catch (NumberFormatException e) {
                    throw unavailable("Invalid list index: " + path);
                }
                if (!(current instanceof List<?> list) || index >= list.size()) {
                    throw unavailable("Missing list element: " + path);
                }
                current = list.get(index);
            }
        }
        return current;
    }

    private WorkflowExecutionException unavailable(String message) {
        return new WorkflowExecutionException(WorkflowError.INPUT_UNAVAILABLE, message);
    }
}
