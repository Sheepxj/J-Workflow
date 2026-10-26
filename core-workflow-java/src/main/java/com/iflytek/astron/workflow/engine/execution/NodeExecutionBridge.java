package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;

@Component
public final class NodeExecutionBridge {
    private final Map<NodeTypeEnum, NodeExecutor> executors = new EnumMap<>(NodeTypeEnum.class);
    private final Clock clock;

    @Autowired
    public NodeExecutionBridge(List<NodeExecutor> executors) {
        this(executors, Clock.systemUTC());
    }

    public NodeExecutionBridge(List<NodeExecutor> executors, Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        for (NodeExecutor executor : executors) {
            if (this.executors.putIfAbsent(executor.getNodeType(), executor) != null) {
                throw new IllegalArgumentException("Duplicate node executor: " + executor.getNodeType());
            }
        }
    }

    public NodeActivation prepare(WorkflowRunContext run, String activationId, Node definition) {
        Objects.requireNonNull(activationId);
        synchronized (run) {
            NodeExecutionScope existing = run.activation(activationId);
            if (existing != null) {
                if (!existing.node().getId().equals(definition.getId())) {
                    throw new IllegalArgumentException("Activation already belongs to another node");
                }
                return existing.snapshot();
            }
            Node node = NodeExecutionScope.copyNode(definition);
            NodeExecutor executor = executor(node);
            Map<String, Map<String, Object>> variables = run.variables();
            VariablePool privatePool = new VariablePool();
            privatePool.restore(variables);
            Map<String, Object> inputs = JsonSnapshot.copy(executor.prepareInputs(new NodeState(node, privatePool, run.callback())));
            Map<String, NodeActivation.InputOrigin> origins = origins(run, node, privatePool, inputs);
            NodeExecutionScope scope = new NodeExecutionScope(run, node, activationId, run.nextActivation(node.getId()),
                    inputs, origins, variables, clock);
            run.register(scope);
            return scope.snapshot();
        }
    }

    public NodeActivation execute(WorkflowRunContext run, Node definition) {
        NodeActivation activation = prepare(run, UUID.randomUUID().toString(), definition);
        return execute(run, activation.executionId());
    }

    public NodeActivation execute(WorkflowRunContext run, String executionId) {
        NodeExecutionScope scope;
        synchronized (run) {
            scope = run.scope(executionId);
            if (!run.claim(scope)) {
                return scope.snapshot();
            }
        }
        NodeExecutor executor = executor(scope.node());
        NodeRunResult result;
        try {
            result = executor instanceof AbstractNodeExecutor ? executor.execute(scope.state())
                    : scope.invoke(scope.state(), null, executor::execute, (state, error) -> failure(error));
        } catch (Exception error) {
            result = scope.failedInvocation(error instanceof NodeCustomException custom ? custom
                    : new NodeCustomException(ErrorCode.NODE_RUN_ERROR, error.getMessage()));
        }
        scope.commit(result);
        return run.execution(executionId);
    }

    private NodeExecutor executor(Node node) {
        NodeExecutor executor = executors.get(node.getNodeType());
        if (executor == null) {
            throw new IllegalArgumentException("No executor for node type: " + node.getNodeType());
        }
        return executor;
    }

    public Set<NodeTypeEnum> nodeTypes() {
        return Set.copyOf(executors.keySet());
    }

    public void validateConfiguration(Node node) {
        executor(node).validateConfiguration(node);
    }

    private NodeRunResult failure(NodeCustomException error) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
        result.setError(error);
        return result;
    }

    private Map<String, NodeActivation.InputOrigin> origins(WorkflowRunContext run, Node node, VariablePool pool,
                                                           Map<String, Object> inputs) {
        Map<String, NodeActivation.InputOrigin> origins = new LinkedHashMap<>();
        inputs.keySet().forEach(name -> origins.put(name, new NodeActivation.InputOrigin(
                node.getNodeType() == NodeTypeEnum.START ? "RUN_INPUT" : "CUSTOM", null, name, null, true)));
        if (node.getData().getInputs() == null || node.getNodeType() == NodeTypeEnum.START) {
            return origins;
        }
        for (InputItem input : node.getData().getInputs()) {
            if (!inputs.containsKey(input.getName()) || input.getSchema() == null || input.getSchema().getValue() == null) {
                continue;
            }
            Value value = input.getSchema().getValue();
            if (!value.isReference()) {
                origins.put(input.getName(), new NodeActivation.InputOrigin("LITERAL", null, null, null, true));
            } else if (value.getContent() instanceof Map<?, ?> ref) {
                String nodeId = (String) ref.get("nodeId");
                String path = (String) ref.get("name");
                origins.put(input.getName(), new NodeActivation.InputOrigin("NODE_OUTPUT", nodeId, path,
                        run.lastExecution(nodeId), pool.contains(nodeId, path)));
            }
        }
        return origins;
    }
}
