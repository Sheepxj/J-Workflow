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

/**
 * 节点执行桥：把「调度层」与「节点执行器」连接起来。
 *
 * <p>职责：
 * <ul>
 *   <li>按节点类型分派到对应的 {@link NodeExecutor}</li>
 *   <li>为一次激活创建 {@link NodeExecutionScope}（冻结入参、变量、历史）并登记到 run 上下文</li>
 *   <li>驱动作用域完成「抢占 -> 执行 -> 提交结果」的完整生命周期</li>
 * </ul>
 */
@Component
public final class NodeExecutionBridge {
    // 节点类型 -> 执行器；启动时装配，运行期只读
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

    /**
     * 为一条控制令牌准备一次节点执行：
     * 同一 activationId 已存在则直接复用（重放幂等），否则新建作用域并登记。
     */
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
            // 冻结激活时刻的变量快照，后续其他节点的写入不会影响本次执行
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

    // 便捷入口：临时造一个随机 activationId 并立即执行
    public NodeActivation execute(WorkflowRunContext run, Node definition) {
        NodeActivation activation = prepare(run, UUID.randomUUID().toString(), definition);
        return execute(run, activation.executionId());
    }

    /**
     * 执行已准备的节点：先 claim 抢占执行位，再调用执行器，
     * 最终把结果 commit 回 run（决定是否回写变量池与历史）。
     */
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
            // 新式执行器走作用域的 invoke（带重试/超时/副作用门控），旧式执行器直接调用
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

    // 执行器抛异常时的统一兜底结果（中断态，交由上层转成 run 失败）
    private NodeRunResult failure(NodeCustomException error) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
        result.setError(error);
        return result;
    }

    /**
     * 记录每个入参的来源：运行入参 / 字面量 / 上游节点输出，
     * 并标记该引用当前是否真的有值（用于判断上游是否已产出该字段）。
     */
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
