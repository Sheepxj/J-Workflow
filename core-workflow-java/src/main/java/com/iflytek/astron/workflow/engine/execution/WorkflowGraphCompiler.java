package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.node.impl.condition.IfElseNodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 工作流图编译器：把编辑器产出的 {@link WorkflowDSL} 转成运行时使用的 {@link WorkflowGraph}。
 *
 * <p>职责有两块：
 * <ul>
 *   <li>静态校验：结构完整性、节点类型与配置、边与分支语义、输入绑定、连通性，尽量在开跑前失败</li>
 *   <li>预编译：产出节点表 + 出边邻接表，并把「正常分支 / 失败分支」等语义提前判定好，运行时只做查表</li>
 * </ul>
 */
public final class WorkflowGraphCompiler {
    // 用来判断节点类型是否有对应执行器，并复用执行器自身的配置校验
    private final NodeExecutionBridge bridge;

    public WorkflowGraphCompiler(NodeExecutionBridge bridge) {
        this.bridge = bridge;
    }

    /**
     * 编译入口。校验不通过直接抛 {@link NodeCustomException}，不会返回半成品图。
     */
    public WorkflowGraph compile(WorkflowDSL definition) {
        // 1. 顶层结构：flowId、节点、边缺一不可
        require(definition != null && present(definition.getFlowId()), "Workflow flowId required");
        require(definition.getNodes() != null && !definition.getNodes().isEmpty(), "Workflow nodes required");
        require(definition.getEdges() != null, "Workflow edges required");
        Map<String, Node> nodes = new LinkedHashMap<>();
        Map<String, List<WorkflowGraph.Transition>> outgoing = new LinkedHashMap<>();
        Map<String, List<String>> incoming = new LinkedHashMap<>();
        // 2. 建节点表：复制节点定义、校验类型与配置，并为每个节点预留出入边槽位
        for (Node original : definition.getNodes()) {
            require(original != null && present(original.getId()) && original.getData() != null, "Node ID/data required");
            require(original.getData().getNodeMeta() != null && original.getData().getNodeParam() != null, "Node metadata/parameters required: " + original.getId());
            require(original.getNodeType() != null && bridge.nodeTypes().contains(original.getNodeType()), "Unsupported node type: " + original.getId());
            Node node = NodeExecutionScope.copyNode(original);
            require(nodes.putIfAbsent(node.getId(), node) == null, "Duplicate node: " + node.getId());
            bridge.validateConfiguration(node);
            outgoing.put(node.getId(), new ArrayList<>());
            incoming.put(node.getId(), new ArrayList<>());
        }
        // 3. 图必须恰好有一个 START 和一个 END
        List<Node> starts = nodes.values().stream().filter(node -> node.getNodeType() == NodeTypeEnum.START).toList();
        List<Node> ends = nodes.values().stream().filter(node -> node.getNodeType() == NodeTypeEnum.END).toList();
        require(starts.size() == 1 && ends.size() == 1, "Exactly one START and END required");
        Set<EdgeKey> keys = new HashSet<>();
        // 4. 建出边表：校验端点与重复边，并把 sourceHandle 归约成正常分支/失败分支
        for (int index = 0; index < definition.getEdges().size(); index++) {
            Edge edge = definition.getEdges().get(index);
            require(edge != null && nodes.containsKey(edge.getSource()) && nodes.containsKey(edge.getTarget()), "Unknown edge endpoint at index " + index);
            String handle = present(edge.getSourceHandle()) ? edge.getSourceHandle() : null;
            require(keys.add(new EdgeKey(edge.getSource(), edge.getTarget(), handle)), "Duplicate workflow edge at index " + index);
            Node source = nodes.get(edge.getSource());
            // INTENT_CHAIN 前缀的 handle 表示失败分支；IF_ELSE 自己声明的出口不算失败
            boolean failure = handle != null && handle.startsWith(NodeTypeEnum.CONDITION_SWITCH_INTENT_CHAIN.getValue());
            if (source.getNodeType() == NodeTypeEnum.IF_ELSE && handle != null && IfElseNodeExecutor.handles(source).contains(handle)) {
                failure = false;
            } else if (!failure) {
                require(source.getNodeType() != NodeTypeEnum.IF_ELSE && (handle == null
                        || handle.startsWith(NodeTypeEnum.CONDITION_SWITCH_NORMAL_ONE_OF.getValue())), "Unknown sourceHandle at edge index " + index);
            }
            outgoing.get(edge.getSource()).add(new WorkflowGraph.Transition(index, edge.getSource(), edge.getTarget(), handle, failure));
            incoming.get(edge.getTarget()).add(edge.getSource());
        }
        String startId = starts.getFirst().getId();
        String endId = ends.getFirst().getId();
        // 5. START 不能有入边，END 不能有出边
        require(incoming.get(startId).isEmpty(), "START cannot have incoming edges");
        require(outgoing.get(endId).isEmpty(), "END cannot have outgoing edges");
        for (Node node : nodes.values()) {
            validateBindings(node, nodes);
            List<WorkflowGraph.Transition> edges = outgoing.get(node.getId());
            if (node.getNodeType() == NodeTypeEnum.IF_ELSE) {
                List<String> handles = IfElseNodeExecutor.handles(node);
                for (String handle : handles) {
                    long count = edges.stream().filter(edge -> !edge.failure() && handle.equals(edge.handle())).count();
                    require(count >= 1, "IF_ELSE outlet has no edge: " + handle);
                }
            }
        }
        // 7. 连通性：从 START 出发必须能走到所有节点
        require(reachable(startId, outgoing).size() == nodes.size(), "Workflow contains nodes unreachable from START");
        // 反向可达：每个节点在结构上都必须有路径到 END，否则运行时会停滞
        Set<String> reachingEnd = new HashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        pending.add(endId);
        while (!pending.isEmpty()) {
            String current = pending.removeFirst();
            if (reachingEnd.add(current)) {
                pending.addAll(incoming.get(current));
            }
        }
        require(reachingEnd.size() == nodes.size(), "Every node must have a structural path to END");
        // 8. 出边列表冻结为不可变，得到运行时只读的图定义（DSL 的 uuid 作为 chatId 传入）
        Map<String, List<WorkflowGraph.Transition>> routes = new LinkedHashMap<>();
        outgoing.forEach((nodeId, edges) -> routes.put(nodeId, List.copyOf(edges)));
        return new WorkflowGraph(definition.getFlowId(), definition.getUuid(), startId, endId, nodes, routes);
    }

    // 校验输入绑定：名字唯一、引用必须指向真实节点与字段，并检查重试配置自洽
    private void validateBindings(Node node, Map<String, Node> nodes) {
        List<InputItem> inputs = node.getNodeType() == NodeTypeEnum.IF_ELSE ? IfElseNodeExecutor.requiredInputs(node)
                : node.getData().getInputs();
        Set<String> names = new HashSet<>();
        if (node.getData().getRetryConfig() != null) {
            Integer retries = node.getData().getRetryConfig().getMaxRetries();
            require(!Boolean.TRUE.equals(node.getData().getRetryConfig().getShouldRetry()) || retries != null && retries >= 0,
                    "Non-negative maxRetries required when retry is enabled: " + node.getId());
        }
        if (inputs == null) {
            return;
        }
        for (InputItem input : inputs) {
            require(input != null && present(input.getName()) && names.add(input.getName()), "Missing or duplicate input name: " + node.getId());
            require(input.getSchema() != null && input.getSchema().getValue() != null, "Input schema/value required: " + input.getName());
            Value value = input.getSchema().getValue();
            if (value.isReference()) {
                require(value.getContent() instanceof Map<?, ?>, "Reference must be an object: " + input.getName());
                Map<?, ?> ref = (Map<?, ?>) value.getContent();
                require(ref.get("nodeId") instanceof String && nodes.containsKey(ref.get("nodeId"))
                        && ref.get("name") instanceof String && present((String) ref.get("name")), "Unknown reference node/path: " + input.getName());
            }
        }
    }

    // 从 start 做 BFS，返回可达节点集合
    private Set<String> reachable(String start, Map<String, List<WorkflowGraph.Transition>> outgoing) {
        Set<String> visited = new HashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        pending.add(start);
        while (!pending.isEmpty()) {
            String node = pending.removeFirst();
            if (visited.add(node)) {
                outgoing.get(node).forEach(edge -> pending.add(edge.target()));
            }
        }
        return visited;
    }

    private boolean present(String value) {
        return value != null && !value.isBlank();
    }

    // 图本身非法：缺节点、脏数据、分支缺失等
    private void require(boolean valid, String message) {
        if (!valid) {
            throw new NodeCustomException(ErrorCode.INVALID_WORKFLOW_GRAPH, message);
        }
    }

    // 边的唯一键：同源、同目标、同 handle 视为重复边
    private record EdgeKey(String source, String target, String handle) {
    }
}
