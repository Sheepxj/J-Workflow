package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.chain.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 编译后的只读工作流图：节点表 + 出边邻接表 + START/END 定位。
 *
 * <p>由 {@link WorkflowGraphCompiler} 产出，一次编译可被多次 run 复用；
 * 集合与本对象构造时都做了不可变处理，可安全地被多线程共享读取。
 */
public final class WorkflowGraph {
    private final String flowId;
    // 会话标识：决定 LLM 历史按哪个会话累积（取自 DSL 的 uuid）
    private final String chatId;
    private final String startNodeId;
    private final String endNodeId;
    private final Map<String, Node> nodes;
    // 节点 ID -> 出边列表，顺序与 DSL 中的声明顺序一致
    private final Map<String, List<Transition>> outgoing;

    WorkflowGraph(String flowId, String chatId, String startNodeId, String endNodeId,
                  Map<String, Node> nodes, Map<String, List<Transition>> outgoing) {
        this.flowId = flowId;
        this.chatId = chatId;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.nodes = Map.copyOf(nodes);
        this.outgoing = Map.copyOf(outgoing);
    }

    public String flowId() {
        return flowId;
    }

    public String chatId() {
        return chatId;
    }

    public String startNodeId() {
        return startNodeId;
    }

    public String endNodeId() {
        return endNodeId;
    }

    public Set<String> nodeIds() {
        return nodes.keySet();
    }

    // 返回 END 节点的副本，避免调用方改到图内的只读定义
    public Node endNodeDefinition() {
        return NodeExecutionScope.copyNode(nodes.get(endNodeId));
    }

    Node node(String nodeId) {
        return nodes.get(nodeId);
    }

    // 出边列表已由编译器冻结，调用方只能读
    List<Transition> outgoing(String nodeId) {
        return outgoing.get(nodeId);
    }

    /**
     * 一条边（转移）的静态定义。
     *
     * @param edgeIndex 该边在 DSL edges 数组中的下标，运行时写进 Token 用于区分分支
     * @param handle    源节点的出口标识；null 表示默认出口
     * @param failure   是否失败分支（节点失败时优先走这条边）
     */
    record Transition(int edgeIndex, String source, String target, String handle, boolean failure) {
    }
}
