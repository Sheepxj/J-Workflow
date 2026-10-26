package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.chain.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class WorkflowGraph {
    private final String flowId;
    private final String chatId;
    private final String startNodeId;
    private final String endNodeId;
    private final Map<String, Node> nodes;
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

    public Node endNodeDefinition() {
        return NodeExecutionScope.copyNode(nodes.get(endNodeId));
    }

    Node node(String nodeId) {
        return nodes.get(nodeId);
    }

    List<Transition> outgoing(String nodeId) {
        return outgoing.get(nodeId);
    }

    record Transition(int edgeIndex, String source, String target, String handle, boolean failure) {
    }
}
