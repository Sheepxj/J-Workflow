package com.iflytek.astron.workflow.engine.v2.compiler;

import com.iflytek.astron.workflow.engine.v2.definition.EdgeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.LoopDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;

import java.util.List;
import java.util.Map;

public record CompiledWorkflow(WorkflowDefinition definition, String contentHash, String startNodeId,
                               Map<String, NodeDefinition> nodes, Map<String, List<EdgeDefinition>> outgoing,
                               Map<String, LoopDefinition> loopsByHeader, Map<String, String> nodeLoopIds) {
    public CompiledWorkflow {
        nodes = Map.copyOf(nodes);
        outgoing = Map.copyOf(outgoing);
        loopsByHeader = Map.copyOf(loopsByHeader);
        nodeLoopIds = Map.copyOf(nodeLoopIds);
    }
}
