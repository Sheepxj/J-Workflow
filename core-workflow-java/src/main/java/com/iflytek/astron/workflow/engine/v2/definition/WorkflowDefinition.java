package com.iflytek.astron.workflow.engine.v2.definition;

import java.util.List;

public record WorkflowDefinition(String dslVersion, String definitionId, int version, ExecutionLimits limits,
                                 List<NodeDefinition> nodes, List<EdgeDefinition> edges, List<LoopDefinition> loops,
                                 List<ParallelDefinition> parallels) {
    public WorkflowDefinition {
        limits = limits == null ? ExecutionLimits.defaults() : limits;
        nodes = nodes == null ? List.of() : List.copyOf(nodes);
        edges = edges == null ? List.of() : List.copyOf(edges);
        loops = loops == null ? List.of() : List.copyOf(loops);
        parallels = parallels == null ? List.of() : List.copyOf(parallels);
    }
}
