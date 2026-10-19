package com.iflytek.astron.workflow.engine.v2.definition;

import java.util.Map;

public record ParallelDefinition(String forkNodeId, String joinNodeId, Map<String, String> branches) {
    public ParallelDefinition {
        branches = branches == null ? Map.of() : Map.copyOf(branches);
    }
}
