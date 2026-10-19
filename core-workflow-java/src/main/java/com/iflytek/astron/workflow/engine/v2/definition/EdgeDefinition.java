package com.iflytek.astron.workflow.engine.v2.definition;

import com.fasterxml.jackson.annotation.JsonProperty;

public record EdgeDefinition(String id, String source, String target, String sourcePort, String targetPort,
                             Condition condition, @JsonProperty("default") boolean defaultEdge,
                             EdgeType edgeType, String loopId) {
    public EdgeDefinition {
        edgeType = edgeType == null ? EdgeType.NORMAL : edgeType;
    }

    public enum EdgeType {
        NORMAL, BACK
    }
}
