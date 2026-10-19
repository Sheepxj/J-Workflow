package com.iflytek.astron.workflow.engine.v2.runtime;

public record ControlToken(String tokenId, String edgeId, String targetNodeId, String targetPort,
                           String scopeId, String causeExecutionId) {
}
