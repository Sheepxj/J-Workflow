package com.iflytek.astron.workflow.engine.v2.runtime;

public record InputOrigin(String source, String executionId, String scopeId, String path, int version) {
}
