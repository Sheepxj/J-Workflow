package com.iflytek.astron.workflow.engine.v2.definition;

public record InputBinding(Source source, String nodeId, String loopId, String scope, String path, Object value) {
    public InputBinding {
        value = JsonValues.freeze(value);
    }

    public enum Source {
        LITERAL, RUN_INPUT, NODE_OUTPUT, NODE_ERROR, LOOP_STATE, LOOP_OUTPUT
    }
}
