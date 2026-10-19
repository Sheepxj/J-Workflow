package com.iflytek.astron.workflow.engine.v2.definition;

import java.util.Map;

public record NodeDefinition(String id, String type, Map<String, Object> config,
                             Map<String, InputBinding> inputBindings, Routing routing, RetryPolicy retry,
                             boolean breakpoint) {
    public NodeDefinition {
        config = JsonValues.snapshot(config);
        inputBindings = inputBindings == null ? Map.of() : Map.copyOf(inputBindings);
        routing = routing == null ? Routing.SEQUENCE : routing;
        retry = retry == null ? RetryPolicy.defaults() : retry;
    }

    public enum Routing {
        SEQUENCE, EXCLUSIVE
    }
}
