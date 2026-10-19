package com.iflytek.astron.workflow.engine.v2.definition;

import java.util.List;
import java.util.Map;

public record LoopDefinition(String loopId, String headerNodeId, List<String> bodyNodeIds, int maxIterations,
                             Map<String, InputBinding> initialState, Condition continueWhen,
                             Map<String, InputBinding> nextState, Map<String, InputBinding> outputBindings) {
    public LoopDefinition {
        bodyNodeIds = bodyNodeIds == null ? List.of() : List.copyOf(bodyNodeIds);
        initialState = initialState == null ? Map.of() : Map.copyOf(initialState);
        nextState = nextState == null ? Map.of() : Map.copyOf(nextState);
        outputBindings = outputBindings == null ? Map.of() : Map.copyOf(outputBindings);
    }
}
