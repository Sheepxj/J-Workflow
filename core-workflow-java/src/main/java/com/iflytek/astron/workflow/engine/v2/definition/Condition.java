package com.iflytek.astron.workflow.engine.v2.definition;

import java.util.List;

public record Condition(Operator operator, String statePath, String output, Object value, List<Condition> operands) {
    public Condition {
        value = JsonValues.freeze(value);
        operands = operands == null ? List.of() : List.copyOf(operands);
    }

    public enum Operator {
        EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, AND, OR, NOT
    }
}
