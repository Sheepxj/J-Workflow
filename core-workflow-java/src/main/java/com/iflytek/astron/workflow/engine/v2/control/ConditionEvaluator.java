package com.iflytek.astron.workflow.engine.v2.control;

import com.iflytek.astron.workflow.engine.v2.binding.InputResolver;
import com.iflytek.astron.workflow.engine.v2.definition.Condition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

public final class ConditionEvaluator {
    private final InputResolver resolver = new InputResolver();

    public boolean evaluate(Condition condition, Map<String, Object> values) {
        return switch (condition.operator()) {
            case AND -> condition.operands().stream().allMatch(item -> evaluate(item, values));
            case OR -> condition.operands().stream().anyMatch(item -> evaluate(item, values));
            case NOT -> !evaluate(condition.operands().getFirst(), values);
            default -> compare(condition, values);
        };
    }

    private boolean compare(Condition condition, Map<String, Object> values) {
        Object left = resolver.path(values, condition.statePath() == null ? condition.output() : condition.statePath());
        Object right = condition.value();
        boolean numeric = left instanceof Number && right instanceof Number;
        if (left != null && right != null && !numeric && !sameType(left, right)) {
            throw new WorkflowExecutionException(WorkflowError.CONDITION_ERROR, "Condition operand types differ");
        }
        return switch (condition.operator()) {
            case EQUALS -> numeric ? number(left).compareTo(number(right)) == 0 : Objects.equals(left, right);
            case NOT_EQUALS -> numeric ? number(left).compareTo(number(right)) != 0 : !Objects.equals(left, right);
            case GREATER_THAN -> ordered(left, right) > 0;
            case LESS_THAN -> ordered(left, right) < 0;
            default -> throw new WorkflowExecutionException(WorkflowError.CONDITION_ERROR, "Unsupported comparison");
        };
    }

    private int ordered(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            return number(left).compareTo(number(right));
        }
        if (left instanceof String text && right instanceof String other) {
            return text.compareTo(other);
        }
        throw new WorkflowExecutionException(WorkflowError.CONDITION_ERROR, "Ordered comparison requires numbers or strings");
    }

    private BigDecimal number(Object value) {
        return new BigDecimal(value.toString());
    }

    private boolean sameType(Object left, Object right) {
        return left instanceof String && right instanceof String
                || left instanceof Boolean && right instanceof Boolean
                || left instanceof Map && right instanceof Map
                || left instanceof Iterable && right instanceof Iterable;
    }
}
