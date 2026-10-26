package com.iflytek.astron.workflow.engine.node.impl.condition;

import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ConditionValues {
    public static final Set<String> UNARY = Set.of("empty", "not_empty", "null", "not_null");
    public static final Set<String> OPERATORS = Set.of("contains", "not_contains", "empty", "not_empty", "is", "is_not",
            "start_with", "end_with", "eq", "ne", "gt", "ge", "lt", "le", "null", "not_null");

    private ConditionValues() {
    }

    public static boolean compare(String operator, Object left, Object right) {
        if (!OPERATORS.contains(operator == null ? "" : operator)) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Unknown condition operator: " + operator);
        }
        return switch (operator) {
            case "null" -> left == null;
            case "not_null" -> left != null;
            case "empty" -> empty(left);
            case "not_empty" -> !empty(left);
            case "contains" -> contains(left, right);
            case "not_contains" -> !contains(left, right);
            case "start_with" -> string(left).startsWith(string(right));
            case "end_with" -> string(left).endsWith(string(right));
            case "is", "is_not" -> {
                require(left instanceof Boolean && right instanceof Boolean, "is/is_not requires Boolean operands");
                yield "is".equals(operator) == left.equals(right);
            }
            case "eq", "ne" -> {
                require(family(left) == family(right), "eq/ne requires compatible operand types");
                yield "eq".equals(operator) == equivalent(left, right);
            }
            case "gt" -> number(left).compareTo(number(right)) > 0;
            case "ge" -> number(left).compareTo(number(right)) >= 0;
            case "lt" -> number(left).compareTo(number(right)) < 0;
            case "le" -> number(left).compareTo(number(right)) <= 0;
            default -> throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Unknown condition operator: " + operator);
        };
    }

    private static boolean contains(Object left, Object right) {
        if (left instanceof String text) {
            return text.contains(string(right));
        }
        if (left instanceof List<?> items) {
            family(right);
            return items.stream().anyMatch(item -> equivalent(item, right));
        }
        throw new NodeCustomException(ErrorCode.CONDITION_TYPE_ERROR, "contains requires String or List on the left");
    }

    private static boolean empty(Object value) {
        if (value instanceof String text) {
            return text.isEmpty();
        }
        if (value instanceof List<?> list) {
            return list.isEmpty();
        }
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        throw new NodeCustomException(ErrorCode.CONDITION_TYPE_ERROR, "empty requires String, List or Map");
    }

    private static boolean equivalent(Object left, Object right) {
        if (family(left) != family(right)) {
            return false;
        }
        if (left instanceof Number) {
            return number(left).compareTo(number(right)) == 0;
        }
        if (left instanceof List<?> first && right instanceof List<?> second) {
            if (first.size() != second.size()) {
                return false;
            }
            for (int index = 0; index < first.size(); index++) {
                if (!equivalent(first.get(index), second.get(index))) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof Map<?, ?> first && right instanceof Map<?, ?> second) {
            return first.keySet().equals(second.keySet())
                    && first.keySet().stream().allMatch(key -> equivalent(first.get(key), second.get(key)));
        }
        return Objects.equals(left, right);
    }

    private static int family(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            number(value);
            return 1;
        }
        if (value instanceof String) {
            return 2;
        }
        if (value instanceof Boolean) {
            return 3;
        }
        if (value instanceof List<?>) {
            return 4;
        }
        if (value instanceof Map<?, ?>) {
            return 5;
        }
        throw new NodeCustomException(ErrorCode.CONDITION_TYPE_ERROR, "Unsupported condition value type: " + value.getClass().getSimpleName());
    }

    private static BigDecimal number(Object value) {
        require(value instanceof Number, "Numeric comparison requires Number operands");
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException invalid) {
            throw new NodeCustomException(ErrorCode.CONDITION_TYPE_ERROR, "Numeric operands must be finite");
        }
    }

    private static String string(Object value) {
        require(value instanceof String, "String comparison requires String operands");
        return (String) value;
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new NodeCustomException(ErrorCode.CONDITION_TYPE_ERROR, message);
        }
    }
}
