package com.iflytek.astron.workflow.engine.node.impl.condition;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class IfElseNodeExecutor extends AbstractNodeExecutor {
    @Override
    public NodeTypeEnum getNodeType() {
        return NodeTypeEnum.IF_ELSE;
    }

    @Override
    public void validateConfiguration(Node node) {
        definition(node);
    }

    @Override
    public Map<String, Object> prepareInputs(NodeState state) {
        Definition definition = definition(state.node());
        Map<String, Object> resolved = new LinkedHashMap<>();
        for (InputItem input : definition.requiredInputs()) {
            Value value = input.getSchema().getValue();
            if (!value.isReference()) {
                resolved.put(input.getName(), value.getContent());
            } else {
                Map<?, ?> ref = (Map<?, ?>) value.getContent();
                String nodeId = (String) ref.get("nodeId");
                String path = (String) ref.get("name");
                resolved.put(input.getName(), state.variablePool().contains(nodeId, path)
                        ? state.variablePool().get(nodeId, path) : null);
            }
        }
        return resolved;
    }

    @Override
    protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
        Definition definition = definition(state.node());
        String selected = definition.cases().getLast().id();
        for (Branch branch : definition.cases()) {
            if (branch.conditions().isEmpty() || matches(branch, definition.inputs(), inputs, state)) {
                selected = branch.id();
                break;
            }
        }
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        result.setInputs(inputs);
        result.setOutputs(Map.of());
        result.setSelectedHandle(selected);
        return result;
    }

    public static List<String> handles(Node node) {
        return definition(node).cases().stream().map(Branch::id).toList();
    }

    public static List<InputItem> requiredInputs(Node node) {
        return definition(node).requiredInputs();
    }

    private static Definition definition(Node node) {
        require(node != null && node.getData() != null && node.getData().getNodeParam() != null, "IF_ELSE configuration required");
        Object configured = node.getData().getNodeParam().get("cases");
        require(configured instanceof List<?> && !((List<?>) configured).isEmpty(), "IF_ELSE cases and default required");
        Map<String, InputItem> inputs = new LinkedHashMap<>();
        Set<String> names = new LinkedHashSet<>();
        if (node.getData().getInputs() != null) {
            for (InputItem input : node.getData().getInputs()) {
                require(input != null && present(input.getId()) && present(input.getName()), "IF_ELSE input ID/name required");
                require(inputs.putIfAbsent(input.getId(), input) == null && names.add(input.getName()), "Duplicate IF_ELSE input ID/name");
            }
        }
        List<?> raw = (List<?>) configured;
        List<Branch> branches = new ArrayList<>();
        Set<String> ids = new LinkedHashSet<>();
        Set<String> required = new LinkedHashSet<>();
        for (int index = 0; index < raw.size(); index++) {
            require(raw.get(index) instanceof Map<?, ?>, "IF_ELSE case must be an object");
            Map<?, ?> item = (Map<?, ?>) raw.get(index);
            String id = text(item.get("id"), "IF_ELSE case ID required");
            require(ids.add(id), "Duplicate IF_ELSE case ID: " + id);
            Object conditions = item.get("conditions");
            if (index == raw.size() - 1) {
                require(conditions == null || conditions instanceof List<?> && ((List<?>) conditions).isEmpty(), "Last IF_ELSE case must be the default");
                branches.add(new Branch(id, "and", List.of()));
                continue;
            }
            require(conditions instanceof List<?> && !((List<?>) conditions).isEmpty(), "Non-default IF_ELSE case needs conditions");
            String logical = text(item.get("logicalOperator"), "IF_ELSE logicalOperator required");
            require(Set.of("and", "or").contains(logical), "Unknown IF_ELSE logicalOperator: " + logical);
            List<Predicate> predicates = new ArrayList<>();
            for (Object entry : (List<?>) conditions) {
                require(entry instanceof Map<?, ?>, "IF_ELSE condition must be an object");
                Map<?, ?> condition = (Map<?, ?>) entry;
                String operator = text(condition.get("compareOperator"), "IF_ELSE compareOperator required");
                require(ConditionValues.OPERATORS.contains(operator), "Unknown IF_ELSE compareOperator: " + operator);
                String left = text(condition.get("leftVarIndex"), "IF_ELSE leftVarIndex required");
                validateInput(inputs.get(left), left);
                required.add(left);
                String right = null;
                if (!ConditionValues.UNARY.contains(operator)) {
                    right = text(condition.get("rightVarIndex"), "IF_ELSE rightVarIndex required");
                    validateInput(inputs.get(right), right);
                    required.add(right);
                }
                predicates.add(new Predicate(left, right, operator));
            }
            branches.add(new Branch(id, logical, List.copyOf(predicates)));
        }
        return new Definition(List.copyOf(branches), Map.copyOf(inputs), required.stream().map(inputs::get).toList());
    }

    private static void validateInput(InputItem input, String id) {
        require(input != null && input.getSchema() != null && input.getSchema().getValue() != null, "Unknown or invalid IF_ELSE input: " + id);
        Value value = input.getSchema().getValue();
        require(value.getType() != null && Set.of("literal", "ref").contains(value.getType()), "Unknown IF_ELSE input value type");
        if (value.isReference()) {
            require(value.getContent() instanceof Map<?, ?>, "IF_ELSE reference must be an object");
            Map<?, ?> ref = (Map<?, ?>) value.getContent();
            text(ref.get("nodeId"), "IF_ELSE reference nodeId required");
            text(ref.get("name"), "IF_ELSE reference name required");
        }
    }

    private boolean matches(Branch branch, Map<String, InputItem> bindings, Map<String, Object> inputs, NodeState state) {
        boolean and = "and".equals(branch.logical());
        for (Predicate predicate : branch.conditions()) {
            Object left = operand(bindings.get(predicate.left()), inputs, state);
            Object right = predicate.right() == null ? null : operand(bindings.get(predicate.right()), inputs, state);
            boolean matched = ConditionValues.compare(predicate.operator(), left, right);
            if (and && !matched) {
                return false;
            }
            if (!and && matched) {
                return true;
            }
        }
        return and;
    }

    private Object operand(InputItem input, Map<String, Object> inputs, NodeState state) {
        boolean available = inputs.containsKey(input.getName());
        if (state.execution() != null) {
            available &= state.execution().inputPresent(input.getName());
        } else if (input.getSchema().getValue().isReference()) {
            Map<?, ?> ref = (Map<?, ?>) input.getSchema().getValue().getContent();
            available &= state.variablePool().contains((String) ref.get("nodeId"), (String) ref.get("name"));
        }
        if (!available) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_INPUT_UNAVAILABLE, "IF_ELSE input unavailable: " + input.getName());
        }
        return inputs.get(input.getName());
    }

    private static String text(Object value, String message) {
        require(value instanceof String && present((String) value), message);
        return (String) value;
    }

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, message);
        }
    }

    private record Definition(List<Branch> cases, Map<String, InputItem> inputs, List<InputItem> requiredInputs) {
    }

    private record Branch(String id, String logical, List<Predicate> conditions) {
    }

    private record Predicate(String left, String right, String operator) {
    }
}
