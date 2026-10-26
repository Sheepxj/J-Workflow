package com.iflytek.astron.workflow.engine.node;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.Value;

import java.util.HashMap;
import java.util.Map;

public final class NodeInputResolver {
    private NodeInputResolver() {
    }

    public static Map<String, Object> resolve(Node node, VariablePool pool) {
        if (node.getNodeType() == NodeTypeEnum.START) {
            return pool.get(node.getId());
        }
        Map<String, Object> inputs = new HashMap<>();
        if (node.getData().getInputs() == null) {
            return inputs;
        }
        for (InputItem input : node.getData().getInputs()) {
            if (input.getSchema() == null || input.getSchema().getValue() == null) {
                continue;
            }
            Value value = input.getSchema().getValue();
            if (!value.isReference()) {
                inputs.put(input.getName(), value.getContent());
            } else if (value.getContent() instanceof Map<?, ?> ref) {
                Object nodeId = ref.get("nodeId");
                Object name = ref.get("name");
                if (nodeId != null && name != null) {
                    inputs.put(input.getName(), pool.get((String) nodeId, (String) name));
                }
            }
        }
        return inputs;
    }
}
