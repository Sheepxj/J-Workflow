package com.iflytek.astron.workflow.engine.v2.executor;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder.EngineContext;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;

import java.util.LinkedHashMap;
import java.util.Map;

public final class PluginTaskExecutor implements NodeTaskExecutor {
    private final PluginServiceClient client;

    public PluginTaskExecutor(PluginServiceClient client) {
        this.client = client;
    }

    @Override
    public String nodeType() {
        return "PLUGIN";
    }

    @Override
    public Map<String, Object> execute(NodeInvocation invocation) throws Exception {
        Node node = new Node();
        node.setId("plugin::" + invocation.executionId());
        NodeData data = new NodeData();
        data.setNodeParam(new LinkedHashMap<>(invocation.node().config()));
        node.setData(data);
        node.init();
        EngineContext previous = EngineContextHolder.get();
        try {
            EngineContextHolder.initContext(invocation.runId(), invocation.executionId(), null);
            return client.toolCall(new NodeState(node, new VariablePool(), null), invocation.inputs());
        } finally {
            if (previous == null) {
                EngineContextHolder.remove();
            } else {
                EngineContextHolder.set(previous);
            }
        }
    }
}
