package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.InputSchema;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.NodeMeta;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class LegacyWorkflowCompatibilityTest {
    @Test
    void oldSequentialDslPreservesReferenceResolutionAndSingleExecution() throws Exception {
        Node start = node("node-start::s", null);
        Node work = node("mock::work", reference(start.getId()));
        Node end = node("node-end::e", reference(work.getId()));
        List<String> calls = new ArrayList<>();
        VariablePool pool = execute(List.of(start, work, end), List.of(edge(start, work), edge(work, end)), calls);

        assertEquals(7, pool.get(end.getId(), "value"));
        assertEquals(List.of(work.getId()), calls);
        assertEquals(NodeStatusEnum.SUCCESS, end.getStatus());
    }

    @Test
    void oldFailureBranchSkipsItsNormalPathAndExecutesTheHandler() throws Exception {
        Node start = node("node-start::s", null);
        Node work = node("mock::work", reference(start.getId()));
        work.getData().getNodeParam().put("fail", true);
        work.getData().setRetryConfig(failurePolicy(2, Map.of()));
        Node normal = node("mock::normal", null);
        Node handler = node("mock::handler", null);
        handler.getData().getNodeParam().put("value", "recovered");
        Node end = node("node-end::e", reference(handler.getId()));
        List<String> calls = new ArrayList<>();
        VariablePool pool = execute(List.of(start, work, normal, handler, end), List.of(edge(start, work),
                new Edge(work.getId(), normal.getId(), "normal_one_of"),
                new Edge(work.getId(), handler.getId(), "intent_chain|fail_one_of"), edge(normal, end), edge(handler, end)), calls);

        assertEquals("recovered", pool.get(end.getId(), "value"));
        assertEquals(List.of(work.getId(), handler.getId()), calls);
        assertEquals(NodeStatusEnum.SKIP, normal.getStatus());
        assertEquals(NodeStatusEnum.ERROR, work.getStatus());
    }

    @Test
    void oldCustomErrorOutputStillFollowsItsNormalEdge() throws Exception {
        Node start = node("node-start::s", null);
        Node work = node("mock::work", reference(start.getId()));
        work.getData().getNodeParam().put("fail", true);
        work.getData().setRetryConfig(failurePolicy(1, Map.of("value", "fallback")));
        Node end = node("node-end::e", reference(work.getId()));
        VariablePool pool = execute(List.of(start, work, end), List.of(edge(start, work), edge(work, end)), new ArrayList<>());

        assertEquals("fallback", pool.get(end.getId(), "value"));
        assertEquals(NodeStatusEnum.ERROR, work.getStatus());
        assertEquals(NodeStatusEnum.SUCCESS, end.getStatus());
    }

    private VariablePool execute(List<Node> nodes, List<Edge> edges, List<String> calls) throws Exception {
        WorkflowStreamVirtualThreadExecutor streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        try {
            List<NodeExecutor> executors = List.of(executor(NodeTypeEnum.START, calls), executor(NodeTypeEnum.MOCK, calls),
                    executor(NodeTypeEnum.END, calls));
            WorkflowEngine engine = new WorkflowEngine(executors, streams);
            VariablePool pool = new VariablePool();
            engine.execute(new WorkflowDSL("legacy-contract", "chat", nodes, edges), pool, Map.of("value", 7), (type, data) -> { });
            return pool;
        } finally {
            streams.destroy();
        }
    }

    private NodeExecutor executor(NodeTypeEnum type, List<String> calls) {
        return new AbstractNodeExecutor() {
            public NodeTypeEnum getNodeType() { return type; }
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                if (type == NodeTypeEnum.MOCK) {
                    calls.add(state.node().getId());
                    if (Boolean.TRUE.equals(state.node().getData().getNodeParam().get("fail"))) {
                        throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR);
                    }
                }
                NodeRunResult result = new NodeRunResult();
                result.setStatus(NodeExecStatusEnum.SUCCESS);
                result.setInputs(inputs);
                Object value = state.node().getData().getNodeParam().get("value");
                result.setOutputs(value == null ? inputs : Map.of("value", value));
                return result;
            }
        };
    }

    private Node node(String id, InputItem input) {
        Node node = new Node();
        node.setId(id);
        NodeData data = new NodeData();
        data.setNodeMeta(new NodeMeta(id.split("::")[0], id));
        data.setNodeParam(new HashMap<>(Map.of("outputMode", 1)));
        data.setInputs(input == null ? List.of() : List.of(input));
        node.setData(data);
        return node;
    }

    private InputItem reference(String nodeId) {
        return new InputItem("value", "value", new InputSchema("object", new Value("ref", Map.of("nodeId", nodeId, "name", "value"))));
    }

    private RetryConfig failurePolicy(int strategy, Map<String, Object> outputs) {
        RetryConfig config = new RetryConfig();
        config.setShouldRetry(false);
        config.setMaxRetries(0);
        config.setErrorStrategy(strategy);
        config.setCustomOutput(outputs);
        return config;
    }

    private Edge edge(Node from, Node to) {
        return new Edge(from.getId(), to.getId(), null);
    }
}
