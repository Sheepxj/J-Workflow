package com.iflytek.astron.workflow.engine.node.impl.mock;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MockNodeExecutorTest {
    private final MockNodeExecutor executor = new MockNodeExecutor();

    @Test
    void returnsConfiguredOutputAfterDelay() {
        Node node = node(Map.of("delayMs", 20, "payloadBytes", 8));
        long start = System.nanoTime();

        NodeRunResult result = executor.executeNode(new NodeState(node, null, null), Map.of("input", "value"));

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs >= 15);
        assertEquals(NodeExecStatusEnum.SUCCESS, result.getStatus());
        assertEquals("value", result.getOutputs().get("output"));
        assertEquals(20L, result.getOutputs().get("actualDelayMs"));
        assertEquals("xxxxxxxx", result.getOutputs().get("payload"));
    }

    @Test
    void alwaysFailsWhenFailureRateIsOne() {
        Node node = node(Map.of("delayMs", 0, "failureRate", 1));

        assertThrows(NodeCustomException.class,
                () -> executor.executeNode(new NodeState(node, null, null), Map.of()));
    }

    @Test
    void rejectsInvalidParameters() {
        Node node = node(Map.of("delayMs", -1));

        assertThrows(NodeCustomException.class,
                () -> executor.executeNode(new NodeState(node, null, null), Map.of()));
    }

    @Test
    void exposesMockNodeType() {
        assertEquals(NodeTypeEnum.MOCK, executor.getNodeType());
    }

    private Node node(Map<String, Object> nodeParam) {
        NodeData data = new NodeData();
        data.setNodeParam(nodeParam);
        Node node = new Node();
        node.setId("mock::test");
        node.setData(data);
        return node;
    }
}
