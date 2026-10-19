package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LegacyExecutionContractTest {
    @Test
    void maxRetriesMeansAdditionalAttemptsAfterTheFirstCall() {
        AtomicInteger calls = new AtomicInteger();
        AbstractNodeExecutor executor = failingExecutor(calls);
        Node node = node("mock::test");
        RetryConfig policy = new RetryConfig();
        policy.setShouldRetry(true);
        policy.setMaxRetries(2);
        node.getData().setRetryConfig(policy);

        NodeRunResult result = executor.execute(new NodeState(node, null, null));

        assertEquals(3, calls.get());
        assertEquals(3, node.getExecutedCount().get());
        assertEquals(NodeExecStatusEnum.ERR_INTERUPT, result.getStatus());
    }

    @Test
    void oldEngineRejectsCyclesBeforeStartingCallbacks() {
        NodeExecutor executor = mock(NodeExecutor.class);
        when(executor.getNodeType()).thenReturn(NodeTypeEnum.MOCK);
        WorkflowEngine engine = new WorkflowEngine(List.of(executor), mock(WorkflowStreamVirtualThreadExecutor.class));
        WorkflowDSL dsl = new WorkflowDSL("legacy", "run", List.of(node("mock::a"), node("mock::b")),
                List.of(new Edge("mock::a", "mock::b", null), new Edge("mock::b", "mock::a", null)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> engine.execute(dsl, new VariablePool(), Map.of(), null));

        assertEquals("Invalid workflow DSL: cycle detected", error.getMessage());
    }

    private Node node(String id) {
        Node node = new Node();
        node.setId(id);
        node.setData(new NodeData());
        node.init();
        return node;
    }

    private AbstractNodeExecutor failingExecutor(AtomicInteger calls) {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult doExecuteWithTimeout(NodeState state, RetryConfig config) {
                calls.incrementAndGet();
                NodeRunResult result = new NodeRunResult();
                result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
                return result;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
