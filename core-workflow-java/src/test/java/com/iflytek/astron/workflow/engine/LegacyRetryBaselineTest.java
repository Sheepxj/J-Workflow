package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.exception.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class LegacyRetryBaselineTest {
    @ParameterizedTest
    @CsvSource({"0, 1", "1, 2", "2, 3"})
    void maxRetriesAddsAttemptsAfterTheFirst(int retries, int expected) throws Exception {
        Node node = node();
        node.getData().setRetryConfig(policy(true, retries));
        AtomicInteger calls = new AtomicInteger();

        executor(calls, Integer.MAX_VALUE).execute(state(node));

        assertEquals(expected, calls.get());
        assertEquals(expected, node.getExecutedCount().get());
    }

    @Test
    void disabledRetryDoesNotUseTheConfiguredRetryAllowance() throws Exception {
        Node node = node();
        node.getData().setRetryConfig(policy(false, 5));
        AtomicInteger calls = new AtomicInteger();

        executor(calls, Integer.MAX_VALUE).execute(state(node));

        assertEquals(1, calls.get());
    }

    @Test
    void reusingTheSameNodeCarriesItsRetryCounterIntoTheNextExecuteCall() throws Exception {
        Node node = node();
        node.getData().setRetryConfig(policy(true, 2));
        AtomicInteger calls = new AtomicInteger();
        AbstractNodeExecutor executor = executor(calls, Integer.MAX_VALUE);

        executor.execute(state(node));
        executor.execute(state(node));

        assertEquals(4, calls.get());
        assertEquals(4, node.getExecutedCount().get());
    }

    @ParameterizedTest
    @CsvSource({"0, 30", "1, 60", "2, 70", "99, 30"})
    void retriesApplyFixedLinearExponentialOrFallbackIntervals(int strategy, long expectedMillis) throws Exception {
        Node node = node();
        RetryConfig policy = policy(true, 3);
        policy.setRetryInterval(0.01F);
        policy.setRetryStrategy(strategy);
        node.getData().setRetryConfig(policy);
        AtomicInteger calls = new AtomicInteger();

        long started = System.nanoTime();
        NodeRunResult result = executor(calls, 3).execute(state(node));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertEquals(4, calls.get());
        assertEquals(NodeExecStatusEnum.SUCCESS, result.getStatus());
        assertTrue(elapsedMillis >= expectedMillis - 5, "elapsed=" + elapsedMillis);
    }

    @Test
    void noRetryConfigurationRunsOnceOnTheCallingThread() throws Exception {
        Node node = node();
        Thread caller = Thread.currentThread();
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult doExecute(NodeState state) {
                assertSame(caller, Thread.currentThread());
                return result(NodeExecStatusEnum.SUCCESS);
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                throw new UnsupportedOperationException();
            }
        };

        assertEquals(NodeExecStatusEnum.SUCCESS, executor.execute(state(node)).getStatus());
        assertEquals(1, node.getExecutedCount().get());
    }

    @Test
    void positiveTimeoutStillAppliesWhenRetryIsDisabled() throws Exception {
        Node node = node();
        RetryConfig policy = policy(false, 0);
        policy.setTimeout(0.02F);
        node.getData().setRetryConfig(policy);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AbstractNodeExecutor executor = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult doExecute(NodeState state) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return result(NodeExecStatusEnum.SUCCESS);
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                throw new UnsupportedOperationException();
            }
        };
        try {
            NodeRunResult result = executor.execute(state(node));

            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertEquals(NodeExecStatusEnum.ERR_INTERUPT, result.getStatus());
            assertEquals(ErrorCode.TIMEOUT_ERROR.getCode(), result.getError().getCode());
            assertEquals(1, node.getExecutedCount().get());
        } finally {
            release.countDown();
        }
    }

    private AbstractNodeExecutor executor(AtomicInteger calls, int failures) {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult doExecute(NodeState state) {
                return result(calls.incrementAndGet() <= failures ? NodeExecStatusEnum.ERR_INTERUPT : NodeExecStatusEnum.SUCCESS);
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private NodeRunResult result(NodeExecStatusEnum status) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(status);
        return result;
    }

    private Node node() throws Exception {
        Node node = LegacyFixtureSupport.load("sequential").findNodeById("mock::work");
        node.init();
        return node;
    }

    private NodeState state(Node node) {
        return new NodeState(node, new VariablePool(), mock(WorkflowMsgCallback.class));
    }

    private RetryConfig policy(boolean enabled, int retries) {
        RetryConfig config = new RetryConfig();
        config.setShouldRetry(enabled);
        config.setMaxRetries(retries);
        config.setTimeout(0F);
        config.setErrorStrategy(0);
        return config;
    }
}
