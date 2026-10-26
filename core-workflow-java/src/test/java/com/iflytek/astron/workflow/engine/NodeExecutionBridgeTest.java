package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder.EngineContext;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.InputSchema;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.NodeExecutionBridge;
import com.iflytek.astron.workflow.engine.execution.WorkflowRunContext;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.node.impl.EndNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.StartNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.mock.MockNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class NodeExecutionBridgeTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private WorkflowMsgCallback callback;
    private final List<LLMGenerate> frames = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = new WorkflowMsgCallback("p1", (type, data) -> frames.add((LLMGenerate) data),
                EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(128), new ConcurrentLinkedQueue<>(), streams);
    }

    @AfterEach
    void tearDown() {
        callback.finished();
        streams.destroy();
        EngineContextHolder.remove();
    }

    @Test
    void originalStartMockAndEndKeepBusinessResultsAndLegacyPresentation() throws Exception {
        StartNodeExecutor start = spy(new StartNodeExecutor());
        MockNodeExecutor work = spy(new MockNodeExecutor());
        EndNodeExecutor end = spy(new EndNodeExecutor());
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(start, work, end));
        WorkflowRunContext run = run("run", "seed");

        NodeActivation started = bridge.execute(run, node("node-start::s"));
        NodeActivation worked = bridge.execute(run, node("mock::work"));
        NodeActivation ended = bridge.execute(run, node("node-end::e"));
        callback.finished();

        verify(start).execute(any());
        verify(work).execute(any());
        verify(end).execute(any());
        assertEquals(Map.of("text", "seed"), started.result().getOutputs());
        assertEquals("seed", worked.result().getOutputs().get("output"));
        assertEquals("Result: seed", ended.result().getOutputs().get("content"));
        assertEquals(Map.of("result", "seed"), ended.result().getInputs());
        LLMGenerate startEnd = frames.stream().filter(frame -> frame.getWorkflowStep().getNode().getId().equals("node-start::s")
                && "stop".equals(frame.getWorkflowStep().getNode().getFinishReason())).findFirst().orElseThrow();
        LLMGenerate endEnd = frames.stream().filter(frame -> frame.getWorkflowStep().getNode().getId().equals("node-end::e")
                && "stop".equals(frame.getWorkflowStep().getNode().getFinishReason())).findFirst().orElseThrow();
        assertEquals(Map.of(), startEnd.getWorkflowStep().getNode().getOutputs());
        assertEquals(Map.of("result", "seed"), endEnd.getWorkflowStep().getNode().getOutputs());
        assertEquals("Result: seed", endEnd.getChoices().getFirst().getDelta().getContent());
    }

    @Test
    void springWiresTheBridgeToTheExistingNodeExecutorBean() throws Exception {
        MockNodeExecutor original = spy(new MockNodeExecutor());
        Node definition = node("mock::work");
        new ApplicationContextRunner().withBean("originalMock", NodeExecutor.class, () -> original)
                .withUserConfiguration(NodeExecutionBridge.class).run(context -> {
                    assertNull(context.getStartupFailure());
                    NodeExecutionBridge bridge = context.getBean(NodeExecutionBridge.class);
                    NodeActivation result = bridge.execute(run("spring", "seed"), definition);
                    assertEquals(NodeActivation.Status.SUCCEEDED, result.status());
                    verify(original).execute(any());
                });
    }

    @Test
    void threeActivationsReuseTheSameExecutorWithIndependentRetryAllowances() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AbstractNodeExecutor executor = executor((state, inputs) -> {
            calls.incrementAndGet();
            if (state.node().getExecutedCount().get() < 3) {
                throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "retry");
            }
            return output(inputs, Map.of("output", "done"));
        });
        Node definition = node("mock::work");
        definition.getData().setRetryConfig(retry(2, 0));
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        WorkflowRunContext run = run("run", "seed");

        List<NodeActivation> records = new ArrayList<>();
        for (int activation = 0; activation < 3; activation++) {
            records.add(bridge.execute(run, definition));
        }

        assertEquals(9, calls.get());
        assertEquals(List.of(1, 2, 3), records.stream().map(NodeActivation::activationNo).toList());
        assertEquals(3, records.stream().map(NodeActivation::executionId).distinct().count());
        assertTrue(records.stream().allMatch(record -> record.attempts().size() == 3 && record.status() == NodeActivation.Status.SUCCEEDED));
        assertNull(definition.getExecutedCount());
        assertNull(definition.getStatus());
    }

    @Test
    void retriesUseFixedInputsAndPrivateVariableViews() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        List<Object> seen = new ArrayList<>();
        AbstractNodeExecutor executor = executor((state, inputs) -> {
            seen.add(inputs.get("input"));
            if (calls.incrementAndGet() == 1) {
                inputs.put("input", "changed-input");
                state.variablePool().set("node-start::s", "text", "private-write");
                throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR);
            }
            return output(inputs, Map.of("output", inputs.get("input")));
        });
        Node definition = node("mock::work");
        definition.getData().setRetryConfig(retry(1, 0));
        WorkflowRunContext run = run("run", "seed");

        NodeActivation record = new NodeExecutionBridge(List.of(executor)).execute(run, definition);

        assertEquals(List.of("seed", "seed"), seen);
        assertEquals(Map.of("input", "seed"), record.inputSnapshot());
        assertEquals("seed", run.variables().get("node-start::s").get("text"));
        assertEquals("seed", run.variables().get("mock::work").get("output"));
    }

    @Test
    void preparedInputsAndDefinitionAreDetachedAndRepeatedExecutionIsDeduplicated() throws Exception {
        MockNodeExecutor executor = spy(new MockNodeExecutor());
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        WorkflowRunContext run = run("run", "seed");
        Node definition = node("mock::work");
        NodeActivation prepared = bridge.prepare(run, "activation", definition);
        definition.getData().getInputs().clear();
        definition.getData().getNodeParam().put("failureRate", 1);

        NodeActivation completed = bridge.execute(run, prepared.executionId());
        NodeActivation repeated = bridge.execute(run, prepared.executionId());

        verify(executor).execute(any());
        assertEquals("seed", completed.result().getOutputs().get("output"));
        assertEquals(completed.executionId(), repeated.executionId());
        assertEquals(1, run.executions().size());
        assertThrows(UnsupportedOperationException.class, () -> completed.inputSnapshot().put("input", "external"));
        completed.result().setOutputs(Map.of("output", "external"));
        assertEquals("seed", run.execution(prepared.executionId()).result().getOutputs().get("output"));
    }

    @Test
    void wholeOutputReplacementDropsOldFieldsAndPreservesNull() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AbstractNodeExecutor executor = executor((state, inputs) -> {
            Map<String, Object> outputs = new LinkedHashMap<>();
            outputs.put("output", calls.incrementAndGet());
            if (calls.get() == 1) {
                outputs.put("oldOnly", true);
            } else {
                outputs.put("nullable", null);
            }
            return output(inputs, outputs);
        });
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        WorkflowRunContext run = run("run", "seed");

        bridge.execute(run, node("mock::work"));
        NodeActivation second = bridge.execute(run, node("mock::work"));

        assertFalse(run.variables().get("mock::work").containsKey("oldOnly"));
        assertTrue(run.variables().get("mock::work").containsKey("nullable"));
        assertNull(second.result().getOutputs().get("nullable"));
    }

    @Test
    void outputsRemainPrivateUntilTheInvocationReturnsAndCommits() throws Exception {
        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AbstractNodeExecutor executor = executor((state, inputs) -> {
            state.variablePool().set("mock::work", "output", "uncommitted");
            written.countDown();
            await(release);
            return output(inputs, Map.of("output", "committed"));
        });
        WorkflowRunContext run = run("run", "seed");
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        Node definition = node("mock::work");
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<NodeActivation> future = workers.submit(() -> bridge.execute(run, definition));
            try {
                assertTrue(written.await(2, TimeUnit.SECONDS));
                assertFalse(run.variables().containsKey("mock::work"));
            } finally {
                release.countDown();
            }
            assertEquals(NodeActivation.Status.SUCCEEDED, future.get(2, TimeUnit.SECONDS).status());
            assertEquals("committed", run.variables().get("mock::work").get("output"));
        }
    }

    @Test
    void callerAndReusedWorkerContextsAreRestoredOnSuccessAndFailure() throws Exception {
        List<String> identities = new ArrayList<>();
        AbstractNodeExecutor executor = executor((state, inputs) -> {
            EngineContext context = EngineContextHolder.get();
            identities.add(context.getFlowId() + ":" + context.getChatId() + ":" + state.node().getId());
            assertSame(state.callback(), context.getCallback());
            assertSame(state.execution(), context.getNodeExecution());
            if ("fail".equals(inputs.get("input"))) {
                throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR);
            }
            return output(inputs, Map.of("output", inputs.get("input")));
        });
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        EngineContext previous = EngineContextHolder.initContext("outside", "caller", callback);
        bridge.execute(run("first", "seed"), node("mock::work"));
        assertSame(previous, EngineContextHolder.get());
        try (ExecutorService worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().inheritInheritableThreadLocals(false).factory())) {
            Future<NodeActivation> failed = worker.submit(() -> bridge.execute(run("second", "fail"), node("mock::work")));
            assertEquals(NodeActivation.Status.FAILED, failed.get(2, TimeUnit.SECONDS).status());
            assertNull(worker.submit(EngineContextHolder::get).get(2, TimeUnit.SECONDS));
        }
        assertEquals(List.of("flow-first:chat-first:mock::work", "flow-second:chat-second:mock::work"), identities);
    }

    @Test
    void separateRunsWithTheSameNodeDefinitionDoNotShareVariablesOrRecords() throws Exception {
        MockNodeExecutor executor = spy(new MockNodeExecutor());
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        Node definition = node("mock::work");
        WorkflowRunContext first = run("first", "A");
        WorkflowRunContext second = run("second", "B");
        try (ExecutorService worker = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<NodeActivation> a = worker.submit(() -> bridge.execute(first, definition));
            Future<NodeActivation> b = worker.submit(() -> bridge.execute(second, definition));
            assertNotEquals(a.get(2, TimeUnit.SECONDS).executionId(), b.get(2, TimeUnit.SECONDS).executionId());
        }
        assertEquals("A", first.variables().get("mock::work").get("output"));
        assertEquals("B", second.variables().get("mock::work").get("output"));
        assertEquals(1, first.executions().size());
        assertEquals(1, second.executions().size());
        verify(executor, atLeastOnce()).execute(any());
    }

    @Test
    void originsDistinguishMissingReferencesFromExplicitNullValues() throws Exception {
        VariablePool variables = new VariablePool();
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("nullable", null);
        variables.replaceNodeOutputs("source", source);
        WorkflowRunContext run = new WorkflowRunContext("run", "flow", "chat", variables, callback);
        Node definition = node("mock::work");
        definition.getData().setInputs(List.of(
                new InputItem("literal", "literal", new InputSchema("object", new Value("literal", null))),
                new InputItem("missing", "missing", new InputSchema("object", new Value("ref", Map.of("nodeId", "source", "name", "missing")))),
                new InputItem("nullable", "nullable", new InputSchema("object", new Value("ref", Map.of("nodeId", "source", "name", "nullable"))))));

        NodeActivation prepared = new NodeExecutionBridge(List.of(new MockNodeExecutor())).prepare(run, "activation", definition);

        assertEquals(3, prepared.inputSnapshot().size());
        assertNull(prepared.inputSnapshot().get("literal"));
        assertTrue(prepared.inputOrigins().get("literal").present());
        assertEquals("LITERAL", prepared.inputOrigins().get("literal").source());
        assertFalse(prepared.inputOrigins().get("missing").present());
        assertTrue(prepared.inputOrigins().get("nullable").present());
    }

    @Test
    void originsPointToTheCommittedProducerExecution() throws Exception {
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(new StartNodeExecutor(), new MockNodeExecutor()));
        WorkflowRunContext run = run("run", "seed");
        NodeActivation start = bridge.execute(run, node("node-start::s"));

        NodeActivation work = bridge.execute(run, node("mock::work"));

        assertEquals(start.executionId(), work.inputOrigins().get("input").executionId());
        assertEquals("node-start::s", work.inputOrigins().get("input").nodeId());
        assertEquals("text", work.inputOrigins().get("input").path());
    }

    @Test
    void aPlainNodeExecutorRetainsItsInterfaceAndNullResultsBecomeRecordedFailures() throws Exception {
        NodeExecutor executor = new NodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            public NodeRunResult execute(NodeState state) {
                assertEquals("seed", state.variablePool().get("node-start::s", "text"));
                assertEquals("flow-run", EngineContextHolder.get().getFlowId());
                return null;
            }
        };
        WorkflowRunContext run = run("run", "seed");

        NodeActivation failed = new NodeExecutionBridge(List.of(executor)).execute(run, node("mock::work"));

        assertEquals(NodeActivation.Status.FAILED, failed.status());
        assertEquals(NodeActivation.AttemptStatus.FAILED, failed.attempts().getFirst().status());
        assertFalse(run.variables().containsKey("mock::work"));
        assertEquals(0, run.physicalAttempts());
    }

    private Node node(String id) throws Exception {
        return LegacyFixtureSupport.load("sequential").findNodeById(id);
    }

    private WorkflowRunContext run(String id, String text) {
        VariablePool variables = new VariablePool();
        variables.set("node-start::s", "text", text);
        return new WorkflowRunContext(id, "flow-" + id, "chat-" + id, variables, callback);
    }

    private RetryConfig retry(int retries, float timeout) {
        RetryConfig retry = new RetryConfig();
        retry.setShouldRetry(true);
        retry.setMaxRetries(retries);
        retry.setTimeout(timeout);
        retry.setErrorStrategy(0);
        retry.setRetryInterval(0F);
        return retry;
    }

    private AbstractNodeExecutor executor(BiFunction<NodeState, Map<String, Object>, NodeRunResult> call) {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return call.apply(state, inputs);
            }
        };
    }

    private NodeRunResult output(Map<String, Object> inputs, Map<String, Object> outputs) {
        NodeRunResult result = new NodeRunResult();
        result.setInputs(inputs);
        result.setOutputs(outputs);
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        return result;
    }

    private void await(CountDownLatch latch) {
        try {
            if (!latch.await(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
