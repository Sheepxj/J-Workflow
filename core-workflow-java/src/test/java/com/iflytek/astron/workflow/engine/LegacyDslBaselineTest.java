package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.constants.NodeStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeSearchClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.node.StreamCallback;
import com.iflytek.astron.workflow.engine.node.impl.EndNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.StartNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.mock.MockNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class LegacyDslBaselineTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private List<NodeExecutor> executors;

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        executors = List.of(new StartNodeExecutor(), new MockNodeExecutor(), new EndNodeExecutor());
    }

    @AfterEach
    void tearDown() {
        streams.destroy();
    }

    @Test
    void componentScanRetainsOriginalExecutorsAndRegistersIfElse() {
        new ApplicationContextRunner().withUserConfiguration(OriginalExecutors.class)
                .withInitializer(context -> {
                    context.getBeanFactory().registerSingleton("p0Models", mock(ModelServiceClient.class));
                    context.getBeanFactory().registerSingleton("p0Plugins", mock(PluginServiceClient.class));
                    context.getBeanFactory().registerSingleton("p0Knowledge", mock(PineconeSearchClient.class));
                })
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    Set<NodeTypeEnum> actual = context.getBeansOfType(NodeExecutor.class).values().stream()
                            .map(NodeExecutor::getNodeType).collect(Collectors.toSet());
                    assertEquals(Set.of(NodeTypeEnum.START, NodeTypeEnum.END, NodeTypeEnum.MOCK,
                            NodeTypeEnum.LLM, NodeTypeEnum.PLUGIN, NodeTypeEnum.KNOWLEDGE_BASE, NodeTypeEnum.IF_ELSE), actual);
                    assertEquals(7, context.getBeansOfType(NodeExecutor.class).size());
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"sequential", "failure-branch", "custom-error-output", "interrupt", "llm-history",
            "branch-cycle", "exclusive-merge", "parallel-fork", "plugin", "knowledge"})
    void originalFixturesRoundTripThroughTheServiceAndHttpDtoFormats(String name) throws Exception {
        WorkflowDSL original = LegacyFixtureSupport.load(name);
        WorkflowDSL jackson = new ObjectMapper().readValue(LegacyFixtureSupport.json(name), WorkflowDSL.class);
        assertEquals(original.getFlowId(), jackson.getFlowId());
        assertEquals(original.getNodes().size(), jackson.getNodes().size());
        assertEquals(original.getEdges(), jackson.getEdges());
        for (int index = 0; index < original.getNodes().size(); index++) {
            Node expected = original.getNodes().get(index);
            Node actual = jackson.getNodes().get(index);
            assertEquals(expected.getId(), actual.getId());
            ObjectMapper mapper = new ObjectMapper();
            assertEquals(mapper.readTree(JSON.toJSONString(expected.getData())), mapper.readTree(JSON.toJSONString(actual.getData())));
            assertNull(expected.getStatus());
        }
    }

    @Test
    void serviceAndHttpParsingKeepJsonNumbersButUseDifferentDecimalClasses() throws Exception {
        WorkflowDSL service = LegacyFixtureSupport.load("knowledge");
        WorkflowDSL http = new ObjectMapper().readValue(LegacyFixtureSupport.json("knowledge"), WorkflowDSL.class);

        assertInstanceOf(BigDecimal.class, service.findNodeById("knowledge-base::search").getData().getNodeParam().get("score"));
        assertInstanceOf(Double.class, http.findNodeById("knowledge-base::search").getData().getNodeParam().get("score"));
    }

    @Test
    void conditionalCasesKeepTheirInputIdsOrderDefaultAndSourceHandles() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("branch-cycle");
        Node branch = dsl.findNodeById("if-else::check");
        List<?> cases = (List<?>) branch.getData().getNodeParam().get("cases");
        Map<?, ?> first = (Map<?, ?>) cases.getFirst();
        Map<?, ?> condition = (Map<?, ?>) ((List<?>) first.get("conditions")).getFirst();
        Map<?, ?> fallback = (Map<?, ?>) cases.getLast();

        assertEquals(NodeTypeEnum.IF_ELSE, branch.getNodeType());
        assertEquals("branch_one_of::pass", first.get("id"));
        assertEquals("and", first.get("logicalOperator"));
        assertEquals("input-actual", condition.get("leftVarIndex"));
        assertEquals("input-expected", condition.get("rightVarIndex"));
        assertEquals("eq", condition.get("compareOperator"));
        assertEquals(List.of(), fallback.get("conditions"));
        assertEquals(999, fallback.get("level"));
        assertEquals(List.of("actual", "expected"), branch.getData().getInputs().stream().map(item -> item.getName()).toList());
        assertEquals(List.of("branch_one_of::pass", "branch_one_of::retry"),
                dsl.getOutgoingEdges(branch.getId()).stream().map(edge -> edge.getSourceHandle()).toList());
        assertEquals("spark-llm::generate", dsl.getOutgoingEdges(branch.getId()).getLast().getTarget());
    }

    @Test
    void originalStartMockAndEndExecuteWithOrderedStreamFrames() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("sequential");
        RecordingCallback callback = new RecordingCallback();
        VariablePool variables = new VariablePool();
        variables.set("old-run", "stale", "removed");

        new WorkflowEngine(executors, streams).execute(dsl, variables, Map.of("text", "seed"), callback);

        assertEquals("seed", variables.get("mock::work", "output"));
        assertEquals("Result: seed", variables.get("node-end::e", "content"));
        assertEquals(Map.of(), variables.get("old-run"));
        assertEquals(List.of("flow_obj", "node-start::s", "node-start::s", "mock::work", "mock::work",
                "node-end::e", "node-end::e", "flow_obj"), callback.frames.stream()
                .map(frame -> frame.getWorkflowStep().getNode().getId()).toList());
        assertEquals("flow_start", callback.frames.getFirst().getWorkflowStep().getNode().getAliasName());
        assertEquals("flow_end", callback.frames.getLast().getWorkflowStep().getNode().getAliasName());
        assertTrue(callback.finished);
        assertTrue(dsl.getNodes().stream().allMatch(node -> node.getExecutedCount().get() == 1));
    }

    @Test
    void realMockFailureSelectsOnlyTheConfiguredErrorBranch() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("failure-branch");
        VariablePool variables = new VariablePool();

        new WorkflowEngine(executors, streams).execute(dsl, variables, Map.of("text", "seed"), (type, data) -> { });

        assertEquals(NodeStatusEnum.ERROR, dsl.findNodeById("mock::work").getStatus());
        assertEquals(NodeStatusEnum.SKIP, dsl.findNodeById("mock::normal").getStatus());
        assertEquals(0, dsl.findNodeById("mock::normal").getExecutedCount().get());
        assertEquals("Result: recovered", variables.get("node-end::e", "content"));
    }

    @Test
    void customErrorOutputIsPublishedAndFollowsTheNormalRoute() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("custom-error-output");
        VariablePool variables = new VariablePool();

        new WorkflowEngine(executors, streams).execute(dsl, variables, Map.of("text", "seed"), (type, data) -> { });

        assertEquals(NodeStatusEnum.ERROR, dsl.findNodeById("mock::work").getStatus());
        assertEquals("fallback", variables.get("mock::work", "output"));
        assertEquals("Result: fallback", variables.get("node-end::e", "content"));
    }

    @Test
    void interruptionFlushesItsFramesBeforeTheEngineThrows() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("interrupt");
        RecordingCallback callback = new RecordingCallback();

        assertThrows(NodeCustomException.class, () -> new WorkflowEngine(executors, streams)
                .execute(dsl, new VariablePool(), Map.of("text", "seed"), callback));

        assertTrue(callback.finished);
        assertEquals("flow_end", callback.frames.getLast().getWorkflowStep().getNode().getAliasName());
        assertTrue(callback.frames.stream().anyMatch(frame -> frame.getEventData() != null));
        assertEquals(0, dsl.findNodeById("node-end::e").getExecutedCount().get());
    }

    @Test
    void anEnumEntryDoesNotMakeAnIfElseWorkflowExecutable() throws Exception {
        RecordingCallback callback = new RecordingCallback();
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> new WorkflowEngine(executors, streams)
                .execute(LegacyFixtureSupport.load("exclusive-merge"), new VariablePool(), Map.of("text", "left"), callback));

        assertTrue(error.getMessage().contains("no executor found for node type: if-else"));
        assertTrue(callback.frames.isEmpty());
        assertFalse(callback.finished);
    }

    @Test
    void parallelDagRetainsBothBranchesAndAOnceOnlyMerge() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("parallel-fork");
        VariablePool variables = new VariablePool();
        ParallelWorkflowEngine engine = new ParallelWorkflowEngine(executors, streams);
        try {
            engine.execute(dsl, variables, Map.of("text", "seed"), (type, data) -> { });
        } finally {
            engine.destroy();
        }

        assertEquals("L", variables.get("mock::left", "output"));
        assertEquals("R", variables.get("mock::right", "output"));
        assertEquals(1, dsl.findNodeById("mock::merge").getExecutedCount().get());
        assertEquals("Result: seed", variables.get("node-end::e", "content"));
    }

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(basePackageClasses = NodeExecutor.class)
    static class OriginalExecutors {
    }

    private static final class RecordingCallback implements StreamCallback {
        private final List<LLMGenerate> frames = new CopyOnWriteArrayList<>();
        private volatile boolean finished;

        @Override
        public void callback(String eventType, Object data) {
            assertEquals("stream", eventType);
            frames.add((LLMGenerate) data);
        }

        @Override
        public void finished() {
            finished = true;
        }
    }
}
