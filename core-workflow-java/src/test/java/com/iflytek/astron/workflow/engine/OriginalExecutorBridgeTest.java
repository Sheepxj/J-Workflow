package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.NodeExecutionBridge;
import com.iflytek.astron.workflow.engine.execution.WorkflowRunContext;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmCallback;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmReqBo;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmResVo;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeMatch;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeSearchClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.node.impl.knowledge.KnowledgeBaseNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.llm.LLMNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.plugin.PluginNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OriginalExecutorBridgeTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private WorkflowMsgCallback callback;
    private final String chatId = UUID.randomUUID().toString();
    private final List<WorkflowMsgCallback> callbacks = new ArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = callback(new CopyOnWriteArrayList<>());
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        callbacks.forEach(WorkflowMsgCallback::finished);
        streams.destroy();
        EngineContextHolder.remove();
        LlmChatHistory.clearHistory(chatId, "spark-llm::generate");
    }

    @Test
    void originalLlmKeepsTemplatesOutputsUsageAndHistoryAcrossActivations() throws Exception {
        ModelServiceClient models = mock(ModelServiceClient.class);
        List<Integer> historySizes = new ArrayList<>();
        List<String> prompts = new ArrayList<>();
        when(models.chatCompletion(any(), any())).thenAnswer(call -> {
            LlmReqBo request = call.getArgument(0);
            assertEquals("spark-llm::generate", request.getNodeId());
            assertEquals("Review seed", request.getSystemMsg());
            assertEquals("test-model", request.getModel());
            historySizes.add(request.getHistory().size());
            prompts.add(request.getUserMsg());
            return response("PASS");
        });
        LLMNodeExecutor executor = spy(new LLMNodeExecutor(models));
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));
        WorkflowRunContext run = run("model", "seed", callback);
        Node definition = node("llm-history", "spark-llm::generate");

        for (int activation = 0; activation < 3; activation++) {
            NodeActivation result = bridge.execute(run, definition);
            assertEquals(Map.of("answer", "PASS", "reason", "checked"), result.result().getOutputs());
            assertEquals(12, result.result().getTokenCost().getTotalTokens());
        }

        verify(executor, times(3)).execute(any());
        assertEquals(List.of(0, 1, 2), historySizes);
        assertEquals(List.of("Rewrite seed", "Rewrite seed", "Rewrite seed"), prompts);
        assertEquals(3, run.histories().get(definition.getId()).size());
        assertEquals(3, LlmChatHistory.getHistory(chatId, definition.getId()).size());
        run.histories().get(definition.getId()).clear();
        assertEquals(3, run.histories().get(definition.getId()).size());
    }

    @Test
    void disabledHistoryPreservesOriginalCollectionBehaviorAndDefaultOutputName() throws Exception {
        ModelServiceClient models = mock(ModelServiceClient.class);
        when(models.chatCompletion(any(), any())).thenAnswer(call -> {
            LlmReqBo request = call.getArgument(0);
            assertEquals(List.of(), request.getHistory());
            return response("PASS");
        });
        LLMNodeExecutor executor = spy(new LLMNodeExecutor(models));
        Node node = node("llm-history", "spark-llm::generate");
        node.getData().getNodeParam().put("enableChatHistoryV2", Map.of("isEnabled", false, "rounds", 2));
        node.getData().setOutputs(List.of());
        WorkflowRunContext run = run("model", "seed", callback);
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(executor));

        bridge.execute(run, node);
        NodeActivation result = bridge.execute(run, node);

        assertEquals(Map.of("output", "PASS"), result.result().getOutputs());
        assertEquals(2, run.histories().get(node.getId()).size());
        verify(executor, times(2)).execute(any());
    }

    @Test
    void twoRunsCaptureTheirOwnHistoryEvenWhenTheBusinessChatIdMatches() throws Exception {
        ModelServiceClient models = mock(ModelServiceClient.class);
        List<List<String>> seenHistory = new ArrayList<>();
        when(models.chatCompletion(any(), any())).thenAnswer(call -> {
            LlmReqBo request = call.getArgument(0);
            seenHistory.add(request.getHistory().stream().map(item -> item.userInputs().getLast().content()).toList());
            return response(request.getUserMsg());
        });
        List<LLMGenerate> firstFrames = new CopyOnWriteArrayList<>();
        List<LLMGenerate> secondFrames = new CopyOnWriteArrayList<>();
        WorkflowMsgCallback firstCallback = callback(firstFrames);
        WorkflowMsgCallback secondCallback = callback(secondFrames);
        WorkflowRunContext first = run("first", "A", firstCallback);
        WorkflowRunContext second = run("second", "B", secondCallback);
        Node node = node("llm-history", "spark-llm::generate");
        NodeExecutionBridge bridge = new NodeExecutionBridge(List.of(new LLMNodeExecutor(models)));

        bridge.execute(first, node);
        bridge.execute(second, node);
        bridge.execute(first, node);
        firstCallback.finished();
        secondCallback.finished();

        assertEquals(List.of(List.of(), List.of(), List.of("Rewrite A")), seenHistory);
        assertEquals(2, first.histories().get(node.getId()).size());
        assertEquals(1, second.histories().get(node.getId()).size());
        assertEquals("Rewrite A", firstFrames.getLast().getWorkflowStep().getNode().getOutputs().get("answer"));
        assertEquals("Rewrite B", secondFrames.getLast().getWorkflowStep().getNode().getOutputs().get("answer"));
    }

    @Test
    void lateOriginalLlmCannotPublishItsStreamOrConversationAfterTheRetryCommits() throws Exception {
        ModelServiceClient models = mock(ModelServiceClient.class);
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch lateFinished = new CountDownLatch(1);
        List<Integer> historySizes = new CopyOnWriteArrayList<>();
        when(models.chatCompletion(any(), any())).thenAnswer(call -> {
            LlmReqBo request = call.getArgument(0);
            historySizes.add(request.getHistory().size());
            if (calls.incrementAndGet() == 1) {
                awaitIgnoringInterrupt();
                LlmCallback stream = call.getArgument(1);
                ChatResponse chunk = mock(ChatResponse.class);
                Generation generation = mock(Generation.class);
                when(chunk.getResults()).thenReturn(List.of(generation));
                when(chunk.getResult()).thenReturn(generation);
                when(generation.getOutput()).thenReturn(new AssistantMessage("late-stream"));
                stream.onResponse(chunk);
                return response("late");
            }
            return response("accepted");
        });
        LLMNodeExecutor executor = spy(new LLMNodeExecutor(models) {
            @Override
            protected void successResponse(NodeState state, NodeRunResult result) {
                super.successResponse(state, result);
                if ("late".equals(result.getRawOutput())) {
                    lateFinished.countDown();
                }
            }
        });
        Node node = node("llm-history", "spark-llm::generate");
        RetryConfig retry = new RetryConfig();
        retry.setShouldRetry(true);
        retry.setMaxRetries(1);
        retry.setTimeout(0.2F);
        retry.setErrorStrategy(0);
        retry.setRetryInterval(0F);
        node.getData().setRetryConfig(retry);
        WorkflowRunContext run = run("model", "seed", callback);

        NodeActivation record = new NodeExecutionBridge(List.of(executor)).execute(run, node);
        assertEquals(NodeActivation.Status.SUCCEEDED, record.status());
        assertEquals(1, run.physicalAttempts());
        release.countDown();
        assertTrue(lateFinished.await(2, TimeUnit.SECONDS));

        assertEquals(List.of(0, 0), historySizes);
        assertEquals("accepted", run.variables().get(node.getId()).get("answer"));
        assertEquals(1, run.histories().get(node.getId()).size());
        assertEquals("accepted", run.histories().get(node.getId()).getFirst().llmResponses().getFirst().content());
        assertEquals("accepted", LlmChatHistory.getHistory(chatId, node.getId()).getFirst().llmResponses().getFirst().content());
        verify(callback, never()).onNodeProcess(anyInt(), anyString(), anyString(), eq("late-stream"), any());
        verify(callback, times(1)).onNodeEnd(eq(node.getId()), anyString(), any());
        verify(executor, times(1)).execute(any());
        assertEquals(2, calls.get());
    }

    @Test
    void originalPluginReceivesItsOriginalConfigurationAndScopedContext() throws Exception {
        PluginServiceClient plugins = mock(PluginServiceClient.class);
        when(plugins.toolCall(any(), any())).thenAnswer(call -> {
            NodeState state = call.getArgument(0);
            assertEquals("plugin::tool", state.node().getId());
            assertEquals("p0-tool", state.node().getData().getNodeParam().get("toolId"));
            assertEquals("invoke", state.node().getData().getNodeParam().get("operationId"));
            assertEquals("flow-plugin", EngineContextHolder.get().getFlowId());
            assertEquals(chatId, EngineContextHolder.get().getChatId());
            assertSame(state.callback(), EngineContextHolder.get().getCallback());
            assertTrue(EngineContextHolder.get().getIdempotencyKey().contains(state.execution().getExecutionId()));
            return Map.of("output", "tool-output");
        });
        PluginNodeExecutor executor = spy(new PluginNodeExecutor());
        ReflectionTestUtils.setField(executor, "pluginServiceClient", plugins);
        WorkflowRunContext run = run("plugin", "seed", callback);

        NodeActivation result = new NodeExecutionBridge(List.of(executor)).execute(run, node("plugin", "plugin::tool"));

        verify(executor).execute(any());
        verify(plugins).toolCall(any(), eq(Map.of("input", "seed")));
        assertEquals(Map.of("output", "tool-output"), result.result().getOutputs());
        assertEquals(Map.of("output", "tool-output"), run.variables().get("plugin::tool"));
    }

    @Test
    void originalPluginErrorStrategyStillPublishesItsConfiguredFallback() throws Exception {
        PluginServiceClient plugins = mock(PluginServiceClient.class);
        when(plugins.toolCall(any(), any())).thenThrow(new NodeCustomException(ErrorCode.NODE_RUN_ERROR));
        PluginNodeExecutor executor = spy(new PluginNodeExecutor());
        ReflectionTestUtils.setField(executor, "pluginServiceClient", plugins);
        Node node = node("plugin", "plugin::tool");
        RetryConfig retry = new RetryConfig();
        retry.setShouldRetry(false);
        retry.setErrorStrategy(1);
        retry.setCustomOutput(Map.of("output", "fallback"));
        node.getData().setRetryConfig(retry);
        WorkflowRunContext run = run("plugin", "seed", callback);

        NodeActivation result = new NodeExecutionBridge(List.of(executor)).execute(run, node);

        assertEquals(NodeExecStatusEnum.ERR_CODE_MSG, result.result().getStatus());
        assertEquals(Map.of("output", "fallback"), result.result().getOutputs());
        assertEquals(Map.of("output", "fallback"), run.variables().get(node.getId()));
        verify(executor).execute(any());
        verify(plugins, times(1)).toolCall(any(), any());
    }

    @Test
    void originalKnowledgeExecutorPreservesItsSearchAndOutputContract() throws Exception {
        PineconeSearchClient search = mock(PineconeSearchClient.class);
        when(search.search(eq("seed"), eq(null), eq(3), any())).thenReturn(List.of(new PineconeMatch(
                "match", 0.9, "accepted", Map.of("repo_id", "repo-1", "file_name", "guide.md"))));
        KnowledgeBaseNodeExecutor executor = spy(new KnowledgeBaseNodeExecutor(search));
        WorkflowRunContext run = run("knowledge", "seed", callback);

        NodeActivation result = new NodeExecutionBridge(List.of(executor))
                .execute(run, node("knowledge", "knowledge-base::search"));

        verify(executor).execute(any());
        verify(search).search(eq("seed"), eq(null), eq(3), argThat(filter -> filter.containsKey("$and")));
        assertEquals(3, result.result().getOutputs().size());
        assertTrue(result.result().getOutputs().get("context").toString().contains("accepted"));
        assertFalse(((List<?>) result.result().getOutputs().get("sources")).isEmpty());
        assertEquals(result.result().getOutputs(), run.variables().get("knowledge-base::search"));
    }

    private WorkflowRunContext run(String id, String text, WorkflowMsgCallback callback) {
        VariablePool variables = new VariablePool();
        variables.set("node-start::s", "text", text);
        return new WorkflowRunContext(id, "flow-" + id, chatId, variables, callback);
    }

    private Node node(String fixture, String id) throws Exception {
        return LegacyFixtureSupport.load(fixture).findNodeById(id);
    }

    private WorkflowMsgCallback callback(List<LLMGenerate> frames) {
        WorkflowMsgCallback callback = spy(new WorkflowMsgCallback("p1", (type, data) -> frames.add((LLMGenerate) data),
                EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(128), new ConcurrentLinkedQueue<>(), streams));
        callbacks.add(callback);
        return callback;
    }

    private LlmResVo response(String content) {
        Usage usage = mock(Usage.class);
        when(usage.getPromptTokens()).thenReturn(7);
        when(usage.getCompletionTokens()).thenReturn(5);
        when(usage.getTotalTokens()).thenReturn(12);
        return new LlmResVo(usage, content, "checked");
    }

    private void awaitIgnoringInterrupt() {
        for (;;) {
            try {
                if (!release.await(4, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test latch timed out");
                }
                return;
            } catch (InterruptedException ignored) {
            }
        }
    }
}
