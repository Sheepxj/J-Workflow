package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.MsgTypeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmReqBo;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmResVo;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeMatch;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeSearchClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.node.impl.EndNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.StartNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.knowledge.KnowledgeBaseNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.llm.LLMNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.plugin.PluginNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LegacyNodeBaselineTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private WorkflowMsgCallback callback;
    private final String chatId = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = new WorkflowMsgCallback("p0", (type, data) -> { }, EndNodeOutputModeEnum.VARIABLE_MODE,
                new ArrayBlockingQueue<LLMGenerate>(64), new ConcurrentLinkedQueue<>(), streams);
        EngineContextHolder.initContext("p0", chatId, callback);
    }

    @AfterEach
    void tearDown() {
        callback.finished();
        streams.destroy();
        LlmChatHistory.clearHistory(chatId, "spark-llm::generate");
        EngineContextHolder.remove();
    }

    @Test
    void startCallbackClearsTheReturnedOutputsAfterTheyHaveBeenStored() throws Exception {
        Node node = initialized("sequential", "node-start::s");
        VariablePool pool = new VariablePool();
        pool.set(node.getId(), "text", "seed");

        NodeRunResult result = new StartNodeExecutor().execute(new NodeState(node, pool, callback));

        assertEquals(Map.of(), result.getOutputs());
        assertEquals(Map.of("text", "seed"), result.getInputs());
        assertEquals("seed", pool.get(node.getId(), "text"));
    }

    @Test
    void endCallbackReplacesReturnedOutputsWithInputsButRetainsRenderedAnswer() throws Exception {
        Node node = initialized("sequential", "node-end::e");
        VariablePool pool = new VariablePool();
        pool.set("mock::work", "output", "seed");

        NodeRunResult result = new EndNodeExecutor().execute(new NodeState(node, pool, callback));

        assertEquals("Result: seed", pool.get(node.getId(), "content"));
        assertEquals("", pool.get(node.getId(), "reasoning_content"));
        assertEquals(Map.of("result", "seed"), result.getOutputs());
        assertEquals(Map.of(), result.getInputs());
        assertEquals("Result: seed", result.getNodeAnswerContent());
    }

    @Test
    void llmUsesOriginalTemplatesOutputNamesAndHistoryLimit() throws Exception {
        LlmChatHistory.newChat(chatId, "spark-llm::generate");
        LlmChatHistory.addMessage(chatId, "spark-llm::generate", MsgTypeEnum.USER, "earlier");
        LlmChatHistory.addMessage(chatId, "spark-llm::generate", MsgTypeEnum.ASSISTANT, "earlier-answer");
        ModelServiceClient models = model();
        Node node = initialized("llm-history", "spark-llm::generate");
        VariablePool pool = new VariablePool();
        pool.set("node-start::s", "text", "seed");

        NodeRunResult result = new LLMNodeExecutor(models).execute(new NodeState(node, pool, callback));

        ArgumentCaptor<LlmReqBo> requests = ArgumentCaptor.forClass(LlmReqBo.class);
        verify(models).chatCompletion(requests.capture(), any());
        LlmReqBo request = requests.getValue();
        assertEquals("Rewrite seed", request.getUserMsg());
        assertEquals("Review seed", request.getSystemMsg());
        assertEquals("test-model", request.getModel());
        assertEquals(node.getId(), request.getNodeId());
        assertEquals(1, request.getHistory().size());
        assertEquals("earlier", request.getHistory().getFirst().userInputs().getFirst().content());
        assertEquals(Map.of("answer", "PASS", "reason", "checked"), result.getOutputs());
        assertEquals(2, LlmChatHistory.getHistory(chatId, node.getId()).size());
        assertEquals("Rewrite seed", LlmChatHistory.getHistory(chatId, node.getId()).getLast().userInputs().getLast().content());
        assertEquals(12, result.getTokenCost().getTotalTokens());
    }

    @Test
    void disabledLlmHistoryStillCollectsTheCurrentConversationAndDefaultsToOutputField() throws Exception {
        Node node = initialized("llm-history", "spark-llm::generate");
        node.getData().getNodeParam().put("enableChatHistoryV2", Map.of("isEnabled", false, "rounds", 2));
        node.getData().setOutputs(List.of());
        LlmChatHistory.newChat(chatId, node.getId());
        ModelServiceClient models = model();
        VariablePool pool = new VariablePool();
        pool.set("node-start::s", "text", "seed");

        NodeRunResult result = new LLMNodeExecutor(models).execute(new NodeState(node, pool, callback));

        ArgumentCaptor<LlmReqBo> requests = ArgumentCaptor.forClass(LlmReqBo.class);
        verify(models).chatCompletion(requests.capture(), any());
        assertEquals(List.of(), requests.getValue().getHistory());
        assertEquals(Map.of("output", "PASS"), result.getOutputs());
        assertEquals(2, LlmChatHistory.getHistory(chatId, node.getId()).size());
    }

    @Test
    void historyLimitKeepsTheLastTwoConversationsForTheOriginalNodeIdentity() throws Exception {
        Node node = initialized("llm-history", "spark-llm::generate");
        for (int round = 1; round <= 3; round++) {
            LlmChatHistory.newChat(chatId, node.getId());
            LlmChatHistory.addMessage(chatId, node.getId(), MsgTypeEnum.USER, "round-" + round);
        }
        ModelServiceClient models = model();
        VariablePool pool = new VariablePool();
        pool.set("node-start::s", "text", "seed");

        new LLMNodeExecutor(models).execute(new NodeState(node, pool, callback));

        ArgumentCaptor<LlmReqBo> requests = ArgumentCaptor.forClass(LlmReqBo.class);
        verify(models).chatCompletion(requests.capture(), any());
        assertEquals(List.of("round-2", "round-3"), requests.getValue().getHistory().stream()
                .map(item -> item.userInputs().getFirst().content()).toList());
    }

    @Test
    void originalPluginForwardsItsNodeStateAndResolvedInputs() throws Exception {
        PluginServiceClient plugins = mock(PluginServiceClient.class);
        when(plugins.toolCall(any(), any())).thenReturn(Map.of("output", "tool-result"));
        PluginNodeExecutor executor = new PluginNodeExecutor();
        ReflectionTestUtils.setField(executor, "pluginServiceClient", plugins);
        Node node = initialized("plugin", "plugin::tool");
        VariablePool pool = new VariablePool();
        pool.set("node-start::s", "text", "seed");
        NodeState state = new NodeState(node, pool, callback);

        NodeRunResult result = executor.execute(state);

        verify(plugins).toolCall(eq(state), eq(Map.of("input", "seed")));
        assertEquals(Map.of("output", "tool-result"), result.getOutputs());
        assertEquals("tool-result", pool.get(node.getId(), "output"));
        assertSame(state.node(), node);
    }

    @Test
    void missingReferenceIsPassedToTheOriginalPluginAsAnExplicitNullInput() throws Exception {
        PluginServiceClient plugins = mock(PluginServiceClient.class);
        when(plugins.toolCall(any(), any())).thenReturn(Map.of("output", "done"));
        PluginNodeExecutor executor = new PluginNodeExecutor();
        ReflectionTestUtils.setField(executor, "pluginServiceClient", plugins);
        Node node = initialized("plugin", "plugin::tool");

        executor.execute(new NodeState(node, new VariablePool(), callback));

        ArgumentCaptor<Map<String, Object>> inputs = ArgumentCaptor.forClass(Map.class);
        verify(plugins).toolCall(any(), inputs.capture());
        assertTrue(inputs.getValue().containsKey("input"));
        assertEquals(null, inputs.getValue().get("input"));
    }

    @Test
    void originalKnowledgeNodePreservesFilteringAndItsThreeOutputFields() throws Exception {
        PineconeSearchClient search = mock(PineconeSearchClient.class);
        Map<String, Object> metadata = Map.of("repo_id", "repo-1", "file_name", "guide.md", "page", 2);
        when(search.search(eq("seed"), eq(null), eq(3), any())).thenReturn(List.of(
                new PineconeMatch("low", 0.2, "excluded", metadata),
                new PineconeMatch("match", 0.9, "accepted", metadata)));
        Node node = initialized("knowledge", "knowledge-base::search");
        VariablePool pool = new VariablePool();
        pool.set("node-start::s", "text", "seed");

        NodeRunResult result = new KnowledgeBaseNodeExecutor(search).execute(new NodeState(node, pool, callback));

        assertEquals(NodeExecStatusEnum.SUCCESS, result.getStatus());
        assertEquals(3, result.getOutputs().size());
        assertTrue(((String) result.getOutputs().get("context")).contains("accepted"));
        assertEquals(1, ((List<?>) result.getOutputs().get("sources")).size());
        assertEquals(1, ((List<?>) result.getOutputs().get("results")).size());
        verify(search).search(eq("seed"), eq(null), eq(3), eq(Map.of("$and", List.of(
                Map.of("repo_id", Map.of("$in", List.of("repo-1"))), Map.of("enabled", Map.of("$eq", true))))));
    }

    private Node initialized(String fixture, String nodeId) throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load(fixture);
        Node node = dsl.findNodeById(nodeId);
        node.init();
        return node;
    }

    private ModelServiceClient model() {
        ModelServiceClient models = mock(ModelServiceClient.class);
        Usage usage = mock(Usage.class);
        when(usage.getPromptTokens()).thenReturn(7);
        when(usage.getCompletionTokens()).thenReturn(5);
        when(usage.getTotalTokens()).thenReturn(12);
        when(models.chatCompletion(any(), any())).thenReturn(new LlmResVo(usage, "PASS", "checked"));
        return models;
    }
}
