package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmReqBo;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmResVo;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.definition.NodeDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.LlmTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.NodeInvocation;
import com.iflytek.astron.workflow.engine.v2.executor.PluginTaskExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.ai.chat.metadata.EmptyUsage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BusinessTaskExecutorTest {
    @Test
    void llmMakesOneCallUsingFixedInputsWithoutLegacyRetryOrGlobalHistory() throws Exception {
        ModelServiceClient client = mock(ModelServiceClient.class);
        when(client.chatCompletion(any(), any())).thenReturn(new LlmResVo(new EmptyUsage(), "generated", ""));
        LlmTaskExecutor executor = new LlmTaskExecutor(client, new ObjectMapper());
        NodeInvocation invocation = invocation("LLM", Map.of("template", "Rewrite {{draft}}", "modelId", "model"));

        Map<String, Object> outputs = executor.execute(invocation);

        ArgumentCaptor<LlmReqBo> request = ArgumentCaptor.forClass(LlmReqBo.class);
        verify(client).chatCompletion(request.capture(), any());
        assertEquals("Rewrite original", request.getValue().getUserMsg());
        assertEquals("execution", request.getValue().getNodeId());
        assertTrue(request.getValue().getHistory().isEmpty());
        assertEquals("generated", outputs.get("text"));
    }

    @Test
    void llmCanReturnStructuredOutputsForConditionalRouting() throws Exception {
        ModelServiceClient client = mock(ModelServiceClient.class);
        when(client.chatCompletion(any(), any())).thenReturn(new LlmResVo(new EmptyUsage(), "{\"passed\":true}", ""));

        Map<String, Object> result = new LlmTaskExecutor(client, new ObjectMapper())
                .execute(invocation("LLM", Map.of("outputMode", "JSON")));

        assertEquals(Map.of("passed", true), result);
    }

    @Test
    void pluginUsesAPrivateLegacyViewAndRestoresTheWorkerContext() throws Exception {
        PluginServiceClient client = mock(PluginServiceClient.class);
        when(client.toolCall(any(), any())).thenReturn(Map.of("result", "ok"));
        var previous = EngineContextHolder.initContext("previous", "chat", null);
        try {
            NodeInvocation invocation = invocation("PLUGIN", Map.of("pluginId", "test-tool"));
            new PluginTaskExecutor(client).execute(invocation);

            ArgumentCaptor<NodeState> state = ArgumentCaptor.forClass(NodeState.class);
            verify(client).toolCall(state.capture(), eq(invocation.inputs()));
            assertSame(previous, EngineContextHolder.get());
            assertEquals("plugin::execution", state.getValue().node().getId());
            assertNotSame(invocation.node().config(), state.getValue().node().getData().getNodeParam());
        } finally {
            EngineContextHolder.remove();
        }
    }

    private NodeInvocation invocation(String type, Map<String, Object> config) {
        NodeDefinition node = new NodeDefinition("node", type, config, Map.of(), null, null, false);
        return new NodeInvocation("run", "execution", "attempt", 1, "scope", 1, node,
                JsonValues.snapshot(Map.of("draft", "original")), Long.MAX_VALUE, () -> false, (kind, data) -> { });
    }
}
