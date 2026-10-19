package com.iflytek.astron.workflow.engine.v2.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmReqBo;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmResVo;
import com.iflytek.astron.workflow.engine.util.VariableTemplateRender;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class LlmTaskExecutor implements NodeTaskExecutor {
    private final ModelServiceClient client;
    private final ObjectMapper mapper;

    public LlmTaskExecutor(ModelServiceClient client, ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper.copy().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public String nodeType() {
        return "LLM";
    }

    @Override
    public Map<String, Object> execute(NodeInvocation invocation) throws Exception {
        Map<String, Object> config = invocation.node().config();
        LlmReqBo request = mapper.convertValue(config, LlmReqBo.class);
        request.setNodeId(invocation.executionId());
        String template = String.valueOf(config.getOrDefault("template", invocation.inputs().getOrDefault("prompt", "")));
        request.setUserMsg(VariableTemplateRender.render(template, invocation.inputs()));
        request.setSystemMsg(VariableTemplateRender.render((String) config.get("systemMsg"), invocation.inputs()));
        if (request.getHistory() == null) {
            request.setHistory(List.of());
        }
        LlmResVo response = client.chatCompletion(request, chunk -> {
            if (!invocation.cancelled().getAsBoolean() && chunk.getResult() != null) {
                String text = chunk.getResult().getOutput().getText();
                if (text != null) {
                    invocation.eventSink().accept("TEXT", Map.of("text", text));
                }
            }
        });
        if ("JSON".equals(config.get("outputMode"))) {
            return mapper.readValue(response.content(), new TypeReference<Map<String, Object>>() { });
        }
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put("text", response.content());
        outputs.put("reasoning", response.thinkContent());
        if (response.usage() != null) {
            outputs.put("usage", Map.of("promptTokens", response.usage().getPromptTokens(),
                    "completionTokens", response.usage().getCompletionTokens(),
                    "totalTokens", response.usage().getTotalTokens()));
        }
        return outputs;
    }
}
