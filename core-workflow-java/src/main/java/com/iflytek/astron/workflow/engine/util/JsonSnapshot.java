package com.iflytek.astron.workflow.engine.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.callbacks.GenerateUsage;
import com.iflytek.astron.workflow.exception.NodeCustomException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JsonSnapshot {
    private JsonSnapshot() {
    }

    public static Map<String, Object> copy(Map<String, ?> value) {
        return freeze(value == null ? Map.of() : JSON.parseObject(JSON.toJSONString(value, JSONWriter.Feature.WriteNulls)));
    }

    public static Map<String, Object> mutable(Map<String, ?> value) {
        return value == null ? new LinkedHashMap<>() : JSON.parseObject(JSON.toJSONString(value, JSONWriter.Feature.WriteNulls));
    }

    public static NodeRunResult result(NodeRunResult source) {
        NodeRunResult copy = new NodeRunResult();
        copy.setInputs(copy(source.getInputs()));
        copy.setOutputs(copy(source.getOutputs()));
        copy.setErrorOutputs(copy(source.getErrorOutputs()));
        copy.setRawOutput(source.getRawOutput());
        copy.setNodeAnswerContent(source.getNodeAnswerContent());
        copy.setNodeAnswerReasoningContent(source.getNodeAnswerReasoningContent());
        GenerateUsage usage = source.getTokenCost();
        copy.setTokenCost(usage == null ? new GenerateUsage() : new GenerateUsage(
                usage.getCompletionTokens(), usage.getPromptTokens(), usage.getTotalTokens()));
        NodeCustomException error = source.getError();
        copy.setError(error == null ? null : new NodeCustomException(error.getCode(), error.getMessage()));
        copy.setStatus(source.getStatus());
        copy.setSelectedHandle(source.getSelectedHandle());
        return copy;
    }

    private static Map<String, Object> freeze(Map<String, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((key, item) -> result.put(key, freezeValue(item)));
        return Collections.unmodifiableMap(result);
    }

    private static Object freezeValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(String.valueOf(key), freezeValue(item)));
            return Collections.unmodifiableMap(result);
        }
        if (value instanceof List<?> list) {
            List<Object> result = new ArrayList<>();
            list.forEach(item -> result.add(freezeValue(item)));
            return Collections.unmodifiableList(result);
        }
        return value;
    }
}
