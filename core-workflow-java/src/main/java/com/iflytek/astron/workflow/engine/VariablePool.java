package com.iflytek.astron.workflow.engine;

import cn.hutool.core.util.ClassUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

/**
 * fixme 这里需要调整为实例级别，全局共享会存在OOM风险
 * <p>
 * Variable pool for managing node outputs and resolving template references
 * Handles variable references in format: {{node-id.output-name}}
 * <p>
 * Example:
 * - Template: "User said: {{node-start::001.user_input}}"
 * - After resolution: "User said: 介绍一下 Java"
 */
@Slf4j
public class VariablePool {

    /**
     * Storage for node outputs
     * Key: "node-id.output-name" (e.g., "node-start::001.user_input")
     * Value: actual output value
     */
    private final Map<String, Map<String, Object>> variables = new ConcurrentHashMap<>();

    /**
     * Set a variable in the pool
     *
     * @param nodeId     node ID (e.g., "node-start::001")
     * @param outputName output name (e.g., "user_input")
     * @param value      actual value
     */
    public synchronized void set(String nodeId, String outputName, Object value) {
        // 基本数据类型、Number、String格式，直接存储； 非基本数据类型，如列表、Map、对象，则转换为JsonObject，JsonArray的方式进行保存
        // 以JsonArray, JsonObject的方式持有变量；方便后续的变量解析
        if (ClassUtil.isPrimitiveWrapper(value.getClass()) || value instanceof Number || value instanceof String
                || value instanceof JSONArray || value instanceof JSONObject || value instanceof UUID) {

        } else if (value instanceof List<?>) {
            value = JSON.parseArray(JSON.toJSONString(value));
        } else {
            value = JSON.parseObject(JSON.toJSONString(value));
        }

        variables.computeIfAbsent(nodeId, k -> new ConcurrentHashMap<>()).put(outputName, value);
    }

    /**
     * Get a variable from the pool
     *
     * @param nodeId     node ID
     * @param outputName output name
     * @return variable value, or null if not found
     */
    public synchronized Object get(String nodeId, String outputName) {
        Map target = variables.getOrDefault(nodeId, Map.of());
        return getVal(target, outputName);
    }

    public synchronized Map<String, Object> get(String nodeId) {
        return variables.getOrDefault(nodeId, Map.of());
    }


    /**
     * 参数提取
     *
     * @param map
     * @param key - data.voice_url 表示从Map中获取key = data的map，然后再从这个map中获取key = voice_url的value
     *            - data[0].voice_url 表示从Map中获取key = data的列表，取第一个元素（是个map），然后从这个map中获取key = voice_url的value
     * @return
     */
    @SuppressWarnings("unchecked")
    private Object getVal(Map map, String key) {
        int index = key.indexOf(".");
        if (index < 0) {
            return map.get(key);
        }

        String rootKey = key.substring(0, index);
        String subKey = key.substring(index + 1);

        Map subMap;

        // 如果subKey中形如 xxx[0]，则需要进一步提取
        index = rootKey.indexOf("[");
        if (index > 0 && rootKey.endsWith("]")) {
            String subIndex = rootKey.substring(index + 1, rootKey.length() - 1);
            rootKey = rootKey.substring(0, index);

            subMap = (Map) ((List) map.get(rootKey)).get(Integer.parseInt(subIndex));
        } else {
            subMap = (Map) map.get(rootKey);
        }


        return getVal(subMap, subKey);
    }

    /**
     * Clear all variables (used between workflow executions)
     */
    public synchronized void clear() {
        variables.clear();
    }

    public synchronized Map<String, Map<String, Object>> snapshot() {
        Map<String, Map<String, Object>> snapshot = new LinkedHashMap<>();
        variables.forEach((nodeId, outputs) -> snapshot.put(nodeId, JsonSnapshot.copy(outputs)));
        return Collections.unmodifiableMap(snapshot);
    }

    public synchronized void restore(Map<String, ? extends Map<String, Object>> snapshot) {
        Map<String, Map<String, Object>> restored = new LinkedHashMap<>();
        Objects.requireNonNull(snapshot).forEach((nodeId, outputs) -> restored.put(
                Objects.requireNonNull(nodeId), JsonSnapshot.mutable(Objects.requireNonNull(outputs))));
        variables.clear();
        variables.putAll(restored);
    }

    public synchronized void replaceNodeOutputs(String nodeId, Map<String, Object> outputs) {
        variables.put(Objects.requireNonNull(nodeId), JsonSnapshot.mutable(Objects.requireNonNull(outputs)));
    }

    public synchronized boolean contains(String nodeId, String outputName) {
        return nodeId != null && containsValue(variables.get(nodeId), outputName);
    }

    private boolean containsValue(Map<?, ?> values, String path) {
        if (values == null || path == null) {
            return false;
        }
        int dot = path.indexOf('.');
        if (dot < 0) {
            return values.containsKey(path);
        }
        String key = path.substring(0, dot);
        Object nested;
        int bracket = key.indexOf('[');
        if (bracket > 0 && key.endsWith("]")) {
            Object list = values.get(key.substring(0, bracket));
            try {
                int index = Integer.parseInt(key.substring(bracket + 1, key.length() - 1));
                if (!(list instanceof List<?> items) || index < 0 || index >= items.size()) {
                    return false;
                }
                nested = items.get(index);
            } catch (NumberFormatException invalid) {
                return false;
            }
        } else {
            nested = values.get(key);
        }
        return nested instanceof Map<?, ?> map && containsValue(map, path.substring(dot + 1));
    }
}
