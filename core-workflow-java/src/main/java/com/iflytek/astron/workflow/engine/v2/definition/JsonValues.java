package com.iflytek.astron.workflow.engine.v2.definition;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JsonValues {
    private JsonValues() {
    }

    public static Map<String, Object> snapshot(Map<String, ?> values) {
        Map<String, Object> copy = new LinkedHashMap<>();
        if (values != null) {
            values.forEach((key, value) -> {
                if (key == null) {
                    throw new IllegalArgumentException("JSON object keys must not be null");
                }
                copy.put(key, freeze(value));
            });
        }
        return Collections.unmodifiableMap(copy);
    }

    public static Object freeze(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean
                || value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Long || value instanceof BigDecimal || value instanceof BigInteger) {
            return value;
        }
        if (value instanceof Double number && Double.isFinite(number)) {
            return value;
        }
        if (value instanceof Float number && Float.isFinite(number)) {
            return value;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, item) -> {
                if (!(key instanceof String name)) {
                    throw new IllegalArgumentException("JSON object keys must be strings");
                }
                copy.put(name, freeze(item));
            });
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            list.forEach(item -> copy.add(freeze(item)));
            return Collections.unmodifiableList(copy);
        }
        throw new IllegalArgumentException("Unsupported JSON value: " + value.getClass().getSimpleName());
    }
}
