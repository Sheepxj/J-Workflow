package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.TypeReference;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VariablePoolSnapshotTest {
    @Test
    void snapshotAndRestoreSurviveJsonAndPreserveExplicitNulls() {
        VariablePool pool = new VariablePool();
        Map<String, Object> outputs = new LinkedHashMap<>();
        outputs.put("items", List.of(Map.of("text", "first")));
        outputs.put("nullable", null);
        pool.replaceNodeOutputs("node", outputs);

        String json = JSON.toJSONString(pool.snapshot(), JSONWriter.Feature.WriteNulls);
        Map<String, Map<String, Object>> persisted = JSON.parseObject(json, new TypeReference<>() { });
        VariablePool restored = new VariablePool();
        restored.set("stale", "value", "removed");
        restored.restore(persisted);

        assertEquals("first", restored.get("node", "items[0].text"));
        assertTrue(restored.get("node").containsKey("nullable"));
        assertNull(restored.get("node", "nullable"));
        assertEquals(Map.of(), restored.get("stale"));
        assertEquals(pool.snapshot(), restored.snapshot());
    }

    @Test
    void snapshotsAndReplacementInputsCannotMutateStoredOutputs() {
        VariablePool pool = new VariablePool();
        List<String> items = new ArrayList<>(List.of("original"));
        Map<String, Object> outputs = new LinkedHashMap<>(Map.of("items", items));
        pool.replaceNodeOutputs("node", outputs);
        Map<String, Map<String, Object>> snapshot = pool.snapshot();
        items.add("external");
        outputs.put("extra", "external");
        pool.set("node", "later", "not-in-snapshot");

        assertEquals(List.of("original"), snapshot.get("node").get("items"));
        assertFalse(snapshot.get("node").containsKey("later"));
        assertEquals(List.of("original"), pool.get("node", "items"));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.get("node").put("changed", true));
        assertThrows(UnsupportedOperationException.class, () -> ((List<?>) snapshot.get("node").get("items")).clear());
    }

    @Test
    void replacingAWholeOutputRemovesFieldsFromEarlierExecutions() {
        VariablePool pool = new VariablePool();
        pool.set("node", "oldOnly", "stale");
        pool.set("node", "output", "first");

        pool.replaceNodeOutputs("node", Map.of("output", "second"));

        assertEquals(Map.of("output", "second"), pool.get("node"));
        pool.replaceNodeOutputs("node", Map.of());
        assertEquals(Map.of(), pool.get("node"));
    }

    @Test
    void invalidRestoreDoesNotPartiallyReplaceExistingVariables() {
        VariablePool pool = new VariablePool();
        pool.set("node", "output", "original");
        Map<String, Map<String, Object>> invalid = new LinkedHashMap<>();
        invalid.put("new", Map.of("output", "new"));
        invalid.put("invalid", null);

        assertThrows(NullPointerException.class, () -> pool.restore(invalid));

        assertEquals(Map.of("node", Map.of("output", "original")), pool.snapshot());
    }
}
