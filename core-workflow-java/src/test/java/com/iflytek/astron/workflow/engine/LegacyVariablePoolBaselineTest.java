package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LegacyVariablePoolBaselineTest {
    @Test
    void primitiveAndUuidValuesKeepTheirTypes() {
        VariablePool pool = new VariablePool();
        UUID uuid = UUID.randomUUID();
        pool.set("node", "integer", 7);
        pool.set("node", "boolean", true);
        pool.set("node", "uuid", uuid);

        assertEquals(7, pool.get("node", "integer"));
        assertEquals(true, pool.get("node", "boolean"));
        assertSame(uuid, pool.get("node", "uuid"));
    }

    @Test
    void objectsAndListsAreConvertedAndNestedArrayMembersCanBeReferenced() {
        VariablePool pool = new VariablePool();
        pool.set("node", "result", Map.of("items", List.of(Map.of("text", "first"), Map.of("text", "second"))));
        pool.set("node", "items", List.of(Map.of("text", "direct")));

        assertInstanceOf(JSONObject.class, pool.get("node", "result"));
        assertInstanceOf(JSONArray.class, pool.get("node", "items"));
        assertEquals("second", pool.get("node", "result.items[1].text"));
        assertEquals("direct", pool.get("node", "items[0].text"));
        assertNull(pool.get("node", "items[0]"));
    }

    @Test
    void directMissingValuesReturnNullButMissingIntermediatePathsThrow() {
        VariablePool pool = new VariablePool();
        pool.set("node", "result", Map.of("exists", "yes"));

        assertNull(pool.get("unknown", "value"));
        assertNull(pool.get("node", "missing"));
        assertNull(pool.get("node", "result.missing"));
        assertThrows(NullPointerException.class, () -> pool.get("node", "missing.value"));
        assertThrows(ClassCastException.class, () -> pool.get("node", "result.exists.value"));
    }

    @Test
    void nullWritesAndInvalidArrayIndexesAreNotNormalized() {
        VariablePool pool = new VariablePool();
        pool.set("node", "items", List.of(Map.of("text", "first")));

        assertThrows(NullPointerException.class, () -> pool.set("node", "nullable", null));
        assertThrows(IndexOutOfBoundsException.class, () -> pool.get("node", "items[4].text"));
        assertThrows(NumberFormatException.class, () -> pool.get("node", "items[bad].text"));
    }

    @Test
    void perFieldUpdatesRetainEarlierOutputsAndExposeTheLiveNodeMap() {
        VariablePool pool = new VariablePool();
        pool.set("node", "oldOnly", "stale");
        pool.set("node", "value", "first");
        pool.set("node", "value", "second");

        assertEquals("stale", pool.get("node", "oldOnly"));
        assertEquals("second", pool.get("node", "value"));
        pool.get("node").put("external", "mutable");
        assertEquals("mutable", pool.get("node", "external"));
    }

    @Test
    void differentPoolInstancesDoNotShareOutputs() {
        VariablePool first = new VariablePool();
        VariablePool second = new VariablePool();
        first.set("node", "value", "first");
        second.set("node", "value", "second");
        first.clear();

        assertEquals(Map.of(), first.get("node"));
        assertEquals("second", second.get("node", "value"));
    }
}
