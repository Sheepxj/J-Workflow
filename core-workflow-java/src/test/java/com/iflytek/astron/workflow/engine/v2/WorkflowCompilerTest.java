package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.v2.compiler.CompiledWorkflow;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.JsonValues;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkflowCompilerTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final WorkflowCompiler compiler = new WorkflowCompiler(mapper, Set.of("MOCK"));

    @Test
    void compilesTheDocumentedCyclicWorkflowWithoutMutatingItsDefinition() throws IOException {
        WorkflowDefinition definition = definition(sample());
        CompiledWorkflow compiled = compiler.compile(definition);

        assertEquals(5, compiled.nodes().size());
        assertEquals("start", compiled.startNodeId());
        assertEquals("refine", compiled.nodeLoopIds().get("generate"));
        assertEquals(64, compiled.contentHash().length());
        assertEquals(compiled.contentHash(), compiler.compile(definition).contentHash());
        assertThrows(UnsupportedOperationException.class, () -> compiled.nodes().clear());
        assertThrows(UnsupportedOperationException.class, () -> compiled.outgoing().get("loop").clear());
    }

    @Test
    void rejectsUndeclaredBackEdges() throws IOException {
        ObjectNode tree = sample();
        ((ObjectNode) tree.withArray("edges").get(3)).put("edgeType", "NORMAL");

        assertEquals(WorkflowError.INVALID_SCOPE, failure(tree).getCode());
    }

    @Test
    void rejectsExternalEntriesIntoTheLoopBody() throws IOException {
        ObjectNode tree = sample();
        ((ObjectNode) tree.withArray("edges").get(0)).put("target", "generate").remove("targetPort");

        assertNotNull(failure(tree));
    }

    @Test
    void rejectsCyclesInsideALoopBody() throws IOException {
        ObjectNode tree = sample();
        ObjectNode edge = (ObjectNode) tree.withArray("edges").get(2);
        edge.put("target", "generate");

        assertEquals(WorkflowError.INVALID_CYCLE, failure(tree).getCode());
    }

    @Test
    void rejectsUnregisteredTypesAndAmbiguousSequentialFanOut() throws IOException {
        ObjectNode tree = sample();
        ((ObjectNode) tree.withArray("nodes").get(2)).put("type", "UNKNOWN");
        assertEquals(WorkflowError.INVALID_DEFINITION, failure(tree).getCode());

        tree = sample();
        tree.withArray("edges").addObject().put("id", "extra").put("source", "generate").put("target", "validate");
        assertEquals(WorkflowError.INVALID_DEFINITION, failure(tree).getCode());
    }

    @Test
    void rejectsDuplicateNodesAndNonPositiveLimits() throws IOException {
        ObjectNode tree = sample();
        tree.withArray("nodes").add(tree.withArray("nodes").get(0).deepCopy());
        assertNotNull(failure(tree));
        tree = sample();
        ((ObjectNode) tree.get("limits")).put("maxNodeExecutions", 0);
        assertNotNull(failure(tree));
        tree = sample();
        ((ObjectNode) tree.get("limits")).put("maxControlTransitions", 0);
        assertNotNull(failure(tree));
        tree = sample();
        ((ObjectNode) tree.get("limits")).put("maxConcurrentAttempts", 0);
        assertNotNull(failure(tree));
    }

    @Test
    void additiveParallelDefaultsPreserveThePreviouslyStoredDefinitionHash() throws Exception {
        WorkflowDefinition definition = definition(sample());
        ObjectMapper canonicalMapper = mapper.copy().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        ObjectNode oldShape = canonicalMapper.valueToTree(definition);
        oldShape.remove("parallels");
        ((ObjectNode) oldShape.get("limits")).remove("maxConcurrentAttempts");
        oldShape.withArray("edges").forEach(edge -> ((ObjectNode) edge).remove("on"));
        String oldHash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(canonicalMapper.writeValueAsBytes(oldShape)));

        assertEquals(oldHash, compiler.compile(definition).contentHash());
    }

    @Test
    void snapshotsNestedMutableDataIncludingNullValues() {
        List<Object> original = new ArrayList<>();
        original.add(null);
        Map<String, Object> values = new HashMap<>();
        values.put("nested", original);
        Map<String, Object> snapshot = JsonValues.snapshot(values);
        original.add("later");
        values.clear();

        assertEquals(1, ((List<?>) snapshot.get("nested")).size());
        assertThrows(UnsupportedOperationException.class, () -> ((List<?>) snapshot.get("nested")).clear());
    }

    private WorkflowExecutionException failure(ObjectNode tree) {
        return assertThrows(WorkflowExecutionException.class, () -> compiler.compile(definition(tree)));
    }

    private WorkflowDefinition definition(ObjectNode tree) throws IOException {
        return mapper.treeToValue(tree, WorkflowDefinition.class);
    }

    static ObjectNode sample() throws IOException {
        try (var stream = WorkflowCompilerTest.class.getResourceAsStream("/workflows/refine.json")) {
            return (ObjectNode) new ObjectMapper().readTree(stream);
        }
    }
}
