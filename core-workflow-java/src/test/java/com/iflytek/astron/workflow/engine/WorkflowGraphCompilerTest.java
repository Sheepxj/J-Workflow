package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.node.StreamCallback;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class WorkflowGraphCompilerTest {
    @ParameterizedTest
    @ValueSource(strings = {"sequential", "failure-branch", "custom-error-output", "interrupt", "llm-history",
            "plugin", "knowledge", "branch-cycle", "exclusive-merge", "parallel-fork", "multiple-outcomes"})
    void acceptsExistingSinglePathDefinitionsWithoutChangingTheirDsl(String fixture) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture(fixture);
            String before = JSON.toJSONString(definition);
            assertEquals(definition.getNodes().size(), harness.executor.prepare(definition).nodeIds().size());
            assertEquals(before, JSON.toJSONString(definition));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"duplicate-node", "duplicate-edge", "unknown-endpoint", "start-incoming", "end-outgoing",
            "extra-start", "extra-end", "unknown-port", "unknown-node-type", "unknown-reference", "unreachable-node",
            "missing-end-path", "missing-branch-edge", "missing-branch-default", "enabled-retry-without-count"})
    void rejectsInvalidDefinitionsBeforeExecution(String mutation) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            Node llm = definition.findNodeById("spark-llm::generate");
            switch (mutation) {
                case "duplicate-node" -> definition.getNodes().add(llm);
                case "duplicate-edge" -> definition.getEdges().add(definition.getEdges().getFirst());
                case "unknown-endpoint" -> definition.getEdges().getFirst().setTargetNodeId("mock::absent");
                case "start-incoming" -> definition.getEdges().set(3, new Edge("if-else::check", "node-start::s", "branch_one_of::retry"));
                case "end-outgoing" -> definition.getEdges().add(new Edge("node-end::e", "spark-llm::generate", null));
                case "extra-start" -> definition.getNodes().add(copy(definition.findNodeById("node-start::s"), "node-start::extra"));
                case "extra-end" -> definition.getNodes().add(copy(definition.findNodeById("node-end::e"), "node-end::extra"));
                case "unknown-port" -> definition.getEdges().getFirst().setSourceHandle("unknown");
                case "unknown-node-type" -> llm.setId("unregistered::work");
                case "unknown-reference" -> llm.getData().getInputs().getFirst().getSchema()
                        .setValue(new Value("ref", Map.of("nodeId", "mock::absent", "name", "value")));
                case "unreachable-node" -> definition.getNodes().add(copy(llm, "spark-llm::unreachable"));
                case "missing-end-path" -> definition.getEdges().get(2).setTargetNodeId("spark-llm::generate");
                case "missing-branch-edge" -> definition.getEdges().removeLast();
                case "missing-branch-default" -> definition.findNodeById("if-else::check").getData().getNodeParam()
                        .put("cases", JSON.parseArray("[{\"id\":\"branch_one_of::pass\",\"conditions\":[{\"leftVarIndex\":\"input-actual\",\"compareOperator\":\"not_null\"}]}]"));
                case "enabled-retry-without-count" -> {
                    RetryConfig retry = new RetryConfig();
                    retry.setShouldRetry(true);
                    llm.getData().setRetryConfig(retry);
                }
                default -> throw new IllegalStateException(mutation);
            }

            assertThrows(NodeCustomException.class, () -> harness.executor.prepare(definition));
            assertEquals(0, harness.modelCalls.get());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"normal-fork", "failure-fork", "same-case-fork"})
    void acceptsMultipleSuccessFailureAndSelectedCaseSuccessors(String mutation) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            switch (mutation) {
                case "normal-fork" -> definition.getEdges().add(new Edge("spark-llm::generate", "node-end::e", null));
                case "failure-fork" -> {
                    definition.getEdges().add(new Edge("spark-llm::generate", "node-end::e", "intent_chain|first"));
                    definition.getEdges().add(new Edge("spark-llm::generate", "if-else::check", "intent_chain|second"));
                }
                case "same-case-fork" -> definition.getEdges().add(new Edge("if-else::check", "if-else::check", "branch_one_of::retry"));
                default -> throw new IllegalStateException(mutation);
            }

            assertEquals(definition.getNodes().size(), harness.executor.prepare(definition).nodeIds().size());
        }
    }

    @Test
    void allowsBranchSelfLoopsWithoutRequiringALoopContainer() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            definition.getEdges().getLast().setTargetNodeId("if-else::check");
            assertEquals(4, harness.executor.prepare(definition).nodeIds().size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void legacyEntriesCannotAccidentallyIgnoreRegisteredConditionalPorts(boolean parallel) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowStreamVirtualThreadExecutor streams = mock(WorkflowStreamVirtualThreadExecutor.class);
            WorkflowDSL definition = harness.fixture("exclusive-merge");
            ParallelWorkflowEngine parallelEngine = new ParallelWorkflowEngine(harness.executors, streams);
            try {
                IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
                    if (parallel) {
                        parallelEngine.execute(definition, new VariablePool(), Map.of("text", "left"), mock(StreamCallback.class));
                    } else {
                        new WorkflowEngine(harness.executors, streams).execute(definition, new VariablePool(),
                                Map.of("text", "left"), mock(StreamCallback.class));
                    }
                });
                assertTrue(failure.getMessage().contains("explicit WorkflowGraphExecutor"));
                assertEquals(0, harness.modelCalls.get());
            } finally {
                parallelEngine.destroy();
            }
        }
    }

    private Node copy(Node source, String id) {
        Node node = JSON.parseObject(JSON.toJSONString(source), Node.class);
        node.setId(id);
        return node;
    }
}
