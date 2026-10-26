package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.NodeMeta;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.WorkflowExecutionLimits;
import com.iflytek.astron.workflow.engine.execution.WorkflowGraph;
import com.iflytek.astron.workflow.engine.execution.WorkflowGraphRun;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class WorkflowGraphExecutorTest {
    @Test
    void originalDslRunsThreeCyclesThroughTheOriginalLlmAndKeepsEveryExecution() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals("Result: PASS", run.outputs().get("content"));
            assertEquals(8, run.getContext().executions().size());
            List<NodeActivation> generations = run.getContext().executions().stream()
                    .filter(item -> item.nodeId().equals("spark-llm::generate")).toList();
            assertEquals(List.of(1, 2, 3), generations.stream().map(NodeActivation::activationNo).toList());
            assertEquals(List.of("RETRY", "RETRY", "PASS"), generations.stream().map(item -> item.result().getOutputs().get("answer")).toList());
            List<NodeActivation> decisions = run.getContext().executions().stream()
                    .filter(item -> item.nodeId().equals("if-else::check")).toList();
            assertEquals(generations.stream().map(NodeActivation::executionId).toList(),
                    decisions.stream().map(item -> item.inputOrigins().get("actual").executionId()).toList());
            assertEquals(List.of("branch_one_of::retry", "branch_one_of::retry", "branch_one_of::pass"),
                    decisions.stream().map(item -> item.result().getSelectedHandle()).toList());
            verify(harness.llm, times(3)).execute(any());
            assertNull(definition.findNodeById("spark-llm::generate").getStatus());
            assertEquals("flow_start", harness.frames.getFirst().getWorkflowStep().getNode().getAliasName());
            assertEquals("flow_end", harness.frames.getLast().getWorkflowStep().getNode().getAliasName());
        }
    }

    @Test
    void anInitialDecisionCanSkipTheWholeLoopBody() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            definition.findNodeById("if-else::check").getData().getInputs().getFirst().getSchema()
                    .setValue(new Value("ref", Map.of("nodeId", "node-start::s", "name", "text")));
            definition.findNodeById("node-end::e").getData().getInputs().getFirst().getSchema()
                    .setValue(new Value("ref", Map.of("nodeId", "node-start::s", "name", "text")));
            definition.getEdges().set(0, new Edge("node-start::s", "if-else::check", null));

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of("text", "PASS"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals("Result: PASS", run.outputs().get("content"));
            assertEquals(3, run.getContext().executions().size());
            assertEquals(0, harness.modelCalls.get());
        }
    }

    @ParameterizedTest
    @CsvSource({"left, mock::left, mock::right", "other, mock::right, mock::left"})
    void exclusivePathsMergeWithoutExecutingOrWaitingForTheOtherPath(String text, String selected, String skipped) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("exclusive-merge"), Map.of("text", text), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(1, run.getContext().activationCount(selected));
            assertEquals(0, run.getContext().activationCount(skipped));
            assertEquals(1, run.getContext().activationCount("mock::merge"));
        }
    }

    @ParameterizedTest
    @CsvSource({"sequential, Result: seed", "custom-error-output, Result: fallback", "failure-branch, Result: recovered", "plugin, Result: tool-output"})
    void originalSuccessAndErrorRoutesStillExecuteTheirExistingNodes(String fixture, String expected) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture(fixture), Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(expected, run.outputs().get("content"));
            if (fixture.equals("failure-branch")) {
                assertEquals(0, run.getContext().activationCount("mock::normal"));
                assertEquals(1, run.getContext().activationCount("mock::handler"));
            }
        }
    }

    @Test
    void originalKnowledgeWorkflowKeepsItsContextAndEndFormatting() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("knowledge"), Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertTrue(run.outputs().get("content").toString().contains("accepted"));
            assertEquals(3, run.getContext().variables().get("knowledge-base::search").size());
        }
    }

    @Test
    void unhandledFailureStopsTheGraphAndItsWorkflowEndCarriesTheError() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("interrupt"), Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.NODE_RUN_ERROR.getCode(), run.errorCode());
            assertEquals(0, run.getContext().activationCount("node-end::e"));
            assertTrue(run.pending().isEmpty());
            assertEquals(ErrorCode.NODE_RUN_ERROR.getCode(), harness.frames.getLast().getCode());
        }
    }

    @Test
    void replayedTokensDoNotRepeatCompletedActivations() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            harness.responses("PASS");
            WorkflowGraphRun run = harness.executor.start(harness.fixture("branch-cycle"), Map.of("text", "seed"), harness.callback, null);
            WorkflowGraphRun.Token first = run.pending().getFirst();
            run.replay(first);
            harness.executor.tick(run);
            run.replay(first);
            harness.executor.runToCompletion(run);
            run.replay(first);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(4, run.getContext().activationCount());
            assertEquals(1, run.getContext().activationCount("node-start::s"));
            assertEquals(1, harness.modelCalls.get());
            assertThrows(NodeCustomException.class, () -> run.replay(new WorkflowGraphRun.Token(first.id(), "node-end::e", null, null)));
        }
    }

    @Test
    void concurrentTicksCannotDispatchTheSameControlTokenTwice() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        NodeExecutor blocking = mockWork((state, inputs) -> {
            calls.incrementAndGet();
            entered.countDown();
            await(release);
            return result(Map.of("output", "done"));
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(blocking);
             ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            WorkflowGraphRun run = harness.executor.start(harness.fixture("sequential"), Map.of("text", "seed"), harness.callback, null);
            harness.executor.tick(run);
            run.replay(run.pending().getFirst());
            Future<Boolean> first = callers.submit(() -> harness.executor.tick(run));
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertFalse(harness.executor.tick(run));
            } finally {
                release.countDown();
            }
            assertTrue(first.get(2, TimeUnit.SECONDS));
            harness.executor.runToCompletion(run);
            assertEquals(1, calls.get());
            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
        }
    }

    @Test
    void missingCurrentOutputCannotReuseAPreviousLoopsField() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        NodeExecutor producer = new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.LLM;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return result(calls.incrementAndGet() == 1 ? Map.of("answer", "RETRY") : Map.of());
            }
        };
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(producer)) {
            WorkflowGraphRun run = harness.executor.execute(harness.fixture("branch-cycle"), Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.WORKFLOW_INPUT_UNAVAILABLE.getCode(), run.errorCode());
            assertEquals(2, calls.get());
            assertEquals(Map.of(), run.getContext().variables().get("spark-llm::generate"));
            assertEquals(0, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void aMissingSuccessRouteIsReportedAsStalledInsteadOfSucceeded() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("sequential");
            definition.getEdges().getLast().setSourceHandle("intent_chain|fail_one_of");

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of("text", "seed"), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.WORKFLOW_CONTROL_FLOW_STALLED.getCode(), run.errorCode());
        }
    }

    @Test
    void preparedGraphDoesNotFollowLaterDefinitionOrReturnedNodeChanges() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            harness.responses("PASS");
            WorkflowDSL definition = harness.fixture("branch-cycle");
            WorkflowGraph graph = harness.executor.prepare(definition);
            definition.getEdges().clear();
            definition.findNodeById("spark-llm::generate").getData().getNodeParam().clear();
            graph.endNodeDefinition().getData().getNodeParam().put("template", "wrong");
            WorkflowGraphRun run = harness.executor.start(graph, Map.of("text", "seed"), harness.callback, null);

            harness.executor.runToCompletion(run);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals("Result: PASS", run.outputs().get("content"));
        }
    }

    @Test
    void unsupportedForksAreRejectedBeforeWorkflowCallbacksOrBusinessCalls() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            NodeCustomException error = assertThrows(NodeCustomException.class, () -> harness.executor.execute(
                    harness.fixture("parallel-fork"), Map.of("text", "seed"), harness.callback, null));

            assertEquals(ErrorCode.UNSUPPORTED_WORKFLOW_GRAPH.getCode(), error.getCode());
            verify(harness.callback, never()).onWorkflowStart();
            assertEquals(0, harness.modelCalls.get());
        }
    }

    @Test
    void longChainsAdvanceThroughTheQueueWithoutRecursiveNodeCalls() throws Exception {
        NodeExecutor empty = mockWork((state, inputs) -> result(Map.of()));
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(empty)) {
            WorkflowDSL template = harness.fixture("sequential");
            List<Node> nodes = new ArrayList<>();
            List<Edge> edges = new ArrayList<>();
            nodes.add(template.findNodeById("node-start::s"));
            for (int index = 0; index < 1000; index++) {
                Node node = new Node();
                node.setId("mock::" + index);
                NodeData data = new NodeData();
                data.setNodeMeta(new NodeMeta("mock", node.getId()));
                node.setData(data);
                nodes.add(node);
            }
            Node end = template.findNodeById("node-end::e");
            end.getData().setInputs(List.of());
            end.getData().getNodeParam().put("template", "done");
            nodes.add(end);
            for (int index = 1; index < nodes.size(); index++) {
                edges.add(new Edge(nodes.get(index - 1).getId(), nodes.get(index).getId(), null));
            }
            WorkflowDSL definition = new WorkflowDSL("long-chain", "long-chat", nodes, edges);

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of(), harness.callback,
                    new WorkflowExecutionLimits(1, 1002, 60000));

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(1002, run.getContext().activationCount());
            assertEquals("done", run.outputs().get("content"));
        }
    }

    private static NodeExecutor mockWork(Work work) {
        return new AbstractNodeExecutor() {
            @Override
            public NodeTypeEnum getNodeType() {
                return NodeTypeEnum.MOCK;
            }

            @Override
            protected NodeRunResult executeNode(NodeState state, Map<String, Object> inputs) {
                return work.call(state, inputs);
            }
        };
    }

    private static NodeRunResult result(Map<String, Object> outputs) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        result.setOutputs(outputs);
        return result;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(4, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test latch timed out");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        }
    }

    @FunctionalInterface
    private interface Work {
        NodeRunResult call(NodeState state, Map<String, Object> inputs);
    }
}
