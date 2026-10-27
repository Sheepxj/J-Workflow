package com.iflytek.astron.workflow.engine;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.chain.Edge;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.InputSchema;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.NodeMeta;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.WorkflowExecutionLimits;
import com.iflytek.astron.workflow.engine.execution.WorkflowGraphRun;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class WorkflowGraphSuccessorsTest {
    @Test
    void originalForkFixtureMatchesTheLegacySerialEngineAndJoinsOnce() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("parallel-fork");
            WorkflowGraphRun run = execute(harness, definition);
            VariablePool legacy = new VariablePool();
            WorkflowStreamVirtualThreadExecutor streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
            try {
                new WorkflowEngine(harness.executors, streams).execute(copy(definition), legacy, Map.of("text", "seed"), (type, data) -> { });
            } finally {
                streams.destroy();
            }

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(legacy.get("node-end::e", "content"), run.outputs().get("content"));
            assertEquals(5, run.getContext().activationCount());
            assertEquals(1, run.getContext().activationCount("mock::merge"));
            NodeActivation merge = execution(run, "mock::merge", 0);
            assertEquals(2, run.arrivals(merge.activationId()).size());
            assertEquals(2, run.arrivals(merge.activationId()).stream().map(WorkflowGraphRun.Token::sourceExecutionId).distinct().count());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "failure", "fallback"})
    void selectedOutcomeDispatchesEverySuccessorAndSkipsTheOppositeOutcome(String outcome) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("multiple-outcomes");
            Node work = definition.findNodeById("mock::work");
            work.getData().setRetryConfig(null);
            work.getData().getNodeParam().put("failureRate", 0);
            if (!outcome.equals("success")) {
                work.getData().getNodeParam().put("failureRate", 1);
                RetryConfig retry = new RetryConfig();
                retry.setShouldRetry(false);
                retry.setErrorStrategy(outcome.equals("failure") ? 2 : 1);
                retry.setCustomOutput(Map.of("output", "fallback"));
                work.getData().setRetryConfig(retry);
            }

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            for (String suffix : List.of("left", "right")) {
                assertEquals(outcome.equals("failure") ? 0 : 1, run.getContext().activationCount("mock::" + suffix));
                assertEquals(outcome.equals("failure") ? 1 : 0, run.getContext().activationCount("mock::fail-" + suffix));
            }
            assertEquals(1, run.getContext().activationCount("mock::merge"));
            assertEquals(7, run.consumedTokenIds().size());
            assertEquals(1, execution(run, "mock::work", 0).attempts().size());
        }
    }

    @Test
    void unequalBranchesFinishBeforeTheJoinResolvesBothInputs() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            Node merge = work("mock::merge");
            merge.getData().setInputs(List.of(ref("left", "mock::left-3", "output"), ref("right", "mock::right", "output")));
            WorkflowDSL definition = definition(harness, List.of(work("mock::left-1"), work("mock::left-2"), work("mock::left-3"),
                    work("mock::right"), merge), List.of(edge("node-start::s", "mock::left-1"), edge("node-start::s", "mock::right"),
                    edge("mock::left-1", "mock::left-2"), edge("mock::left-2", "mock::left-3"),
                    edge("mock::left-3", "mock::merge"), edge("mock::right", "mock::merge"), edge("mock::merge", "node-end::e")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(Map.of("left", "mock::left-3", "right", "mock::right"), execution(run, "mock::merge", 0).inputSnapshot());
            assertEquals(1, run.getContext().activationCount("mock::merge"));
        }
    }

    @Test
    void variedDagForksAndJoinsKeepTheLegacyOncePerNodeContract() throws Exception {
        Random random = new Random(20261026);
        for (int sample = 0; sample < 20; sample++) {
            try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
                List<Node> nodes = new ArrayList<>();
                List<Edge> edges = new ArrayList<>();
                for (int index = 0; index < 12; index++) {
                    nodes.add(work("mock::" + index));
                }
                if (sample % 2 == 0) {
                    edges.add(edge("node-start::s", "mock::0"));
                    edges.add(edge("mock::11", "node-end::e"));
                } else {
                    for (int index = 0; index < 12; index++) {
                        edges.add(edge("node-start::s", "mock::" + index));
                        edges.add(edge("mock::" + index, "node-end::e"));
                    }
                }
                for (int source = 0; source < 11; source++) {
                    if (sample % 2 == 0) {
                        edges.add(edge("mock::" + source, "mock::" + (source + 1)));
                    }
                    for (int target = source + (sample % 2 == 0 ? 2 : 1); target < 12; target++) {
                        if (random.nextInt(4) == 0) {
                            edges.add(edge("mock::" + source, "mock::" + target));
                        }
                    }
                }
                WorkflowDSL definition = definition(harness, nodes, edges);

                WorkflowGraphRun run = execute(harness, definition);

                assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status(), "sample " + sample);
                assertEquals(14, run.getContext().activationCount(), "sample " + sample);
                for (Node node : definition.getNodes()) {
                    assertEquals(1, run.getContext().activationCount(node.getId()), node.getId());
                }
            }
        }
    }

    @Test
    void retriesDispatchTheFailureSuccessorsOnceAfterTheFinalAttempt() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            Node work = work("mock::work");
            work.getData().getNodeParam().put("failureRate", 1);
            RetryConfig retry = new RetryConfig();
            retry.setShouldRetry(true);
            retry.setMaxRetries(2);
            retry.setErrorStrategy(2);
            retry.setCustomOutput(Map.of());
            work.getData().setRetryConfig(retry);
            WorkflowDSL definition = definition(harness, List.of(work, work("mock::left"), work("mock::right")), List.of(
                    edge("node-start::s", "mock::work"), new Edge("mock::work", "mock::left", "intent_chain|left"),
                    new Edge("mock::work", "mock::right", "intent_chain|right"), edge("mock::left", "node-end::e"),
                    edge("mock::right", "node-end::e")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(3, execution(run, "mock::work", 0).attempts().size());
            assertEquals(1, run.getContext().activationCount("mock::left"));
            assertEquals(1, run.getContext().activationCount("mock::right"));
        }
    }

    @Test
    void failureSuccessorsCanRejoinAndRepeatInsideTheBusinessLoop() throws Exception {
        AtomicInteger recoveries = new AtomicInteger();
        NodeExecutor recovering = mockWork((state, inputs) -> {
            if (state.node().getId().equals("mock::work")) {
                throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "expected failure");
            }
            return success(Map.of("output", state.node().getId().equals("mock::left")
                    ? recoveries.incrementAndGet() == 3 ? "PASS" : "RETRY" : "right"));
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(recovering)) {
            Node work = work("mock::work");
            RetryConfig retry = new RetryConfig();
            retry.setShouldRetry(false);
            retry.setErrorStrategy(2);
            retry.setCustomOutput(Map.of());
            work.getData().setRetryConfig(retry);
            Node merge = work("mock::merge");
            merge.getData().setInputs(List.of(ref("left", "mock::left", "output"), ref("right", "mock::right", "output")));
            WorkflowDSL definition = definition(harness, List.of(work, work("mock::left"), work("mock::right"), merge,
                    decision("if-else::check", "mock::left", "output", "PASS")), List.of(edge("node-start::s", "mock::work"),
                    new Edge("mock::work", "mock::left", "intent_chain|left"), new Edge("mock::work", "mock::right", "intent_chain|right"),
                    edge("mock::left", "mock::merge"), edge("mock::right", "mock::merge"), edge("mock::merge", "if-else::check"),
                    new Edge("if-else::check", "mock::work", "no"), new Edge("if-else::check", "node-end::e", "yes")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(3, recoveries.get());
            assertEquals(3, run.getContext().activationCount("mock::merge"));
            assertEquals(Map.of("left", "PASS", "right", "right"), execution(run, "mock::merge", 2).inputSnapshot());
        }
    }

    @Test
    void nestedCrossingDagBranchesShareIntermediateJoinsWithoutDuplicateExecutions() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            Node c = work("mock::c");
            c.getData().setInputs(List.of(ref("input", "mock::b", "output")));
            WorkflowDSL definition = definition(harness, List.of(work("mock::a"), work("mock::b"), c, work("mock::d"), work("mock::merge")),
                    List.of(edge("node-start::s", "mock::a"), edge("node-start::s", "mock::b"), edge("mock::a", "mock::c"),
                            edge("mock::a", "mock::d"), edge("mock::b", "mock::c"), edge("mock::c", "mock::merge"),
                            edge("mock::d", "mock::merge"), edge("mock::merge", "node-end::e")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(7, run.getContext().activationCount());
            assertEquals("mock::b", execution(run, "mock::c", 0).inputSnapshot().get("input"));
            assertEquals(2, run.arrivals(execution(run, "mock::c", 0).activationId()).size());
            assertEquals(2, run.arrivals(execution(run, "mock::merge", 0).activationId()).size());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"yes", "no"})
    void everyEdgeOfTheSelectedIfElseCaseIsDispatched(String input) throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            Node decision = decision("if-else::check", "node-start::s", "text", "yes");
            WorkflowDSL definition = definition(harness, List.of(decision, work("mock::left"), work("mock::right"), work("mock::other")), List.of(
                    edge("node-start::s", "if-else::check"), new Edge("if-else::check", "mock::left", "yes"),
                    new Edge("if-else::check", "mock::right", "yes"), new Edge("if-else::check", "mock::other", "no"),
                    edge("mock::left", "node-end::e"), edge("mock::right", "node-end::e"), edge("mock::other", "node-end::e")));

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of("text", input), harness.callback, null);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(input.equals("yes") ? 1 : 0, run.getContext().activationCount("mock::left"));
            assertEquals(input.equals("yes") ? 1 : 0, run.getContext().activationCount("mock::right"));
            assertEquals(input.equals("yes") ? 0 : 1, run.getContext().activationCount("mock::other"));
            assertEquals(1, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void eachLoopIterationExecutesBothSuccessorsAndJoinsItsOwnOutputs() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            Node left = work("mock::left");
            left.getData().setInputs(List.of(ref("input", "spark-llm::generate", "answer")));
            Node right = work("mock::right");
            right.getData().setInputs(List.of(ref("input", "spark-llm::generate", "answer")));
            Node merge = work("mock::merge");
            merge.getData().setInputs(List.of(ref("left", "mock::left", "output"), ref("right", "mock::right", "output")));
            definition.getNodes().addAll(List.of(left, right, merge));
            definition.getEdges().remove(1);
            definition.getEdges().addAll(List.of(edge("spark-llm::generate", "mock::left"), edge("spark-llm::generate", "mock::right"),
                    edge("mock::left", "mock::merge"), edge("mock::right", "mock::merge"), edge("mock::merge", "if-else::check")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals("Result: PASS", run.outputs().get("content"));
            assertEquals(17, run.getContext().activationCount());
            for (String node : List.of("mock::left", "mock::right", "mock::merge")) {
                assertEquals(3, run.getContext().activationCount(node));
            }
            for (int index = 0; index < 3; index++) {
                String expected = index == 2 ? "PASS" : "RETRY";
                NodeActivation mergeExecution = execution(run, "mock::merge", index);
                assertEquals(Map.of("left", expected, "right", expected), mergeExecution.inputSnapshot());
                assertEquals(execution(run, "mock::left", index).executionId(), mergeExecution.inputOrigins().get("left").executionId());
                assertEquals(execution(run, "mock::right", index).executionId(), mergeExecution.inputOrigins().get("right").executionId());
                assertEquals(2, run.arrivals(mergeExecution.activationId()).size());
            }
        }
    }

    @Test
    void anInnerLoopFinishesBeforeJoiningItsIndependentSibling() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            Node merge = work("mock::merge");
            merge.getData().setInputs(List.of(ref("left", "spark-llm::generate", "answer"), ref("right", "mock::right", "output")));
            definition.getNodes().addAll(List.of(work("mock::right"), merge));
            definition.getEdges().get(2).setTargetNodeId("mock::merge");
            definition.getEdges().addAll(List.of(edge("node-start::s", "mock::right"), edge("mock::right", "mock::merge"),
                    edge("mock::merge", "node-end::e")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(3, harness.modelCalls.get());
            assertEquals(1, run.getContext().activationCount("mock::right"));
            assertEquals(1, run.getContext().activationCount("mock::merge"));
            assertEquals(Map.of("left", "PASS", "right", "mock::right"), execution(run, "mock::merge", 0).inputSnapshot());
        }
    }

    @Test
    void twoFeedbackArrivalsStartOneNewActivationInsteadOfMixingLoopIterations() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("branch-cycle");
            Node second = copy(definition.findNodeById("if-else::check"));
            second.setId("if-else::second");
            definition.getNodes().add(second);
            definition.getEdges().addAll(List.of(edge("spark-llm::generate", "if-else::second"),
                    new Edge("if-else::second", "node-end::e", "branch_one_of::pass"),
                    new Edge("if-else::second", "spark-llm::generate", "branch_one_of::retry")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(3, harness.modelCalls.get());
            assertEquals(3, run.getContext().activationCount("if-else::second"));
            assertEquals(2, run.arrivals(execution(run, "spark-llm::generate", 1).activationId()).size());
            assertEquals(1, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void multipleEntriesIntoACycleDoNotWaitForFuturePredecessors() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        NodeExecutor counter = mockWork((state, inputs) -> success(Map.of("output", calls.incrementAndGet() == 3 ? "PASS" : "RETRY")));
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(counter)) {
            WorkflowDSL definition = definition(harness, List.of(work("mock::work"), decision("if-else::check", "mock::work", "output", "PASS")), List.of(
                    edge("node-start::s", "mock::work"), edge("node-start::s", "if-else::check"), edge("mock::work", "if-else::check"),
                    new Edge("if-else::check", "mock::work", "no"), new Edge("if-else::check", "node-end::e", "yes")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(3, calls.get());
            assertEquals(3, run.getContext().activationCount("if-else::check"));
        }
    }

    @Test
    void replayOfOneJoinArrivalDoesNotAddAnotherBusinessActivation() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowGraphRun run = harness.executor.start(harness.fixture("parallel-fork"), Map.of("text", "seed"), harness.callback, null);
            harness.executor.tick(run);
            harness.executor.tick(run);
            WorkflowGraphRun.Token arrival = run.pending().stream().filter(token -> token.targetNodeId().equals("mock::merge")).findFirst().orElseThrow();
            run.replay(arrival);
            run.replay(arrival);

            harness.executor.runToCompletion(run);

            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(1, run.getContext().activationCount("mock::merge"));
            assertEquals(2, run.arrivals(execution(run, "mock::merge", 0).activationId()).size());
        }
    }

    @Test
    void blockedSiblingIsNotExecutedInParallel() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger rightCalls = new AtomicInteger();
        NodeExecutor blocking = mockWork((state, inputs) -> {
            if (state.node().getId().equals("mock::left")) {
                entered.countDown();
                try {
                    assertTrue(release.await(3, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(error);
                }
            } else if (state.node().getId().equals("mock::right")) {
                rightCalls.incrementAndGet();
            }
            return success(Map.of("output", "done"));
        });
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport(blocking);
             ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor()) {
            WorkflowGraphRun run = harness.executor.start(harness.fixture("parallel-fork"), Map.of("text", "seed"), harness.callback, null);
            harness.executor.tick(run);
            Future<Boolean> advancing = callers.submit(() -> harness.executor.tick(run));
            try {
                assertTrue(entered.await(2, TimeUnit.SECONDS));
                assertEquals(0, rightCalls.get());
                assertFalse(harness.executor.tick(run));
            } finally {
                release.countDown();
            }
            assertTrue(advancing.get(2, TimeUnit.SECONDS));
            harness.executor.runToCompletion(run);
            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, run.status());
            assertEquals(1, rightCalls.get());
        }
    }

    @Test
    void aStalledSiblingCannotBeHiddenByAnotherBranchReachingEnd() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = definition(harness, List.of(work("mock::left"), work("mock::right")), List.of(
                    edge("node-start::s", "mock::right"), edge("node-start::s", "mock::left"),
                    new Edge("mock::left", "node-end::e", "intent_chain|failure"), edge("mock::right", "node-end::e")));

            WorkflowGraphRun run = harness.executor.start(definition, Map.of("text", "seed"), harness.callback, null);
            assertTrue(harness.executor.tick(run));
            assertTrue(harness.executor.tick(run));
            assertTrue(run.pending().stream().anyMatch(token -> token.targetNodeId().equals("node-end::e")));
            assertEquals(0, run.getContext().activationCount("node-end::e"));
            harness.executor.runToCompletion(run);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.WORKFLOW_CONTROL_FLOW_STALLED.getCode(), run.errorCode());
            assertEquals(0, run.getContext().activationCount("node-end::e"));
        }
    }

    @Test
    void anUnhandledSiblingFailureStopsRemainingWorkWithoutExecutingEnd() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            Node failed = work("mock::failed");
            failed.getData().getNodeParam().put("failureRate", 1);
            WorkflowDSL definition = definition(harness, List.of(failed, work("mock::other")), List.of(
                    edge("node-start::s", "mock::failed"), edge("node-start::s", "mock::other"),
                    edge("mock::failed", "node-end::e"), edge("mock::other", "node-end::e")));

            WorkflowGraphRun run = execute(harness, definition);

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.NODE_RUN_ERROR.getCode(), run.errorCode());
            assertEquals(0, run.getContext().activationCount("mock::other"));
            assertEquals(0, run.getContext().activationCount("node-end::e"));
            assertTrue(run.pending().isEmpty());
        }
    }

    @Test
    void joinsCountAsOneActivationAndAnEndlessForkLoopStillHonorsTheBudget() throws Exception {
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            WorkflowDSL definition = harness.fixture("parallel-fork");
            WorkflowGraphRun exact = harness.executor.execute(definition, Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(1, 5, 5000));
            assertEquals(WorkflowGraphRun.Status.SUCCEEDED, exact.status());
        }
        try (WorkflowGraphTestSupport harness = new WorkflowGraphTestSupport()) {
            harness.responses("RETRY");
            WorkflowDSL definition = harness.fixture("branch-cycle");
            Node second = copy(definition.findNodeById("if-else::check"));
            second.setId("if-else::second");
            definition.getNodes().add(second);
            definition.getEdges().addAll(List.of(edge("spark-llm::generate", "if-else::second"),
                    new Edge("if-else::second", "node-end::e", "branch_one_of::pass"),
                    new Edge("if-else::second", "spark-llm::generate", "branch_one_of::retry")));

            WorkflowGraphRun run = harness.executor.execute(definition, Map.of("text", "seed"), harness.callback,
                    new WorkflowExecutionLimits(2, 100, 5000));

            assertEquals(WorkflowGraphRun.Status.FAILED, run.status());
            assertEquals(ErrorCode.WORKFLOW_BUDGET_EXCEEDED.getCode(), run.errorCode());
            assertEquals(2, harness.modelCalls.get());
        }
    }

    private WorkflowGraphRun execute(WorkflowGraphTestSupport harness, WorkflowDSL definition) {
        return harness.executor.execute(definition, Map.of("text", "seed"), harness.callback, null);
    }

    private WorkflowDSL definition(WorkflowGraphTestSupport harness, List<Node> body, List<Edge> edges) throws Exception {
        WorkflowDSL definition = harness.fixture("sequential");
        Node start = definition.findNodeById("node-start::s");
        Node end = definition.findNodeById("node-end::e");
        end.getData().setInputs(List.of());
        end.getData().getNodeParam().put("template", "done");
        List<Node> nodes = new ArrayList<>();
        nodes.add(start);
        nodes.addAll(body);
        nodes.add(end);
        definition.setNodes(nodes);
        definition.setEdges(new ArrayList<>(edges));
        return definition;
    }

    private Node work(String id) {
        Node node = new Node();
        node.setId(id);
        NodeData data = new NodeData();
        data.setNodeMeta(new NodeMeta(id.split("::")[0], id));
        data.getNodeParam().put("delayMs", 0);
        data.setInputs(List.of(new InputItem("input", "input", new InputSchema("string", new Value("literal", id)))));
        node.setData(data);
        return node;
    }

    private Node decision(String id, String source, String field, String expected) {
        Node node = work(id);
        node.getData().setInputs(List.of(ref("actual", source, field),
                new InputItem("expected", "expected", new InputSchema("string", new Value("literal", expected)))));
        node.getData().getNodeParam().put("cases", List.of(
                Map.of("id", "yes", "logicalOperator", "and", "conditions", List.of(
                        Map.of("leftVarIndex", "actual", "rightVarIndex", "expected", "compareOperator", "eq"))),
                Map.of("id", "no", "conditions", List.of())));
        return node;
    }

    private InputItem ref(String name, String nodeId, String field) {
        return new InputItem(name, name, new InputSchema("string", new Value("ref", Map.of("nodeId", nodeId, "name", field))));
    }

    private Edge edge(String source, String target) {
        return new Edge(source, target, null);
    }

    private NodeActivation execution(WorkflowGraphRun run, String nodeId, int index) {
        return run.getContext().executions().stream().filter(item -> item.nodeId().equals(nodeId)).toList().get(index);
    }

    private WorkflowDSL copy(WorkflowDSL source) {
        return JSON.parseObject(JSON.toJSONString(source), WorkflowDSL.class);
    }

    private Node copy(Node source) {
        return JSON.parseObject(JSON.toJSONString(source), Node.class);
    }

    private NodeExecutor mockWork(Work work) {
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

    private NodeRunResult success(Map<String, Object> outputs) {
        NodeRunResult result = new NodeRunResult();
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        result.setOutputs(outputs);
        return result;
    }

    @FunctionalInterface
    private interface Work {
        NodeRunResult call(NodeState state, Map<String, Object> inputs);
    }
}
