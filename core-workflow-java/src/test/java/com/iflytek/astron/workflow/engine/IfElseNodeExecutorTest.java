package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.domain.chain.InputItem;
import com.iflytek.astron.workflow.engine.domain.chain.InputSchema;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.domain.chain.Value;
import com.iflytek.astron.workflow.engine.execution.NodeActivation;
import com.iflytek.astron.workflow.engine.execution.NodeExecutionBridge;
import com.iflytek.astron.workflow.engine.execution.WorkflowRunContext;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.node.impl.condition.IfElseNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class IfElseNodeExecutorTest {
    private WorkflowStreamVirtualThreadExecutor streams;
    private WorkflowMsgCallback callback;
    private final IfElseNodeExecutor executor = new IfElseNodeExecutor();

    @BeforeEach
    void setUp() {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = new WorkflowMsgCallback("condition", (type, data) -> { }, EndNodeOutputModeEnum.VARIABLE_MODE,
                new ArrayBlockingQueue<>(64), new ConcurrentLinkedQueue<>(), streams);
    }

    @AfterEach
    void tearDown() {
        callback.finished();
        streams.destroy();
    }

    static Stream<Arguments> comparisons() {
        return Stream.of(
                Arguments.of("contains", "AlphaBeta", "Beta", true),
                Arguments.of("contains", "AlphaBeta", "beta", false),
                Arguments.of("not_contains", "Alpha", "z", true),
                Arguments.of("contains", List.of("1", 2), new BigDecimal("2.0"), true),
                Arguments.of("contains", List.of("1", 2), 1, false),
                Arguments.of("not_contains", List.of(1, 2), 3, true),
                Arguments.of("contains", List.of(Map.of("a", 1)), Map.of("a", 1.0), true),
                Arguments.of("empty", "", null, true),
                Arguments.of("empty", " ", null, false),
                Arguments.of("empty", List.of(), null, true),
                Arguments.of("empty", Map.of(), null, true),
                Arguments.of("not_empty", List.of(0), null, true),
                Arguments.of("not_empty", Map.of(), null, false),
                Arguments.of("is", true, true, true),
                Arguments.of("is", false, true, false),
                Arguments.of("is_not", false, true, true),
                Arguments.of("start_with", "Abcd", "Ab", true),
                Arguments.of("start_with", "Abcd", "ab", false),
                Arguments.of("end_with", "Abcd", "cd", true),
                Arguments.of("eq", 1, new BigDecimal("1.00"), true),
                Arguments.of("eq", new BigInteger("9007199254740993"), new BigDecimal("9007199254740993"), true),
                Arguments.of("ne", 1L, 1.0, false),
                Arguments.of("eq", "01", "1", false),
                Arguments.of("ne", "a", "b", true),
                Arguments.of("eq", List.of(1, Map.of("x", 2)), List.of(1.0, Map.of("x", 2.0)), true),
                Arguments.of("eq", Map.of("x", "1"), Map.of("x", 1), false),
                Arguments.of("eq", null, null, true),
                Arguments.of("gt", 2, 1, true),
                Arguments.of("gt", 1, 1, false),
                Arguments.of("ge", 1, 1.0, true),
                Arguments.of("lt", -2, -1, true),
                Arguments.of("le", 1, 1, true),
                Arguments.of("null", null, null, true),
                Arguments.of("null", "", null, false),
                Arguments.of("not_null", false, null, true));
    }

    @ParameterizedTest
    @MethodSource("comparisons")
    void originalOperatorNamesSelectAHandleWithoutProducingBusinessFailure(String operator, Object left, Object right,
                                                                         boolean matches) throws Exception {
        Node node = branch(operator, left, right);
        NodeActivation result = execute(node, new VariablePool());

        assertEquals(NodeActivation.Status.SUCCEEDED, result.status());
        assertEquals(NodeExecStatusEnum.SUCCESS, result.result().getStatus());
        assertEquals(matches ? "branch_one_of::yes" : "branch_one_of::default", result.result().getSelectedHandle());
        assertEquals(Map.of(), result.result().getOutputs());
    }

    static Stream<Arguments> incompatible() {
        return Stream.of(Arguments.of("eq", "1", 1), Arguments.of("ne", false, "false"),
                Arguments.of("gt", "2", 1), Arguments.of("contains", "abc", 1),
                Arguments.of("empty", null, null), Arguments.of("not_empty", 1, null),
                Arguments.of("is", "true", true), Arguments.of("end_with", 1, "1"),
                Arguments.of("eq", null, ""));
    }

    @ParameterizedTest
    @MethodSource("incompatible")
    void typeMismatchIsAnErrorAndDoesNotSelectTheDefault(String operator, Object left, Object right) throws Exception {
        NodeActivation result = execute(branch(operator, left, right), new VariablePool());

        assertEquals(NodeActivation.Status.FAILED, result.status());
        assertEquals(ErrorCode.CONDITION_TYPE_ERROR.getCode(), result.result().getError().getCode());
        assertEquals(null, result.result().getSelectedHandle());
    }

    @Test
    void unaryComparisonDoesNotValidateOrReadItsUnusedRightBinding() throws Exception {
        Node node = branch("null", null, "unused");
        node.getData().getInputs().get(1).setSchema(null);

        NodeActivation result = execute(node, new VariablePool());

        assertEquals("branch_one_of::yes", result.result().getSelectedHandle());
        assertEquals(1, result.inputSnapshot().size());
        assertFalse(result.inputSnapshot().containsKey("right-name"));
    }

    @Test
    void missingReferenceDoesNotCountAsAnExplicitNull() throws Exception {
        Node node = branch("null", null, null);
        node.getData().getInputs().getFirst().getSchema().setValue(new Value("ref", Map.of("nodeId", "future", "name", "value")));

        NodeActivation result = execute(node, new VariablePool());

        assertEquals(NodeActivation.Status.FAILED, result.status());
        assertEquals(ErrorCode.WORKFLOW_INPUT_UNAVAILABLE.getCode(), result.result().getError().getCode());
        assertFalse(result.inputOrigins().get("left-name").present());
    }

    @Test
    void andAndOrShortCircuitBeforeMissingOrIncompatibleLaterOperands() throws Exception {
        for (String logical : List.of("and", "or")) {
            Node node = branch("eq", "same", "or".equals(logical) ? "same" : "different");
            List<InputItem> inputs = new ArrayList<>(node.getData().getInputs());
            inputs.add(new InputItem("missing-id", "missing-name", new InputSchema("string",
                    new Value("ref", Map.of("nodeId", "future", "name", "value")))));
            node.getData().setInputs(inputs);
            node.getData().getNodeParam().put("cases", List.of(Map.of("id", "branch_one_of::yes", "logicalOperator", logical,
                    "conditions", List.of(predicate("eq", "left-id", "right-id"), predicate("gt", "missing-id", "right-id"))), fallback()));

            NodeActivation result = execute(node, new VariablePool());

            assertEquals(NodeActivation.Status.SUCCEEDED, result.status());
            assertEquals("or".equals(logical) ? "branch_one_of::yes" : "branch_one_of::default", result.result().getSelectedHandle());
        }
    }

    @Test
    void firstMatchingCaseWinsByArrayOrderEvenWhenLevelsSuggestOtherwise() throws Exception {
        Node node = branch("eq", 1, 1);
        node.getData().getNodeParam().put("cases", List.of(
                Map.of("id", "first", "level", 20, "logicalOperator", "and", "conditions", List.of(predicate("eq", "left-id", "right-id"))),
                Map.of("id", "second", "level", 1, "logicalOperator", "and", "conditions", List.of(predicate("eq", "left-id", "right-id"))), fallback()));

        assertEquals("first", execute(node, new VariablePool()).result().getSelectedHandle());
    }

    @Test
    void falseConditionIsSuccessfulAndDoesNotConsumeRetryAllowance() throws Exception {
        Node node = branch("eq", 1, 2);
        RetryConfig retry = new RetryConfig();
        retry.setShouldRetry(true);
        retry.setMaxRetries(2);
        retry.setErrorStrategy(0);
        node.getData().setRetryConfig(retry);

        NodeActivation result = execute(node, new VariablePool());

        assertEquals(NodeActivation.Status.SUCCEEDED, result.status());
        assertEquals(1, result.attempts().size());
        assertEquals("branch_one_of::default", result.result().getSelectedHandle());
    }

    @Test
    void configurationValidationRejectsUnknownOperatorsMissingDefaultsAndDanglingInputIds() throws Exception {
        Node unknown = branch("eq", 1, 1);
        unknown.getData().getNodeParam().put("cases", List.of(Map.of("id", "bad", "logicalOperator", "and",
                "conditions", List.of(predicate("unknown", "left-id", "right-id"))), fallback()));
        Node defaultMissing = branch("eq", 1, 1);
        defaultMissing.getData().getNodeParam().put("cases", List.of(Map.of("id", "bad", "logicalOperator", "and",
                "conditions", List.of(predicate("eq", "left-id", "right-id")))));
        Node dangling = branch("eq", 1, 1);
        dangling.getData().getInputs().clear();
        for (Node invalid : List.of(unknown, defaultMissing, dangling)) {
            NodeCustomException error = assertThrows(NodeCustomException.class, () -> executor.validateConfiguration(invalid));
            assertEquals(ErrorCode.INVALID_NODE_CONFIGURATION.getCode(), error.getCode());
        }
    }

    private Node branch(String operator, Object left, Object right) throws Exception {
        Node node = LegacyFixtureSupport.load("branch-cycle").findNodeById("if-else::check");
        node.getData().setInputs(new ArrayList<>(List.of(
                new InputItem("left-id", "left-name", new InputSchema("object", new Value("literal", left))),
                new InputItem("right-id", "right-name", new InputSchema("object", new Value("literal", right))))));
        node.getData().getNodeParam().put("cases", List.of(Map.of("id", "branch_one_of::yes", "logicalOperator", "and",
                "conditions", List.of(predicate(operator, "left-id", "right-id"))), fallback()));
        return node;
    }

    private Map<String, Object> predicate(String operator, String left, String right) {
        return Map.of("compareOperator", operator, "leftVarIndex", left, "rightVarIndex", right);
    }

    private Map<String, Object> fallback() {
        return Map.of("id", "branch_one_of::default", "conditions", List.of());
    }

    private NodeActivation execute(Node node, VariablePool variables) {
        WorkflowRunContext run = new WorkflowRunContext("condition", "flow", "chat", variables, callback);
        return new NodeExecutionBridge(List.of(executor)).execute(run, node);
    }
}
