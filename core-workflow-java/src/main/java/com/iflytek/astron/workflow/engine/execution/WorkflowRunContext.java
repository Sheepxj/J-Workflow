package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatItem;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.FlowUtil;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Clock;

public final class WorkflowRunContext {
    @Getter
    private final String runId;
    @Getter
    private final String flowId;
    @Getter
    private final String chatId;
    @Getter
    private final String sid = FlowUtil.genSid();
    private final VariablePool variables = new VariablePool();
    private final WorkflowMsgCallback callback;
    private final Map<String, NodeExecutionScope> executions = new ConcurrentHashMap<>();
    private final List<NodeExecutionScope> executionOrder = new ArrayList<>();
    private final Map<String, String> activations = new LinkedHashMap<>();
    private final Map<String, Integer> activationCounts = new LinkedHashMap<>();
    private final Map<String, String> lastExecutions = new LinkedHashMap<>();
    private final Map<String, List<ChatItem>> histories;
    private String runningExecution;
    private final AtomicInteger physicalAttempts = new AtomicInteger();
    private volatile long executionDeadline = Long.MAX_VALUE;
    private volatile Clock executionClock = Clock.systemUTC();

    public WorkflowRunContext(String runId, String flowId, String chatId, VariablePool initialVariables,
                              WorkflowMsgCallback callback) {
        this.runId = Objects.requireNonNull(runId);
        this.flowId = Objects.requireNonNull(flowId);
        this.chatId = Objects.requireNonNull(chatId);
        this.callback = Objects.requireNonNull(callback);
        variables.restore(initialVariables.snapshot());
        histories = new LinkedHashMap<>(LlmChatHistory.snapshot(chatId));
    }

    public synchronized Map<String, Map<String, Object>> variables() {
        return variables.snapshot();
    }

    public synchronized List<NodeActivation> executions() {
        return executionOrder.stream().map(NodeExecutionScope::snapshot).toList();
    }

    public synchronized NodeActivation execution(String executionId) {
        return scope(executionId).snapshot();
    }

    public synchronized Map<String, List<ChatItem>> histories() {
        return LlmChatHistory.copyHistories(histories);
    }

    public int physicalAttempts() {
        return physicalAttempts.get();
    }

    public synchronized int activationCount() {
        return executions.size();
    }

    public synchronized int activationCount(String nodeId) {
        return activationCounts.getOrDefault(nodeId, 0);
    }

    public synchronized boolean hasPendingExecutions() {
        return executionOrder.stream().anyMatch(scope -> scope.status() == NodeActivation.Status.READY
                || scope.status() == NodeActivation.Status.RUNNING);
    }

    void setExecutionDeadline(long deadline, Clock clock) {
        executionClock = Objects.requireNonNull(clock);
        executionDeadline = deadline;
    }

    boolean executionAllowed() {
        return executionClock.millis() < executionDeadline;
    }

    public synchronized void cancel(String executionId) {
        NodeExecutionScope scope = scope(executionId);
        scope.cancel();
        if (executionId.equals(runningExecution)) {
            runningExecution = null;
        }
    }

    synchronized NodeExecutionScope activation(String activationId) {
        String executionId = activations.get(activationId);
        return executionId == null ? null : scope(executionId);
    }

    synchronized int nextActivation(String nodeId) {
        return activationCounts.merge(nodeId, 1, Integer::sum);
    }

    synchronized void register(NodeExecutionScope scope) {
        executions.put(scope.getExecutionId(), scope);
        executionOrder.add(scope);
        activations.put(scope.getActivationId(), scope.getExecutionId());
    }

    synchronized NodeExecutionScope scope(String executionId) {
        NodeExecutionScope scope = executions.get(executionId);
        if (scope == null) {
            throw new IllegalArgumentException("Unknown node execution: " + executionId);
        }
        return scope;
    }

    synchronized boolean claim(NodeExecutionScope scope) {
        if (scope.status() != NodeActivation.Status.READY) {
            return false;
        }
        if (runningExecution != null) {
            throw new IllegalStateException("Run already has an active node execution");
        }
        runningExecution = scope.getExecutionId();
        scope.start();
        return true;
    }

    synchronized void complete(NodeExecutionScope scope, NodeRunResult result, List<ChatItem> history) {
        if (!scope.isActive()) {
            return;
        }
        NodeRunResult committed = scope.complete(result);
        if (committed.getStatus() == null || committed.getStatus().isSuccess()
                || committed.getStatus() == NodeExecStatusEnum.ERR_CODE_MSG) {
            variables.replaceNodeOutputs(scope.node().getId(), committed.getOutputs());
            lastExecutions.put(scope.node().getId(), scope.getExecutionId());
        }
        if (history != null) {
            histories.put(scope.node().getId(), LlmChatHistory.copyHistory(history));
            LlmChatHistory.restore(getChatId(), scope.node().getId(), history);
        }
        runningExecution = null;
    }

    synchronized List<ChatItem> history(String nodeId) {
        return LlmChatHistory.copyHistory(histories.getOrDefault(nodeId, List.of()));
    }

    synchronized String lastExecution(String nodeId) {
        return lastExecutions.get(nodeId);
    }

    WorkflowMsgCallback callback() {
        return callback;
    }

    void enteredPhysicalAttempt() {
        physicalAttempts.incrementAndGet();
    }

    void exitedPhysicalAttempt() {
        physicalAttempts.decrementAndGet();
    }
}
