package com.iflytek.astron.workflow.engine.execution;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.MsgTypeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeStatusEnum;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder;
import com.iflytek.astron.workflow.engine.context.EngineContextHolder.EngineContext;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.NodeData;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.execution.NodeActivation.AttemptStatus;
import com.iflytek.astron.workflow.engine.execution.NodeActivation.Status;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatItem;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatMessage;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.AsyncUtil;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

@Slf4j
public final class NodeExecutionScope {
    private final WorkflowRunContext run;
    private final Node node;
    @Getter
    private final String executionId;
    @Getter
    private final String activationId;
    private final int activationNo;
    private final Map<String, Object> inputs;
    private final Map<String, NodeActivation.InputOrigin> inputOrigins;
    private final Map<String, Map<String, Object>> variables;
    private final List<ChatItem> initialHistory;
    private final Clock clock;
    private final NodeExecutionScope root;
    private final AttemptState attempt;
    private final List<AttemptState> attempts = new ArrayList<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private volatile Status status = Status.READY;
    private volatile AttemptState currentAttempt;
    private NodeRunResult result;
    private List<ChatItem> history;

    NodeExecutionScope(WorkflowRunContext run, Node node, String activationId, int activationNo,
                       Map<String, Object> inputs, Map<String, NodeActivation.InputOrigin> inputOrigins,
                       Map<String, Map<String, Object>> variables, Clock clock) {
        this.run = run;
        this.node = node;
        this.executionId = UUID.randomUUID().toString();
        this.activationId = activationId;
        this.activationNo = activationNo;
        this.inputs = JsonSnapshot.copy(inputs);
        this.inputOrigins = Map.copyOf(inputOrigins);
        this.variables = variables;
        this.initialHistory = run.history(node.getId());
        this.clock = clock;
        this.root = this;
        this.attempt = null;
    }

    private NodeExecutionScope(NodeExecutionScope root, Node node, AttemptState attempt) {
        this.run = root.run;
        this.node = node;
        this.executionId = root.executionId;
        this.activationId = root.activationId;
        this.activationNo = root.activationNo;
        this.inputs = root.inputs;
        this.inputOrigins = root.inputOrigins;
        this.variables = root.variables;
        this.initialHistory = root.initialHistory;
        this.clock = root.clock;
        this.root = root;
        this.attempt = attempt;
    }

    public Map<String, Object> inputs() {
        return JsonSnapshot.mutable(inputs);
    }

    public boolean isActive() {
        return run.executionAllowed() && !root.cancelled.get() && root.status == Status.RUNNING && (attempt == null || attempt.status == AttemptStatus.RUNNING
                && clock.millis() < attempt.deadline);
    }

    public boolean inputPresent(String name) {
        NodeActivation.InputOrigin origin = inputOrigins.get(name);
        return origin == null ? inputs.containsKey(name) : origin.present();
    }

    public NodeRunResult invoke(NodeState state, RetryConfig retry, Invocation invocation,
                                BiFunction<NodeState, NodeCustomException, NodeRunResult> failure) {
        AttemptState current;
        NodeExecutionScope child;
        root.publishTerminals();
        synchronized (run) {
            if (!isActive()) {
                return interrupted();
            }
            long startedAt = clock.millis();
            long timeout = retry != null && retry.timeOutEnabled() ? retry.toMillis() : Long.MAX_VALUE;
            long deadline = timeout > Long.MAX_VALUE - startedAt ? Long.MAX_VALUE : startedAt + timeout;
            current = new AttemptState(root.attempts.size() + 1, startedAt, deadline, initialHistory);
            root.attempts.add(current);
            root.currentAttempt = current;
            child = new NodeExecutionScope(root, copyNode(node), current);
            child.node.getExecutedCount().set(current.attemptNo);
        }
        VariablePool privatePool = pool();
        WorkflowMsgCallback guarded = run.callback().guarded(child::gate, child::deferTerminal, child::isActive);
        NodeState call = new NodeState(child.node, privatePool, guarded, child);
        NodeRunResult returned;
        boolean timedOut = false;
        try {
            if (retry != null && retry.timeOutEnabled()) {
                returned = AsyncUtil.callWithTimeLimit(retry.toMillis(), TimeUnit.MILLISECONDS,
                        () -> child.withContext(call, invocation));
            } else {
                returned = child.withContext(call, invocation);
            }
            returned = JsonSnapshot.result(Objects.requireNonNull(returned, "Node executor returned no result"));
            synchronized (run) {
                if (clock.millis() >= current.deadline) {
                    throw new TimeoutException("Node attempt deadline exceeded");
                }
                current.accepting = false;
            }
        } catch (Exception error) {
            timedOut = error instanceof TimeoutException || error instanceof InterruptedException;
            synchronized (run) {
                current.accepting = false;
                current.terminals.clear();
            }
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Throwable cause = error instanceof ExecutionException && error.getCause() != null ? error.getCause() : error;
            NodeCustomException business = timedOut ? new NodeCustomException(ErrorCode.TIMEOUT_ERROR)
                    : cause instanceof NodeCustomException custom ? custom
                    : new NodeCustomException(ErrorCode.NODE_RUN_ERROR, cause.getMessage());
            WorkflowMsgCallback control = run.callback().guarded(action -> controlGate(current, action),
                    action -> deferControlTerminal(current, action),
                    () -> controlActive(current));
            returned = failure.apply(new NodeState(copyNode(node), pool(), control, child), business);
        }
        synchronized (run) {
            if (!run.executionAllowed() || root.cancelled.get() || root.status != Status.RUNNING || current.status == AttemptStatus.CANCELLED) {
                return interrupted();
            }
            NodeRunResult fixed = JsonSnapshot.result(returned);
            fixed.setInputs(inputs);
            if (fixed.getStatus() == null) {
                fixed.setStatus(NodeExecStatusEnum.SUCCESS);
            }
            current.status = timedOut ? AttemptStatus.TIMED_OUT
                    : fixed.getStatus().isSuccess() ? AttemptStatus.SUCCEEDED : AttemptStatus.FAILED;
            current.finishedAt = clock.millis();
            current.errorCode = fixed.getError() == null ? null : fixed.getError().getCode();
            current.error = fixed.getError() == null ? null : fixed.getError().getMessage();
            current.control = false;
            root.history = current.historyTouched ? LlmChatHistory.copyHistory(current.history) : null;
            return fixed;
        }
    }

    public void newChat(String chatId, String nodeId) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            if (attempt.history.size() >= 10) {
                attempt.history.removeFirst();
            }
            attempt.history.add(new ChatItem(chatId, nodeId));
            attempt.historyTouched = true;
        });
    }

    public void addMessage(String chatId, String nodeId, MsgTypeEnum role, String content) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            if (attempt.history.isEmpty()) {
                attempt.history.add(new ChatItem(chatId, nodeId));
            }
            ChatItem item = attempt.history.getLast();
            ChatMessage message = new ChatMessage(role, content);
            switch (role) {
                case USER, SYSTEM -> item.userInputs().add(message);
                case THINKING -> item.llmThinking().add(message);
                case ASSISTANT -> item.llmResponses().add(message);
            }
            attempt.historyTouched = true;
        });
    }

    public List<ChatItem> getHistory(String chatId, String nodeId, int count) {
        synchronized (run) {
            requireHistoryIdentity(chatId, nodeId);
            List<ChatItem> items = attempt == null ? initialHistory : attempt.history;
            return LlmChatHistory.copyHistory(items.subList(Math.max(0, items.size() - count), items.size()));
        }
    }

    public void clearHistory(String chatId, String nodeId) {
        mutate(() -> {
            requireHistoryIdentity(chatId, nodeId);
            attempt.history.clear();
            attempt.historyTouched = true;
        });
    }

    NodeState state() {
        return new NodeState(node, pool(), run.callback().guarded(this::gate, this::isActive), this);
    }

    Node node() {
        return node;
    }

    Status status() {
        return root.status;
    }

    void start() {
        status = Status.RUNNING;
        node.setStatus(NodeStatusEnum.RUNNING);
    }

    void cancel() {
        if (status != Status.READY && status != Status.RUNNING) {
            return;
        }
        cancelled.set(true);
        status = Status.CANCELLED;
        for (AttemptState current : attempts) {
            current.accepting = false;
            current.control = false;
            current.terminals.clear();
            if (current.status == AttemptStatus.RUNNING) {
                current.status = AttemptStatus.CANCELLED;
                current.finishedAt = clock.millis();
            }
        }
    }

    void publishTerminals() {
        List<Runnable> terminal;
        synchronized (run) {
            if (attempts.isEmpty() || cancelled.get() || !run.executionAllowed()) {
                return;
            }
            terminal = new ArrayList<>(attempts.getLast().terminals);
            attempts.getLast().terminals.clear();
        }
        for (Runnable action : terminal) {
            if (cancelled.get() || !run.executionAllowed()) {
                break;
            }
            try {
                action.run();
            } catch (RuntimeException error) {
                log.warn("Cannot deliver committed node event for execution {}", executionId, error);
            }
        }
    }

    NodeRunResult complete(NodeRunResult returned) {
        result = JsonSnapshot.result(returned);
        result.setInputs(inputs);
        status = result.getStatus() == null || result.getStatus().isSuccess() ? Status.SUCCEEDED : Status.FAILED;
        node.setStatus(status == Status.SUCCEEDED ? NodeStatusEnum.SUCCESS : NodeStatusEnum.ERROR);
        return result;
    }

    void commit(NodeRunResult returned) {
        run.complete(root, returned, history);
        publishTerminals();
    }

    NodeActivation snapshot() {
        return new NodeActivation(run.getRunId(), executionId, activationId, node.getId(), activationNo, root.status,
                inputs, inputOrigins, result, attempts.stream().map(AttemptState::snapshot).toList());
    }

    static Node copyNode(Node source) {
        Node copy = new Node();
        copy.setId(source.getId());
        copy.setData(JSON.parseObject(JSON.toJSONString(source.getData()), NodeData.class));
        copy.init();
        return copy;
    }

    private VariablePool pool() {
        VariablePool pool = new VariablePool();
        pool.restore(variables);
        return pool;
    }

    private NodeRunResult withContext(NodeState state, Invocation invocation) throws Exception {
        if (!isActive()) {
            return interrupted();
        }
        EngineContext previous = EngineContextHolder.get();
        EngineContext context = new EngineContext();
        context.setFlowId(run.getFlowId());
        context.setChatId(run.getChatId());
        context.setSid(run.getSid());
        context.setCallback(state.callback());
        context.setIdempotencyKey(run.getRunId() + ":" + executionId);
        context.setNodeExecution(this);
        EngineContextHolder.set(context);
        run.enteredPhysicalAttempt();
        try {
            return invocation.call(state);
        } finally {
            run.exitedPhysicalAttempt();
            if (previous == null) {
                EngineContextHolder.remove();
            } else {
                EngineContextHolder.set(previous);
            }
        }
    }

    NodeRunResult failedInvocation(NodeCustomException error) {
        synchronized (run) {
            if (!attempts.isEmpty()) {
                AttemptState current = attempts.getLast();
                if (current.status == AttemptStatus.RUNNING) {
                    current.status = AttemptStatus.FAILED;
                    current.finishedAt = clock.millis();
                    current.errorCode = error.getCode();
                    current.error = error.getMessage();
                    current.accepting = false;
                    current.control = false;
                    current.terminals.clear();
                }
            }
            NodeRunResult result = interrupted();
            result.setError(error);
            return result;
        }
    }

    private void gate(Runnable action) {
        if (isActive() && (attempt == null || attempt.accepting)) {
            action.run();
        }
    }

    private void mutate(Runnable action) {
        synchronized (run) {
            gate(action);
        }
    }

    private void deferTerminal(Runnable action) {
        mutate(() -> attempt.terminals.add(action));
    }

    private boolean controlActive(AttemptState current) {
        return run.executionAllowed() && !root.cancelled.get() && root.status == Status.RUNNING && current.control && root.currentAttempt == current;
    }

    private void controlGate(AttemptState current, Runnable action) {
        if (controlActive(current)) {
            action.run();
        }
    }

    private void deferControlTerminal(AttemptState current, Runnable action) {
        synchronized (run) {
            if (controlActive(current)) {
                current.terminals.add(action);
            }
        }
    }

    private void requireHistoryIdentity(String chatId, String nodeId) {
        if (!run.getChatId().equals(chatId) || !node.getId().equals(nodeId)) {
            throw new IllegalArgumentException("History identity differs from node execution");
        }
    }

    private NodeRunResult interrupted() {
        NodeRunResult result = new NodeRunResult();
        result.setInputs(inputs);
        result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
        result.setError(new NodeCustomException(ErrorCode.INTERRUPTED_ERROR));
        return result;
    }

    @FunctionalInterface
    public interface Invocation {
        NodeRunResult call(NodeState state) throws Exception;
    }

    private static final class AttemptState {
        private final String attemptId = UUID.randomUUID().toString();
        private final int attemptNo;
        private final long startedAt;
        private final long deadline;
        private final List<ChatItem> history;
        private final List<Runnable> terminals = new ArrayList<>();
        private volatile AttemptStatus status = AttemptStatus.RUNNING;
        private volatile boolean accepting = true;
        private volatile boolean control = true;
        private boolean historyTouched;
        private long finishedAt;
        private Integer errorCode;
        private String error;

        private AttemptState(int attemptNo, long startedAt, long deadline, List<ChatItem> history) {
            this.attemptNo = attemptNo;
            this.startedAt = startedAt;
            this.deadline = deadline;
            this.history = LlmChatHistory.copyHistory(history);
        }

        private NodeActivation.Attempt snapshot() {
            return new NodeActivation.Attempt(attemptId, attemptNo, status, startedAt, deadline, finishedAt, errorCode, error);
        }
    }
}
