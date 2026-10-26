package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.Getter;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class WorkflowGraphRun {
    @Getter
    private final WorkflowRunContext context;
    @Getter
    private final WorkflowExecutionLimits limits;
    @Getter
    private final long startedAt;
    @Getter
    private final long deadline;
    private final WorkflowGraph graph;
    private final ArrayDeque<Token> pending = new ArrayDeque<>();
    private final Map<String, Token> issued = new HashMap<>();
    private final Set<String> consumed = new HashSet<>();
    private volatile Status status = Status.RUNNING;
    private Map<String, Object> outputs = Map.of();
    private Integer errorCode;
    private String error;
    private boolean advancing;
    private boolean endReached;

    WorkflowGraphRun(WorkflowGraph graph, WorkflowRunContext context, WorkflowExecutionLimits limits, long startedAt, long deadline) {
        this.graph = graph;
        this.context = context;
        this.limits = limits;
        this.startedAt = startedAt;
        this.deadline = deadline;
        issue(graph.startNodeId(), null, null);
    }

    public Status status() {
        return status;
    }

    public synchronized Map<String, Object> outputs() {
        return outputs;
    }

    public synchronized Integer errorCode() {
        return errorCode;
    }

    public synchronized String error() {
        return error;
    }

    public synchronized List<Token> pending() {
        return List.copyOf(pending);
    }

    public synchronized Set<String> consumedTokenIds() {
        return Set.copyOf(consumed);
    }

    public synchronized void replay(Token token) {
        if (token == null || !token.equals(issued.get(token.id()))) {
            throw new NodeCustomException(ErrorCode.WORKFLOW_ROUTING_ERROR, "Only an issued control token can be replayed");
        }
        if (status == Status.RUNNING) {
            pending.addLast(token);
        }
    }

    WorkflowGraph graph() {
        return graph;
    }

    synchronized boolean beginAdvance() {
        if (status != Status.RUNNING || advancing) {
            return false;
        }
        advancing = true;
        return true;
    }

    synchronized void endAdvance() {
        advancing = false;
    }

    synchronized Token next() {
        while (!pending.isEmpty()) {
            Token next = pending.removeFirst();
            if (consumed.add(next.id())) {
                return next;
            }
        }
        return null;
    }

    synchronized void issue(String nodeId, String sourceExecutionId, Integer edgeIndex) {
        Token token = new Token(UUID.randomUUID().toString(), nodeId, sourceExecutionId, edgeIndex);
        issued.put(token.id(), token);
        pending.addLast(token);
    }

    synchronized void reachedEnd(NodeRunResult result) {
        endReached = true;
        outputs = JsonSnapshot.copy(result.getOutputs());
    }

    synchronized void finishIfIdle(boolean hasPendingExecutions, int physicalAttempts) {
        pending.removeIf(token -> consumed.contains(token.id()));
        if (status != Status.RUNNING || !pending.isEmpty() || hasPendingExecutions || physicalAttempts != 0) {
            return;
        }
        if (endReached) {
            status = Status.SUCCEEDED;
        } else {
            fail(new NodeCustomException(ErrorCode.WORKFLOW_CONTROL_FLOW_STALLED, "No runnable work remains before END"));
        }
    }

    synchronized void fail(NodeCustomException failure) {
        if (status != Status.RUNNING) {
            return;
        }
        status = Status.FAILED;
        errorCode = failure.getCode();
        error = failure.getMessage();
        pending.clear();
    }

    public enum Status {
        RUNNING, SUCCEEDED, FAILED
    }

    public record Token(String id, String targetNodeId, String sourceExecutionId, Integer edgeIndex) {
    }
}
