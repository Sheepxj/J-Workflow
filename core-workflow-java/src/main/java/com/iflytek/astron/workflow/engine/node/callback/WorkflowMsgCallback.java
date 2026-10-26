package com.iflytek.astron.workflow.engine.node.callback;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.callbacks.ChatCallBackStreamResult;
import com.iflytek.astron.workflow.engine.domain.callbacks.ChatCallBacks;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.node.StreamCallback;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

@Slf4j
public class WorkflowMsgCallback implements StreamCallback {

    private final ChatCallBacks chatCallBacks;
    private final StreamCallback clientCallback;
    private final BlockingQueue<LLMGenerate> streamQueue;
    private final AtomicBoolean finishing = new AtomicBoolean();
    private final AtomicBoolean aborted = new AtomicBoolean();
    private final AtomicBoolean consumerRunning = new AtomicBoolean();
    private final CompletableFuture<Void> consumerStopped = new CompletableFuture<>();
    private final Future<?> consumerTask;
    private final ThreadLocal<BooleanSupplier> producerGuard = new ThreadLocal<>();

    private WorkflowMsgCallback() {
        chatCallBacks = null;
        clientCallback = null;
        streamQueue = null;
        consumerTask = null;
    }

    public WorkflowMsgCallback guarded(Consumer<Runnable> gate, BooleanSupplier active) {
        return guarded(gate, gate, active);
    }

    public WorkflowMsgCallback guarded(Consumer<Runnable> gate, Consumer<Runnable> terminalGate, BooleanSupplier active) {
        WorkflowMsgCallback target = this;
        return new WorkflowMsgCallback() {
            @Override
            public void onWorkflowStart() {
            }

            @Override
            public void onWorkflowEnd(NodeRunResult message) {
            }

            @Override
            public void onNodeStart(int code, String nodeId, String aliasName) {
                gate.accept(() -> target.produce(active, () -> target.onNodeStart(code, nodeId, aliasName)));
            }

            @Override
            public void onNodeProcess(int code, String nodeId, String aliasName, String message, String reasoningContent) {
                gate.accept(() -> target.produce(active, () -> target.onNodeProcess(code, nodeId, aliasName, message, reasoningContent)));
            }

            @Override
            public void onNodeInterrupt(String eventId, Map<String, Object> value, String nodeId, String aliasName,
                                        int code, String finishReason, boolean needReply) {
                terminalGate.accept(() -> target.onNodeInterrupt(eventId, value, nodeId, aliasName, code, finishReason, needReply));
            }

            @Override
            public void onNodeEnd(String nodeId, String aliasName, NodeRunResult message) {
                terminalGate.accept(() -> target.onNodeEnd(nodeId, aliasName, message));
            }

            @Override
            public void onStartNodeExecuted(String nodeId, String aliasName, NodeRunResult message) {
                terminalGate.accept(() -> target.onStartNodeExecuted(nodeId, aliasName, message));
            }

            @Override
            public void onEndNodeExecuted(String nodeId, String aliasName, NodeRunResult message) {
                terminalGate.accept(() -> target.onEndNodeExecuted(nodeId, aliasName, message));
            }

            @Override
            public void callback(String eventType, Object data) {
                gate.accept(() -> target.produce(active, () -> target.callback(eventType, data)));
            }

            @Override
            public void finished() {
            }

            @Override
            public void abort() {
            }

            @Override
            public boolean isActive() {
                return active.getAsBoolean() && target.isActive();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return !active.getAsBoolean();
            }
        };
    }

    public WorkflowMsgCallback(String sid,
                               StreamCallback clientCallback,
                               EndNodeOutputModeEnum endNodeOutputMode,
                               BlockingQueue<LLMGenerate> streamQueue,
                               Queue<ChatCallBackStreamResult> needOrderStreamResultQ,
                               WorkflowStreamVirtualThreadExecutor executor) {
        this.clientCallback = clientCallback;
        this.streamQueue = streamQueue;
        this.chatCallBacks = new ChatCallBacks(
                sid,
                streamQueue,
                endNodeOutputMode,
                Set.of(),
                needOrderStreamResultQ,
                this::canProduce
        );
        this.consumerTask = executor.submit(this::consumeStream);
    }

    public void onWorkflowStart() {
        chatCallBacks.onSparkflowStart();
    }

    public void onWorkflowEnd(NodeRunResult message) {
        chatCallBacks.onSparkflowEnd(message);
    }

    public void onNodeStart(int code, String nodeId, String aliasName) {
        chatCallBacks.onNodeStart(code, nodeId, aliasName);
    }

    public void onNodeProcess(int code, String nodeId, String aliasName,
                              String message, String reasoningContent) {
        chatCallBacks.onNodeProcess(code, nodeId, aliasName, message, reasoningContent);
    }

    public void onNodeInterrupt(String eventId, Map<String, Object> value,
                                String nodeId, String aliasName, int code,
                                String finishReason, boolean needReply) {
        chatCallBacks.onNodeInterrupt(eventId, value, nodeId, aliasName, code, finishReason, needReply);
    }

    public void onNodeEnd(String nodeId, String aliasName, NodeRunResult message) {
        chatCallBacks.onNodeEnd(nodeId, aliasName, message, message.getError());
    }

    @Override
    public void callback(String eventType, Object data) {
        clientCallback.callback(eventType, data);
    }

    @Override
    public void finished() {
        if (!finishing.compareAndSet(false, true)) {
            awaitConsumer();
            return;
        }
        awaitConsumer();
        if (!aborted.get() && clientCallback.isActive()) {
            while (!chatCallBacks.getOrderStreamResultQ().isEmpty()) {
                ChatCallBackStreamResult response = chatCallBacks.getOrderStreamResultQ().poll();
                if (response != null) {
                    clientCallback.callback("stream", response.getNodeAnswerContent());
                }
            }
            clientCallback.finished();
        }
    }

    @Override
    public void abort() {
        if (!aborted.compareAndSet(false, true)) {
            return;
        }
        finishing.set(true);
        streamQueue.clear();
        consumerTask.cancel(true);
        if (!consumerRunning.get()) {
            consumerStopped.complete(null);
        }
        clientCallback.abort();
    }

    @Override
    public boolean isActive() {
        return !aborted.get() && clientCallback.isActive() && !consumerStopped.isDone();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            consumerStopped.get(timeout, unit);
            return true;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    public void onEndNodeExecuted(String nodeId, String aliasName, NodeRunResult message) {
        message.setNodeAnswerContent((String) message.getOutputs().getOrDefault("content", ""));
        message.setNodeAnswerReasoningContent((String) message.getOutputs().getOrDefault("reasoning_content", ""));
        message.setOutputs(message.getInputs());
        message.setInputs(Map.of());
        onNodeEnd(nodeId, aliasName, message);
    }

    public void onStartNodeExecuted(String nodeId, String aliasName, NodeRunResult message) {
        message.setOutputs(Map.of());
        onNodeEnd(nodeId, aliasName, message);
    }

    private boolean canProduce() {
        BooleanSupplier guard = producerGuard.get();
        return !finishing.get() && !aborted.get() && clientCallback.isActive() && (guard == null || guard.getAsBoolean());
    }

    private void produce(BooleanSupplier active, Runnable action) {
        BooleanSupplier previous = producerGuard.get();
        producerGuard.set(active);
        try {
            action.run();
        } finally {
            if (previous == null) {
                producerGuard.remove();
            } else {
                producerGuard.set(previous);
            }
        }
    }

    private void consumeStream() {
        consumerRunning.set(true);
        try {
            while (!aborted.get() && (!finishing.get() || !streamQueue.isEmpty())) {
                if (!clientCallback.isActive()) {
                    aborted.set(true);
                    streamQueue.clear();
                    return;
                }
                LLMGenerate response = streamQueue.poll(100, TimeUnit.MILLISECONDS);
                if (response != null) {
                    clientCallback.callback("stream", response);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("Error in workflow stream callback", e);
            aborted.set(true);
            clientCallback.abort();
        } finally {
            consumerRunning.set(false);
            consumerStopped.complete(null);
        }
    }

    private void awaitConsumer() {
        try {
            consumerTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abort();
        } catch (ExecutionException e) {
            log.error("Workflow stream consumer failed", e.getCause());
            abort();
        } catch (CancellationException ignored) {
        }
    }
}
