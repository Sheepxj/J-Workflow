package com.iflytek.astron.workflow.engine.node.callback;

import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.node.StreamCallback;
import com.iflytek.astron.workflow.engine.util.SseVirtualThreadExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class SseStreamCallback implements StreamCallback {

    private static final int EVENT_QUEUE_CAPACITY = 1024;
    private static final long ENQUEUE_RECHECK_MILLIS = 100;

    private final SseEmitter emitter;
    private final SseVirtualThreadExecutor executor;
    private final BlockingQueue<SseCommand> commands;
    private final ReentrantLock enqueueLock = new ReentrantLock();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean aborted = new AtomicBoolean();
    private final AtomicBoolean writerRunning = new AtomicBoolean();
    private final AtomicBoolean lifecycleClosed = new AtomicBoolean();
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private final Future<?> writerTask;

    public SseStreamCallback(SseEmitter emitter, SseVirtualThreadExecutor executor) {
        this(emitter, executor, EVENT_QUEUE_CAPACITY);
    }

    SseStreamCallback(SseEmitter emitter, SseVirtualThreadExecutor executor, int queueCapacity) {
        this.emitter = emitter;
        this.executor = executor;
        this.commands = new ArrayBlockingQueue<>(queueCapacity);
        executor.connectionOpened();
        try {
            this.writerTask = executor.submit(this::writeLoop);
        } catch (RuntimeException e) {
            closeLifecycle();
            throw e;
        }
        emitter.onCompletion(this::abort);
        emitter.onTimeout(this::abort);
        emitter.onError(error -> abort());
    }

    @Override
    public void callback(String eventType, Object data) {
        enqueue(SseCommand.send(eventType, data));
    }

    public void complete() {
        enqueueTerminal(SseCommand.complete());
    }

    public void completeWithError(Throwable error) {
        enqueueTerminal(SseCommand.error(error));
    }

    @Override
    public void abort() {
        if (!aborted.compareAndSet(false, true)) {
            return;
        }
        accepting.set(false);
        commands.clear();
        writerTask.cancel(true);
        if (!writerRunning.get()) {
            stopped.complete(null);
            closeLifecycle();
        }
    }

    @Override
    public boolean isActive() {
        return accepting.get() && !aborted.get() && !stopped.isDone();
    }

    @Override
    public void finished() {
        CompletableFuture<Void> flushed = new CompletableFuture<>();
        if (!enqueue(SseCommand.flush(flushed))) {
            return;
        }
        try {
            CompletableFuture.anyOf(flushed, stopped).join();
        } catch (CompletionException e) {
            log.warn("Failed while waiting for SSE events to flush", e);
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            stopped.get(timeout, unit);
            return true;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    private boolean enqueue(SseCommand command) {
        enqueueLock.lock();
        try {
            if (!isActive()) {
                return false;
            }
            return offerWithBackpressure(command, true);
        } finally {
            enqueueLock.unlock();
        }
    }

    private void enqueueTerminal(SseCommand command) {
        enqueueLock.lock();
        try {
            if (!accepting.compareAndSet(true, false) || aborted.get() || stopped.isDone()) {
                return;
            }
            if (!offerWithBackpressure(command, false)) {
                abort();
            }
        } finally {
            enqueueLock.unlock();
        }
    }

    private boolean offerWithBackpressure(SseCommand command, boolean requireAccepting) {
        if (commands.offer(command)) {
            return true;
        }
        executor.backpressure();
        try {
            while (!aborted.get() && !stopped.isDone() && (!requireAccepting || accepting.get())) {
                if (commands.offer(command, ENQUEUE_RECHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    private void writeLoop() {
        writerRunning.set(true);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                SseCommand command = commands.take();
                switch (command.type()) {
                    case SEND -> send(command);
                    case FLUSH -> command.flushed().complete(null);
                    case COMPLETE -> {
                        emitter.complete();
                        return;
                    }
                    case ERROR -> {
                        emitter.completeWithError(command.error());
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            if (!aborted.get()) {
                log.error("Failed to send SSE event: {}", e.getMessage(), e);
                emitter.completeWithError(e);
            }
        } finally {
            writerRunning.set(false);
            accepting.set(false);
            stopped.complete(null);
            closeLifecycle();
        }
    }

    private void closeLifecycle() {
        if (lifecycleClosed.compareAndSet(false, true)) {
            executor.connectionClosed();
        }
    }

    private void send(SseCommand command) throws IOException {
        String jsonData = JSON.toJSONString(command.data());
        emitter.send(SseEmitter.event().name(command.eventType()).data(jsonData));
        if (log.isDebugEnabled()) {
            log.debug("Response data to client: {}", jsonData);
        }
    }

    private enum CommandType {
        SEND,
        FLUSH,
        COMPLETE,
        ERROR
    }

    private record SseCommand(
            CommandType type,
            String eventType,
            Object data,
            Throwable error,
            CompletableFuture<Void> flushed) {

        private static SseCommand send(String eventType, Object data) {
            return new SseCommand(CommandType.SEND, eventType, data, null, null);
        }

        private static SseCommand flush(CompletableFuture<Void> flushed) {
            return new SseCommand(CommandType.FLUSH, null, null, null, flushed);
        }

        private static SseCommand complete() {
            return new SseCommand(CommandType.COMPLETE, null, null, null, null);
        }

        private static SseCommand error(Throwable error) {
            return new SseCommand(CommandType.ERROR, null, null, error, null);
        }
    }
}
