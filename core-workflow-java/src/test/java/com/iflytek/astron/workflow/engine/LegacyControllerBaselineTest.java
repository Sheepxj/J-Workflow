package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.controller.WorkflowController;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.SseStreamCallback;
import com.iflytek.astron.workflow.engine.util.SseVirtualThreadExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowExecutor;
import com.iflytek.astron.workflow.flow.service.WorkflowService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class LegacyControllerBaselineTest {
    private SimpleMeterRegistry registry;
    private SseVirtualThreadExecutor streams;
    private WorkflowService service;
    private WorkflowEngine engine;
    private WorkflowController controller;
    private final AtomicReference<SseStreamCallback> callback = new AtomicReference<>();
    private final AtomicReference<Runnable> work = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        streams = new SseVirtualThreadExecutor(new WorkflowMetrics(registry));
        service = mock(WorkflowService.class);
        engine = mock(WorkflowEngine.class);
        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        doAnswer(invocation -> {
            callback.set(invocation.getArgument(0));
            work.set(invocation.getArgument(1));
            return null;
        }).when(executor).execute(any(SseStreamCallback.class), any(Runnable.class));
        controller = new WorkflowController(service, engine, streams, executor);
    }

    @AfterEach
    void tearDown() {
        if (callback.get() != null) {
            callback.get().abort();
        }
        streams.destroy();
        registry.close();
    }

    @Test
    void successBindsChatIdAndClosesTheSseWriter() throws Exception {
        WorkflowDSL dsl = LegacyFixtureSupport.load("sequential");
        when(service.getWorkflowDSL("flow")).thenReturn(dsl);

        assertNotNull(controller.executeWorkflow(request()));
        work.get().run();

        assertClosed();
        ArgumentCaptor<VariablePool> pools = ArgumentCaptor.forClass(VariablePool.class);
        verify(engine).execute(eq(dsl), pools.capture(), eq(Map.of("text", "seed")), eq(callback.get()));
        assertEquals("chat", dsl.getUuid());
        assertNotNull(pools.getValue());
    }

    @Test
    void executionFailureClosesTheWriterThroughTheControllerErrorPath() throws Exception {
        when(service.getWorkflowDSL("flow")).thenReturn(LegacyFixtureSupport.load("sequential"));
        doThrow(new IllegalStateException("execution failed")).when(engine).execute(any(), any(), anyMap(), any());

        controller.executeWorkflow(request());
        work.get().run();

        assertClosed();
        verify(engine).execute(any(), any(), eq(Map.of("text", "seed")), eq(callback.get()));
    }

    @Test
    void loadingFailureDoesNotInvokeTheEngineAndStillClosesTheWriter() {
        when(service.getWorkflowDSL("flow")).thenThrow(new IllegalArgumentException("missing flow"));

        controller.executeWorkflow(request());
        work.get().run();

        assertClosed();
        verifyNoInteractions(engine);
    }

    @Test
    void disconnectingTheCallbackDoesNotCancelTheQueuedLegacyWorkflow() throws Exception {
        when(service.getWorkflowDSL("flow")).thenReturn(LegacyFixtureSupport.load("sequential"));

        controller.executeWorkflow(request());
        callback.get().abort();
        work.get().run();

        assertClosed();
        verify(engine).execute(any(), any(), eq(Map.of("text", "seed")), eq(callback.get()));
    }

    private void assertClosed() {
        try {
            assertTrue(callback.get().awaitTermination(2, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        assertFalse(callback.get().isActive());
        assertEquals(0, registry.get("sse.connections.active").gauge().value());
    }

    private WorkflowController.WorkflowRequest request() {
        WorkflowController.WorkflowRequest request = new WorkflowController.WorkflowRequest();
        request.setFlowId("flow");
        request.setChatId("chat");
        request.setInputs(Map.of("text", "seed"));
        return request;
    }
}
