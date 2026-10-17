package com.iflytek.astron.workflow.controller;

import com.iflytek.astron.workflow.engine.WorkflowEngine;
import com.iflytek.astron.workflow.engine.WorkflowEngineNodeDebug;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.callback.SseStreamCallback;
import com.iflytek.astron.workflow.engine.util.SseVirtualThreadExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowExecutor;
import com.iflytek.astron.workflow.exception.WorkflowOverloadedException;
import com.iflytek.astron.workflow.flow.service.WorkflowService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class SseControllerOverloadTest {

    private SimpleMeterRegistry registry;
    private SseVirtualThreadExecutor sseExecutor;
    private WorkflowExecutor workflowExecutor;
    private AtomicReference<SseStreamCallback> rejectedCallback;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        sseExecutor = new SseVirtualThreadExecutor(new WorkflowMetrics(registry));
        workflowExecutor = mock(WorkflowExecutor.class);
        rejectedCallback = new AtomicReference<>();
        doAnswer(invocation -> {
            SseStreamCallback callback = invocation.getArgument(0);
            rejectedCallback.set(callback);
            callback.abort();
            throw new WorkflowOverloadedException(new RejectedExecutionException("saturated"));
        }).when(workflowExecutor).execute(any(SseStreamCallback.class), any(Runnable.class));
    }

    @AfterEach
    void tearDown() {
        sseExecutor.destroy();
        registry.close();
    }

    @Test
    void workflowChatRejectsWith503AndStopsWriter() throws Exception {
        WorkflowController controller = new WorkflowController(
                mock(WorkflowService.class), mock(WorkflowEngine.class), sseExecutor, workflowExecutor);
        WorkflowController.WorkflowRequest request = new WorkflowController.WorkflowRequest();
        request.setFlowId("flow");
        request.setInputs(Map.of());

        assertRejected(() -> controller.executeWorkflow(request));
    }

    @Test
    void debugChatRejectsWith503AndStopsWriter() throws Exception {
        WorkflowDebugController controller = new WorkflowDebugController(
                mock(WorkflowService.class),
                mock(WorkflowEngineNodeDebug.class),
                mock(WorkflowEngine.class),
                sseExecutor,
                workflowExecutor);
        WorkflowDebugController.WorkflowDebugRequest request = new WorkflowDebugController.WorkflowDebugRequest();
        request.setFlowId("flow");
        request.setParameters(Map.of());

        assertRejected(() -> controller.executeWorkflow(request));
    }

    @Test
    void frontendChatRejectsWith503AndStopsWriter() throws Exception {
        WorkflowFrontendController controller = new WorkflowFrontendController(
                mock(WorkflowService.class), mock(WorkflowEngine.class), sseExecutor, workflowExecutor);
        WorkflowFrontendController.FrontendWorkflowRequest request =
                new WorkflowFrontendController.FrontendWorkflowRequest();
        request.setFlowId("flow");
        request.setInputs(Map.of());

        assertRejected(() -> controller.workflowChatStream(request));
    }

    @Test
    void protocolBuildRejectsWith503AndStopsWriter() throws Exception {
        ProtocolController controller = new ProtocolController(
                mock(WorkflowService.class), sseExecutor, workflowExecutor);

        assertRejected(() -> controller.buildWorkflow("flow"));
    }

    private void assertRejected(ThrowingCall call) throws Exception {
        WorkflowOverloadedException failure = assertThrows(WorkflowOverloadedException.class, call::run);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE,
                WorkflowOverloadedException.class.getAnnotation(ResponseStatus.class).value());
        assertTrue(failure.getCause() instanceof RejectedExecutionException);
        assertTrue(rejectedCallback.get().awaitTermination(2, TimeUnit.SECONDS));
        assertEquals(0, registry.get("sse.connections.active").gauge().value());
        assertEquals(0, registry.get("sse.virtual.threads").gauge().value());
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run();
    }
}
