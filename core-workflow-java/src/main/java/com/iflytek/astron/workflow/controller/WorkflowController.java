package com.iflytek.astron.workflow.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.WorkflowEngine;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.node.callback.SseStreamCallback;
import com.iflytek.astron.workflow.engine.util.SseVirtualThreadExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowExecutor;
import com.iflytek.astron.workflow.flow.service.WorkflowService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * Workflow execution controller
 * Provides REST API for workflow execution with SSE streaming support
 * 
 * @author 二哥编程星球&Java进阶之路（沉默王二&一灰）
 * @version 1.0.0
 */
@Slf4j
@RestController
@RequestMapping("/api/workflow")
public class WorkflowController {

    private final WorkflowService workflowService;
    private final WorkflowEngine workflowEngine;
    private final SseVirtualThreadExecutor sseVirtualThreadExecutor;
    private final WorkflowExecutor workflowExecutor;

    public WorkflowController(WorkflowService workflowService, WorkflowEngine workflowEngine,
                              SseVirtualThreadExecutor sseVirtualThreadExecutor,
                              WorkflowExecutor workflowExecutor) {
        this.workflowService = workflowService;
        this.workflowEngine = workflowEngine;
        this.sseVirtualThreadExecutor = sseVirtualThreadExecutor;
        this.workflowExecutor = workflowExecutor;
    }

    /**
     * Execute workflow with SSE streaming
     * <p>
     * Endpoint: POST /api/workflow/chat
     * Request body:
     * {
     * "flow_id": "184736",
     * "inputs": { "user_input": "介绍一下 Java" },
     * "chatId": "some-chat-id",
     * "regen": false
     * }
     * <p>
     * Response: SSE stream with events:
     * - node_start: Node execution started
     * - node_output: Node produced output
     * - node_end: Node execution completed
     * - workflow_complete: Workflow finished
     * - error: Error occurred
     */
    @PostMapping(value = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter executeWorkflow(@RequestBody WorkflowRequest request) {
        log.info("Workflow execution request: flowId={}, inputs={}", request.getFlowId(), request.getInputs());

        SseEmitter emitter = new SseEmitter(600_000L);
        SseStreamCallback callback = new SseStreamCallback(emitter, sseVirtualThreadExecutor);

        workflowExecutor.execute(callback, () -> {
            try {
                WorkflowDSL workflowDSL = workflowService.getWorkflowDSL(request.getFlowId());
                workflowDSL.setUuid(request.getChatId());

                workflowEngine.execute(workflowDSL, new VariablePool(), request.getInputs(), callback);

                callback.complete();

            } catch (Exception e) {
                log.error("Workflow execution failed: {}", e.getMessage(), e);
                callback.completeWithError(e);
            }
        });

        return emitter;
    }

    /**
     * Workflow execution request
     */
    @Data
    public static class WorkflowRequest {

        @JsonProperty("flow_id")
        private String flowId;

        @JsonProperty("inputs")
        private Map<String, Object> inputs;

        @JsonProperty("chatId")
        private String chatId;

        @JsonProperty("regen")
        private Boolean regen;
    }
}
