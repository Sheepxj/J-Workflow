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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * Frontend-compatible workflow execution controller
 * Provides API compatible with console-hub's workflow chat endpoint
 * 
 * @author 二哥编程星球&Java进阶之路（沉默王二&一灰）
 * @version 1.0.0
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/workflow")
public class WorkflowFrontendController {

    private final WorkflowService workflowService;
    private final WorkflowEngine workflowEngine;
    private final SseVirtualThreadExecutor sseVirtualThreadExecutor;
    private final WorkflowExecutor workflowExecutor;

    public WorkflowFrontendController(WorkflowService workflowService, WorkflowEngine workflowEngine,
                                      SseVirtualThreadExecutor sseVirtualThreadExecutor,
                                      WorkflowExecutor workflowExecutor) {
        this.workflowService = workflowService;
        this.workflowEngine = workflowEngine;
        this.sseVirtualThreadExecutor = sseVirtualThreadExecutor;
        this.workflowExecutor = workflowExecutor;
    }

    /**
     * Frontend-compatible workflow chat stream endpoint
     * Endpoint: POST /api/v1/workflow/chat/stream
     * <p>
     * Request body:
     * {
     * "flowId": "184736",
     * "inputs": { "user_input": "介绍一下 Java" },
     * "chatId": "some-chat-id",
     * "userId": "user-id"
     * }
     */
    @PostMapping(value = "/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter workflowChatStream(@RequestBody FrontendWorkflowRequest request) {
        log.info("Frontend workflow chat stream request: flowId={}, userId={}, chatId={}",
                request.getFlowId(), request.getUserId(), request.getChatId());

        SseEmitter emitter = new SseEmitter(600_000L);
        SseStreamCallback callback = new SseStreamCallback(emitter, sseVirtualThreadExecutor);

        workflowExecutor.execute(callback, () -> {
            try {
                WorkflowDSL workflowDSL = workflowService.getWorkflowDSL(request.getFlowId());
                workflowDSL.setUuid(request.getChatId());

                workflowEngine.execute(workflowDSL, new VariablePool(), request.getInputs(), callback);

                callback.callback("workflow_complete", Map.of("status", "success"));

                callback.complete();

            } catch (Exception e) {
                log.error("Workflow execution failed: {}", e.getMessage(), e);
                callback.callback("error", Map.of(
                        "error", e.getClass().getSimpleName(),
                        "message", e.getMessage()
                ));
                callback.completeWithError(e);
            }
        });

        return emitter;
    }

    /**
     * Frontend workflow request (compatible with console-hub format)
     */
    @Data
    public static class FrontendWorkflowRequest {

        @JsonProperty("flow_id")
        private String flowId;

        @JsonProperty("parameters")
        private Map<String, Object> inputs;

        @JsonProperty("chatId")
        private String chatId;

        @JsonProperty("uid")
        private String userId;

        @JsonProperty("regen")
        private Boolean regen;
    }
}
