package com.iflytek.astron.workflow.controller;

import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowExecutionFacade;
import com.iflytek.astron.workflow.engine.v2.runtime.RunEvent;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowError;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowExecutionException;
import com.iflytek.astron.workflow.engine.v2.runtime.WorkflowRun;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workflow/v2/runs")
@ConditionalOnProperty(name = "workflow.v2.enabled", havingValue = "true", matchIfMissing = true)
public class WorkflowV2Controller {
    private final WorkflowExecutionFacade facade;

    public WorkflowV2Controller(WorkflowExecutionFacade facade) {
        this.facade = facade;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> start(@RequestBody StartRequest request) {
        return ResponseEntity.accepted().body(Map.of("runId", facade.start(request.definition(), request.inputs())));
    }

    @GetMapping("/{runId}")
    public WorkflowRun get(@PathVariable String runId) {
        return facade.get(runId);
    }

    @GetMapping("/{runId}/events")
    public List<RunEvent> events(@PathVariable String runId, @RequestParam(defaultValue = "0") long after,
                                 @RequestParam(defaultValue = "100") int limit) {
        if (after < 0 || limit < 1 || limit > 1000) {
            throw new WorkflowExecutionException(WorkflowError.INVALID_DEFINITION, "Invalid event cursor or limit");
        }
        return facade.get(runId).getEvents().stream().filter(event -> event.sequence() > after).limit(limit).toList();
    }

    @PostMapping("/{runId}/pause")
    public ResponseEntity<Void> pause(@PathVariable String runId) {
        facade.pause(runId);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{runId}/resume")
    public ResponseEntity<Void> resume(@PathVariable String runId) {
        facade.resume(runId);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/{runId}/cancel")
    public ResponseEntity<Void> cancel(@PathVariable String runId) {
        facade.cancel(runId);
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler(WorkflowExecutionException.class)
    public ResponseEntity<Map<String, String>> workflowError(WorkflowExecutionException error) {
        HttpStatus status = switch (error.getCode()) {
            case RUN_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case INVALID_RUN_STATE, STALE_REVISION -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(Map.of("code", error.getCode().name(), "message", error.getMessage()));
    }

    public record StartRequest(WorkflowDefinition definition, Map<String, Object> inputs) {
    }
}
