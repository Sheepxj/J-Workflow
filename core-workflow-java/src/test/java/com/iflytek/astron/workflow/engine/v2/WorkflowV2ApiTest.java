package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.controller.WorkflowV2Controller;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowExecutionFacade;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.util.List;
import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class WorkflowV2ApiTest {
    @Test
    void runsTheDocumentedWorkflowThroughTheHttpContract() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InMemoryExecutionStore store = new InMemoryExecutionStore(mapper);
        WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("MOCK")), store,
                List.of(new MockTaskExecutor()), Runnable::run, Clock.systemUTC());
        try (WorkflowExecutionFacade facade = new WorkflowExecutionFacade(scheduler, store)) {
            MockMvc mvc = MockMvcBuilders.standaloneSetup(new WorkflowV2Controller(facade)).build();
            ObjectNode definition = WorkflowCompilerTest.sample();
            ((ObjectNode) definition.withArray("nodes").get(2)).putObject("config").put("operation", "GENERATE");
            ((ObjectNode) definition.withArray("nodes").get(3)).putObject("config")
                    .put("operation", "VALIDATE").put("passesOnIteration", 3);
            ObjectNode request = mapper.createObjectNode();
            request.set("definition", definition);
            request.putObject("inputs").put("text", "seed");
            String response = mvc.perform(post("/api/workflow/v2/runs").contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request))).andExpect(status().isAccepted())
                    .andReturn().getResponse().getContentAsString();
            String runId = mapper.readTree(response).get("runId").asText();
            for (int index = 0; index < 40; index++) {
                scheduler.tick(runId);
            }

            mvc.perform(get("/api/workflow/v2/runs/{id}", runId)).andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                    .andExpect(jsonPath("$.outputs.result").value("draft-3"));
            mvc.perform(get("/api/workflow/v2/runs/{id}/events", runId).param("after", "1").param("limit", "2"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(2))
                    .andExpect(jsonPath("$[0].sequence").value(2));
            mvc.perform(post("/api/workflow/v2/runs/{id}/pause", runId)).andExpect(status().isConflict());
            mvc.perform(get("/api/workflow/v2/runs/unknown")).andExpect(status().isNotFound());
            mvc.perform(get("/api/workflow/v2/runs/{id}/events", runId).param("limit", "1001"))
                    .andExpect(status().isBadRequest());

            definition.put("dslVersion", "1");
            mvc.perform(post("/api/workflow/v2/runs").contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(request))).andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("INVALID_DEFINITION"));
        }
    }
}
