package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.integration.plugins.aitools.Tool;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.definition.WorkflowDefinition;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.PluginTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.runtime.RuntimeStatus.RunStatus;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginIdempotencyWireTest {
    @Test
    void retriesSendTheSameKeyWhileEachLoopExecutionUsesANewKey() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Boolean> effects = new ConcurrentHashMap<>();
        ConcurrentLinkedQueue<String> keys = new ConcurrentLinkedQueue<>();
        AtomicInteger calls = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/run", exchange -> {
            try {
                JsonNode request = mapper.readTree(exchange.getRequestBody());
                byte[] encoded = Base64.getDecoder().decode(request.at("/payload/message/header").asText());
                String key = mapper.readTree(encoded).get("Idempotency-Key").asText();
                keys.add(key);
                calls.incrementAndGet();
                boolean firstCall = effects.putIfAbsent(key, true) == null;
                byte[] response = firstCall ? "response lost after side effect".getBytes(StandardCharsets.UTF_8)
                        : mapper.writeValueAsBytes(Map.of("header", Map.of("code", 0), "payload",
                                Map.of("text", Map.of("text", "{\"text\":\"generated\"}"))));
                exchange.sendResponseHeaders(firstCall ? 500 : 200, response.length);
                exchange.getResponseBody().write(response);
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            String endpoint = new URI("http", null, server.getAddress().getAddress().getHostAddress(),
                    server.getAddress().getPort(), "/run", null, null).toString();
            Tool tool = new Tool("test-app", "test-tool", "generate", Map.of(), Map.of(), endpoint, endpoint, "V1.0");
            PluginServiceClient client = mock(PluginServiceClient.class);
            when(client.toolCall(any(), any())).thenAnswer(invocation -> tool.run(invocation.getArgument(1), Map.of()));
            ObjectNode sample = WorkflowCompilerTest.sample();
            ((ObjectNode) sample.withArray("nodes").get(2)).put("type", "PLUGIN").putObject("retry")
                    .put("maxAttempts", 2).put("delayMs", 0).put("timeoutMs", 10000);
            ((ObjectNode) sample.withArray("nodes").get(3)).putObject("config").put("operation", "VALIDATE");
            WorkflowScheduler scheduler = new WorkflowScheduler(new WorkflowCompiler(mapper, Set.of("PLUGIN", "MOCK")),
                    new InMemoryExecutionStore(mapper), List.of(new PluginTaskExecutor(client), new MockTaskExecutor()),
                    Runnable::run, Clock.systemUTC());
            String runId = scheduler.start(mapper.treeToValue(sample, WorkflowDefinition.class), Map.of("text", "seed"));
            for (int index = 0; index < 60; index++) {
                scheduler.tick(runId);
            }

            assertEquals(RunStatus.SUCCEEDED, scheduler.get(runId).getStatus());
            assertEquals(6, calls.get());
            assertEquals(3, effects.size());
            assertTrue(effects.keySet().stream().allMatch(key -> keys.stream().filter(key::equals).count() == 2));
            assertEquals(3, scheduler.get(runId).getExecutions().values().stream()
                    .filter(execution -> execution.getNodeId().equals("generate") && execution.getAttempts().size() == 2).count());
        } finally {
            server.stop(0);
        }
    }
}
