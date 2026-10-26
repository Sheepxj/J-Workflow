package com.iflytek.astron.workflow.engine;

import com.iflytek.astron.workflow.engine.constants.EndNodeOutputModeEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.WorkflowDSL;
import com.iflytek.astron.workflow.engine.domain.callbacks.LLMGenerate;
import com.iflytek.astron.workflow.engine.execution.NodeExecutionBridge;
import com.iflytek.astron.workflow.engine.execution.WorkflowExecutionLimits;
import com.iflytek.astron.workflow.engine.execution.WorkflowGraphExecutor;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmReqBo;
import com.iflytek.astron.workflow.engine.integration.model.bo.LlmResVo;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeMatch;
import com.iflytek.astron.workflow.engine.integration.pinecone.PineconeSearchClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import com.iflytek.astron.workflow.engine.node.NodeExecutor;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.node.impl.EndNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.StartNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.condition.IfElseNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.knowledge.KnowledgeBaseNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.llm.LLMNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.mock.MockNodeExecutor;
import com.iflytek.astron.workflow.engine.node.impl.plugin.PluginNodeExecutor;
import com.iflytek.astron.workflow.engine.util.WorkflowStreamVirtualThreadExecutor;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

final class WorkflowGraphTestSupport implements AutoCloseable {
    final ModelServiceClient models = mock(ModelServiceClient.class);
    final AtomicInteger modelCalls = new AtomicInteger();
    final List<LlmReqBo> requests = new CopyOnWriteArrayList<>();
    final List<LLMGenerate> frames = new CopyOnWriteArrayList<>();
    final WorkflowMsgCallback callback;
    final WorkflowGraphExecutor executor;
    final NodeExecutionBridge bridge;
    final LLMNodeExecutor llm;
    final List<NodeExecutor> executors;
    private final WorkflowStreamVirtualThreadExecutor streams;
    private final List<String> chats = new ArrayList<>();
    private volatile List<String> responses = List.of("RETRY", "RETRY", "PASS");

    WorkflowGraphTestSupport(NodeExecutor... replacements) throws Exception {
        this(Clock.systemUTC(), Executors.newVirtualThreadPerTaskExecutor(), replacements);
    }

    WorkflowGraphTestSupport(Clock clock, ExecutorService workers, NodeExecutor... replacements) throws Exception {
        streams = new WorkflowStreamVirtualThreadExecutor(mock(WorkflowMetrics.class));
        callback = spy(new WorkflowMsgCallback("graph", (type, data) -> frames.add((LLMGenerate) data),
                EndNodeOutputModeEnum.VARIABLE_MODE, new ArrayBlockingQueue<>(1024), new ConcurrentLinkedQueue<>(), streams));
        Usage usage = mock(Usage.class);
        when(usage.getPromptTokens()).thenReturn(7);
        when(usage.getCompletionTokens()).thenReturn(5);
        when(usage.getTotalTokens()).thenReturn(12);
        when(models.chatCompletion(any(), any())).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            int index = modelCalls.getAndIncrement();
            return new LlmResVo(usage, responses.get(Math.min(index, responses.size() - 1)), "checked");
        });
        llm = spy(new LLMNodeExecutor(models));
        PluginServiceClient plugins = mock(PluginServiceClient.class);
        when(plugins.toolCall(any(), any())).thenReturn(Map.of("output", "tool-output"));
        PluginNodeExecutor plugin = new PluginNodeExecutor();
        ReflectionTestUtils.setField(plugin, "pluginServiceClient", plugins);
        PineconeSearchClient search = mock(PineconeSearchClient.class);
        when(search.search(any(), any(), anyInt(), any())).thenReturn(List.of(new PineconeMatch(
                "match", 0.9, "accepted", Map.of("repo_id", "repo-1", "file_name", "guide.md"))));
        Map<NodeTypeEnum, NodeExecutor> registry = new EnumMap<>(NodeTypeEnum.class);
        for (NodeExecutor node : List.of(new StartNodeExecutor(), new EndNodeExecutor(), new MockNodeExecutor(),
                new IfElseNodeExecutor(), llm, plugin, new KnowledgeBaseNodeExecutor(search))) {
            registry.put(node.getNodeType(), node);
        }
        for (NodeExecutor replacement : replacements) {
            registry.put(replacement.getNodeType(), replacement);
        }
        executors = List.copyOf(registry.values());
        bridge = new NodeExecutionBridge(executors, clock);
        executor = new WorkflowGraphExecutor(bridge, new WorkflowExecutionLimits(30, 200, 5000), clock, workers);
    }

    WorkflowDSL fixture(String name) throws Exception {
        WorkflowDSL definition = LegacyFixtureSupport.load(name);
        String chatId = UUID.randomUUID().toString();
        chats.add(chatId);
        definition.setUuid(chatId);
        return definition;
    }

    void responses(String... values) {
        responses = List.of(values);
    }

    @Override
    public void close() {
        executor.close();
        callback.finished();
        streams.destroy();
        chats.forEach(chat -> LlmChatHistory.clearHistory(chat, "spark-llm::generate"));
    }
}
