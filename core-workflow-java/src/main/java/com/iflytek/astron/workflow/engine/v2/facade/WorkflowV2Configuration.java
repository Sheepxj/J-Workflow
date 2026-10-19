package com.iflytek.astron.workflow.engine.v2.facade;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.v2.compiler.WorkflowCompiler;
import com.iflytek.astron.workflow.engine.v2.executor.MockTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.LlmTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.PluginTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.NodeTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.store.ExecutionStore;
import com.iflytek.astron.workflow.engine.v2.store.InMemoryExecutionStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "workflow.v2.enabled", havingValue = "true", matchIfMissing = true)
public class WorkflowV2Configuration {
    @Bean
    public MockTaskExecutor mockTaskExecutor() {
        return new MockTaskExecutor();
    }

    @Bean
    @ConditionalOnBean(ModelServiceClient.class)
    public LlmTaskExecutor llmTaskExecutor(ModelServiceClient client, ObjectMapper mapper) {
        return new LlmTaskExecutor(client, mapper);
    }

    @Bean
    @ConditionalOnBean(PluginServiceClient.class)
    public PluginTaskExecutor pluginTaskExecutor(PluginServiceClient client) {
        return new PluginTaskExecutor(client);
    }

    @Bean
    public ExecutionStore workflowV2Store(ObjectMapper mapper) {
        return new InMemoryExecutionStore(mapper);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService workflowV2Workers() {
        return new ThreadPoolExecutor(4, 32, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256),
                Thread.ofPlatform().daemon(true).name("workflow-v2-worker-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean
    public WorkflowScheduler workflowV2Scheduler(ObjectMapper mapper, ExecutionStore store, List<NodeTaskExecutor> executors,
                                                @Qualifier("workflowV2Workers") ExecutorService workers) {
        WorkflowCompiler compiler = new WorkflowCompiler(mapper, executors.stream().map(NodeTaskExecutor::nodeType).collect(Collectors.toSet()));
        return new WorkflowScheduler(compiler, store, executors, workers, Clock.systemUTC());
    }

    @Bean(destroyMethod = "close")
    public WorkflowExecutionFacade workflowExecutionFacade(WorkflowScheduler scheduler, ExecutionStore store) {
        return new WorkflowExecutionFacade(scheduler, store);
    }
}
