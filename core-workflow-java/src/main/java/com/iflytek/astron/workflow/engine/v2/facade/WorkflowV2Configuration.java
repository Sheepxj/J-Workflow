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
import com.iflytek.astron.workflow.engine.v2.store.JdbcExecutionStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import javax.sql.DataSource;
import java.util.List;
import java.util.ArrayList;
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
    public ExecutionStore workflowV2Store(ObjectMapper mapper, @Qualifier("workflowDataSource") ObjectProvider<DataSource> sources,
                                          @Value("${workflow.v2.store:memory}") String storeType,
                                          @Value("${workflow.v2.initialize-schema:false}") boolean initializeSchema) {
        if ("memory".equals(storeType)) {
            return new InMemoryExecutionStore(mapper);
        }
        if ("jdbc".equals(storeType)) {
            return new JdbcExecutionStore(sources.getObject(), mapper, initializeSchema);
        }
        throw new IllegalArgumentException("Unknown workflow.v2.store: " + storeType);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService workflowV2Workers() {
        return new ThreadPoolExecutor(4, 32, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256),
                Thread.ofPlatform().daemon(true).name("workflow-v2-worker-", 0).factory(), new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean
    public WorkflowScheduler workflowV2Scheduler(ObjectMapper mapper, ExecutionStore store, List<NodeTaskExecutor> executors,
                                                @Qualifier("workflowV2Workers") ExecutorService workers,
                                                ObjectProvider<ModelServiceClient> models,
                                                ObjectProvider<PluginServiceClient> plugins) {
        List<NodeTaskExecutor> registered = new ArrayList<>(executors);
        if (registered.stream().noneMatch(executor -> "LLM".equals(executor.nodeType()))) {
            ModelServiceClient client = models.getIfAvailable();
            if (client != null) {
                registered.add(new LlmTaskExecutor(client, mapper));
            }
        }
        if (registered.stream().noneMatch(executor -> "PLUGIN".equals(executor.nodeType()))) {
            PluginServiceClient client = plugins.getIfAvailable();
            if (client != null) {
                registered.add(new PluginTaskExecutor(client));
            }
        }
        WorkflowCompiler compiler = new WorkflowCompiler(mapper, registered.stream().map(NodeTaskExecutor::nodeType).collect(Collectors.toSet()));
        return new WorkflowScheduler(compiler, store, registered, workers, Clock.systemUTC());
    }

    @Bean(destroyMethod = "close")
    public WorkflowExecutionFacade workflowExecutionFacade(WorkflowScheduler scheduler, ExecutionStore store) {
        return new WorkflowExecutionFacade(scheduler, store);
    }
}
