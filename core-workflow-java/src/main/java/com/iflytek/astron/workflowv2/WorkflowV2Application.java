package com.iflytek.astron.workflowv2;

import com.iflytek.astron.workflow.controller.WorkflowV2Controller;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.model.OpenAiStyleLlmIntegration;
import com.iflytek.astron.workflow.engine.integration.plugins.aitools.AiToolsIntegration;
import com.iflytek.astron.workflow.engine.v2.executor.PluginTaskExecutor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;

@SpringBootApplication(scanBasePackages = "com.iflytek.astron.workflow.engine.v2",
        exclude = {RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class})
@Import({WorkflowV2Controller.class, ModelServiceClient.class, OpenAiStyleLlmIntegration.class})
public class WorkflowV2Application {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(WorkflowV2Application.class);
        application.setAdditionalProfiles("workflow-v2");
        application.run(args);
    }

    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource workflowDataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().build();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "workflow.v2.link-enabled", havingValue = "true")
    @Import(AiToolsIntegration.class)
    public static class LinkPluginConfiguration {
        @Bean
        public PluginTaskExecutor standalonePluginTaskExecutor(AiToolsIntegration integration) {
            return new PluginTaskExecutor(integration::call);
        }
    }
}
