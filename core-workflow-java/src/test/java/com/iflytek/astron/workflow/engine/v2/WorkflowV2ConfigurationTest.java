package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.v2.executor.LlmTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.executor.PluginTaskExecutor;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowExecutionFacade;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowV2Configuration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class WorkflowV2ConfigurationTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(WorkflowV2Configuration.class)
            .withBean(ObjectMapper.class, ObjectMapper::new);

    @Test
    void registersBusinessExecutorsWhenTheirIntegrationsExist() {
        context.withInitializer(application -> {
                    application.getBeanFactory().registerSingleton("modelServiceClient", mock(ModelServiceClient.class));
                    application.getBeanFactory().registerSingleton("pluginServiceClient", mock(PluginServiceClient.class));
                })
                .run(application -> {
                    assertThat(application).hasNotFailed();
                    assertThat(application).hasSingleBean(WorkflowExecutionFacade.class);
                    assertThat(application).hasSingleBean(LlmTaskExecutor.class);
                    assertThat(application).hasSingleBean(PluginTaskExecutor.class);
                });
    }

    @Test
    void featureSwitchDisablesNewRunServices() {
        context.withPropertyValues("workflow.v2.enabled=false").run(application -> {
            assertThat(application).hasNotFailed();
            assertThat(application).doesNotHaveBean(WorkflowExecutionFacade.class);
        });
    }
}
