package com.iflytek.astron.workflow.engine.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflytek.astron.workflow.engine.integration.model.ModelServiceClient;
import com.iflytek.astron.workflow.engine.integration.plugins.PluginServiceClient;
import com.iflytek.astron.workflow.engine.v2.scheduler.WorkflowScheduler;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowExecutionFacade;
import com.iflytek.astron.workflow.engine.v2.facade.WorkflowV2Configuration;
import com.iflytek.astron.workflow.engine.v2.store.ExecutionStore;
import com.iflytek.astron.workflow.engine.v2.store.JdbcExecutionStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.UUID;

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
                    assertThat(application.getBean(WorkflowScheduler.class).registeredNodeTypes()).contains("LLM", "PLUGIN", "MOCK");
                });
    }

    @Test
    void featureSwitchDisablesNewRunServices() {
        context.withPropertyValues("workflow.v2.enabled=false").run(application -> {
            assertThat(application).hasNotFailed();
            assertThat(application).doesNotHaveBean(WorkflowExecutionFacade.class);
        });
    }

    @Test
    void jdbcCheckpointsUseTheWorkflowDatabaseWhenTheLinkDatabaseAlsoExists() {
        String database = "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1";
        context.withPropertyValues("workflow.v2.store=jdbc", "workflow.v2.initialize-schema=true")
                .withBean("workflowDataSource", DataSource.class, () -> new DriverManagerDataSource(database, "sa", ""))
                .withBean("linkDataSource", DataSource.class, () -> mock(DataSource.class))
                .run(application -> {
                    assertThat(application).hasNotFailed();
                    assertThat(application.getBean(ExecutionStore.class)).isInstanceOf(JdbcExecutionStore.class);
                    assertThat(application.getBean(ExecutionStore.class).activeRunIds()).isEmpty();
                });
    }
}
