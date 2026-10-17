package com.iflytek.astron.workflow.engine.util;

import com.iflytek.astron.workflow.engine.metrics.WorkflowMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class WorkflowExecutorSpringTest {

    @Test
    void springSelectsProductionConstructor() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new);
            context.register(WorkflowMetrics.class, WorkflowExecutor.class);
            context.refresh();
            assertNotNull(context.getBean(WorkflowExecutor.class));
        }
    }
}
