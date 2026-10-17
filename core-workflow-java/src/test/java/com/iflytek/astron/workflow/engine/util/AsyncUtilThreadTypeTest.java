package com.iflytek.astron.workflow.engine.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncUtilThreadTypeTest {

    @Test
    void workflowExecutorUsesPlatformThreads() throws Exception {
        CompletableFuture<Thread> threadFuture = new CompletableFuture<>();

        AsyncUtil.execute(() -> threadFuture.complete(Thread.currentThread()));

        Thread thread = threadFuture.get(2, TimeUnit.SECONDS);
        assertFalse(thread.isVirtual());
        assertTrue(thread.getName().startsWith("workflow-aux-"));
    }
}
