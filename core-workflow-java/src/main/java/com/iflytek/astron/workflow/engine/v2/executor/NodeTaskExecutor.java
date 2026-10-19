package com.iflytek.astron.workflow.engine.v2.executor;

import java.util.Map;

public interface NodeTaskExecutor {
    String nodeType();

    Map<String, Object> execute(NodeInvocation invocation) throws Exception;
}
