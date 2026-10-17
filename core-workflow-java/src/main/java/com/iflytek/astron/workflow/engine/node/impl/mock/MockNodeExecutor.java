package com.iflytek.astron.workflow.engine.node.impl.mock;

import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.node.AbstractNodeExecutor;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class MockNodeExecutor extends AbstractNodeExecutor {
    private static final long DEFAULT_DELAY_MS = 100;
    private static final int MAX_PAYLOAD_BYTES = 10 * 1024 * 1024;
    private static final int MAX_EVENT_COUNT = 100_000;
    private static final int MAX_EVENT_BYTES = 64 * 1024;

    @Override
    public NodeTypeEnum getNodeType() {
        return NodeTypeEnum.MOCK;
    }

    @Override
    protected NodeRunResult executeNode(NodeState nodeState, Map<String, Object> inputs) {
        Map<String, Object> nodeParam = nodeState.node().getData().getNodeParam();
        long delayMs = nonNegativeLong(nodeParam.get("delayMs"), DEFAULT_DELAY_MS, "delayMs");
        long jitterMs = nonNegativeLong(nodeParam.get("jitterMs"), 0, "jitterMs");
        double failureRate = rate(nodeParam.get("failureRate"));
        int payloadBytes = payloadBytes(nodeParam.get("payloadBytes"));
        int eventCount = boundedInt(nodeParam.get("eventCount"), 0, MAX_EVENT_COUNT, "eventCount");
        int eventBytes = boundedInt(nodeParam.get("eventBytes"), 0, MAX_EVENT_BYTES, "eventBytes");
        long actualDelayMs = actualDelay(delayMs, jitterMs);

        sleep(actualDelayMs);
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new NodeCustomException(ErrorCode.NODE_RUN_ERROR, "Mock node configured failure");
        }
        emitEvents(nodeState, eventCount, eventBytes);

        Map<String, Object> outputs = new HashMap<>();
        outputs.put("output", inputs.getOrDefault("input", inputs));
        outputs.put("actualDelayMs", actualDelayMs);
        outputs.put("payload", "x".repeat(payloadBytes));

        NodeRunResult result = new NodeRunResult();
        result.setInputs(inputs);
        result.setOutputs(outputs);
        result.setStatus(NodeExecStatusEnum.SUCCESS);
        return result;
    }

    private void emitEvents(NodeState nodeState, int eventCount, int eventBytes) {
        if (eventCount == 0 || nodeState.callback() == null) {
            return;
        }
        String payload = "x".repeat(eventBytes);
        String nodeId = nodeState.node().getId();
        String aliasName = nodeState.node().getData().getNodeMeta().getAliasName();
        for (int index = 0; index < eventCount; index++) {
            nodeState.callback().onNodeProcess(0, nodeId, aliasName, "event-" + index + ":" + payload, "");
        }
    }

    private long actualDelay(long delayMs, long jitterMs) {
        if (jitterMs == 0) {
            return delayMs;
        }
        long lowerBound = Math.max(0, delayMs - jitterMs);
        long upperBound = delayMs > Long.MAX_VALUE - jitterMs ? Long.MAX_VALUE : delayMs + jitterMs;
        return ThreadLocalRandom.current().nextLong(lowerBound, upperBound == Long.MAX_VALUE ? upperBound : upperBound + 1);
    }

    private void sleep(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NodeCustomException(ErrorCode.INTERRUPTED_ERROR);
        }
    }

    private long nonNegativeLong(Object value, long defaultValue, String field) {
        if (value == null) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(String.valueOf(value));
            if (parsed >= 0) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
        }
        throw new NodeCustomException(ErrorCode.PARAM_ERROR, field + " must be a non-negative integer");
    }

    private double rate(Object value) {
        if (value == null) {
            return 0;
        }
        try {
            double parsed = Double.parseDouble(String.valueOf(value));
            if (parsed >= 0 && parsed <= 1) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
        }
        throw new NodeCustomException(ErrorCode.PARAM_ERROR, "failureRate must be between 0 and 1");
    }

    private int payloadBytes(Object value) {
        long parsed = nonNegativeLong(value, 0, "payloadBytes");
        if (parsed > MAX_PAYLOAD_BYTES) {
            throw new NodeCustomException(ErrorCode.PARAM_ERROR, "payloadBytes must not exceed " + MAX_PAYLOAD_BYTES);
        }
        return (int) parsed;
    }

    private int boundedInt(Object value, int defaultValue, int maximum, String field) {
        long parsed = nonNegativeLong(value, defaultValue, field);
        if (parsed > maximum) {
            throw new NodeCustomException(ErrorCode.PARAM_ERROR, field + " must not exceed " + maximum);
        }
        return (int) parsed;
    }
}
