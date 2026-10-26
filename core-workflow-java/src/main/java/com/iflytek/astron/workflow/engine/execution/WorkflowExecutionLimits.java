package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;

/**
 * 单次工作流运行的资源上限，用来防止死循环或失控执行。
 *
 * @param maxNodeActivations  单个节点在一次 run 中被激活的次数上限
 * @param maxTotalActivations 一次 run 中所有节点激活次数之和的上限
 * @param maxDurationMs       一次 run 的总时长上限（毫秒）
 */
public record WorkflowExecutionLimits(int maxNodeActivations, int maxTotalActivations, long maxDurationMs) {
    public WorkflowExecutionLimits {
        // 三项都必须为正，避免误配成 0/负数导致一开跑就超限
        if (maxNodeActivations <= 0 || maxTotalActivations <= 0 || maxDurationMs <= 0) {
            throw new NodeCustomException(ErrorCode.INVALID_NODE_CONFIGURATION, "Workflow execution limits must be positive");
        }
    }

    // 默认配额：单节点 100 次、全局 10000 次、总时长 5 分钟
    public static WorkflowExecutionLimits defaults() {
        return new WorkflowExecutionLimits(100, 10000, 300000);
    }
}
