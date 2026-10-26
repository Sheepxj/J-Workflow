package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;

import java.util.List;
import java.util.Map;

/**
 * 单个节点「一次激活」的不可变快照，供路由判断、对外观测与测试使用。
 *
 * <p>构造时会对入参、结果做深拷贝，所以持有方无法通过改快照影响真实执行状态。
 *
 * @param executionId 本次执行的唯一 ID（重试时保持不变）
 * @param activationId 激活 ID，即触发它的控制令牌 ID（重放时保持不变）
 * @param activationNo 该节点在本 run 内第几次被激活
 * @param inputOrigins 每个入参的来源（字面量/节点输出/运行入参）及是否真的有值
 * @param attempts 本次激活经历的所有物理尝试（含重试）
 */
public record NodeActivation(String runId, String executionId, String activationId, String nodeId, int activationNo,
                             Status status, Map<String, Object> inputSnapshot, Map<String, InputOrigin> inputOrigins, NodeRunResult result,
                             List<Attempt> attempts) {
    public NodeActivation {
        inputSnapshot = JsonSnapshot.copy(inputSnapshot);
        inputOrigins = Map.copyOf(inputOrigins);
        result = result == null ? null : JsonSnapshot.result(result);
        attempts = List.copyOf(attempts);
    }

    // 结果同样每次返回副本，防止调用方修改快照内部数据
    @Override
    public NodeRunResult result() {
        return result == null ? null : JsonSnapshot.result(result);
    }

    // 一次激活的生命周期状态
    public enum Status {
        READY, RUNNING, SUCCEEDED, FAILED, CANCELLED
    }

    // 单次物理尝试的结果状态
    public enum AttemptStatus {
        RUNNING, SUCCEEDED, FAILED, TIMED_OUT, CANCELLED
    }

    // 一次物理尝试（重试的第 N 次）的时间线与错误信息
    public record Attempt(String attemptId, int attemptNo, AttemptStatus status, long startedAt,
                          long deadline, long finishedAt, Integer errorCode, String error) {
    }

    /**
     * 入参的来源描述。
     *
     * @param source      RUN_INPUT / LITERAL / NODE_OUTPUT / CUSTOM
     * @param nodeId      引用的上游节点
     * @param path        引用的上游输出字段
     * @param executionId 该上游输出产生时的执行 ID，用于排查取到了旧值
     * @param present     该入参是否真的存在（上游没产出时为 false）
     */
    public record InputOrigin(String source, String nodeId, String path, String executionId, boolean present) {
    }
}
