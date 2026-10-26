package com.iflytek.astron.workflow.engine.execution;

import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory;
import com.iflytek.astron.workflow.engine.integration.model.LlmChatHistory.ChatItem;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.FlowUtil;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Clock;

/**
 * 一次工作流运行（run）的状态账本：变量池、节点执行记录、激活计数、对话历史、超时窗口与事件出口。
 *
 * <p>每个 run 独占一个实例，替代全局/静态状态，保证同一 JVM 内多个工作流并发互不干扰。
 * 本类不含业务逻辑，只做状态存取与生命周期编排；几乎所有方法都以自身为锁，使状态变更串行化。
 */
public final class WorkflowRunContext {
    // 运行标识：日志、指标、幂等键都基于它
    @Getter
    private final String runId;
    @Getter
    private final String flowId;
    // 会话标识：LLM 多轮历史按它累积，可跨多次 run 共享
    @Getter
    private final String chatId;
    @Getter
    private final String sid = FlowUtil.genSid();
    // 变量池：nodeId -> 节点输出；节点执行时只读快照，成功后才回写
    private final VariablePool variables = new VariablePool();
    // 节点事件/流式消息的投递出口
    private final WorkflowMsgCallback callback;
    // executionId -> 执行作用域
    private final Map<String, NodeExecutionScope> executions = new ConcurrentHashMap<>();
    // 按注册顺序保存作用域，让 executions() 输出稳定顺序
    private final List<NodeExecutionScope> executionOrder = new ArrayList<>();
    // activationId（即令牌 ID）-> executionId：同一令牌重放时复用同一作用域
    private final Map<String, String> activations = new LinkedHashMap<>();
    // nodeId -> 已激活次数，用于单节点/全局预算校验
    private final Map<String, Integer> activationCounts = new LinkedHashMap<>();
    // nodeId -> 最近一次成功执行的 ID，用于追溯入参取自哪次执行
    private final Map<String, String> lastExecutions = new LinkedHashMap<>();
    // nodeId -> 该节点的 LLM 对话历史
    private final Map<String, List<ChatItem>> histories;
    // 当前正在执行的 executionId；非空时不允许再 claim，保证一次只跑一个节点
    private String runningExecution;
    // 正在真实执行的调用数（含每次重试），用于判断 run 是否真的空闲
    private final AtomicInteger physicalAttempts = new AtomicInteger();
    // run 的截止时间与时钟（时钟可注入，便于测试超时）
    private volatile long executionDeadline = Long.MAX_VALUE;
    private volatile Clock executionClock = Clock.systemUTC();

    // 初始变量作为「已产出的变量快照」建立；历史从该 chatId 的持久层快照拷入
    public WorkflowRunContext(String runId, String flowId, String chatId, VariablePool initialVariables,
                              WorkflowMsgCallback callback) {
        this.runId = Objects.requireNonNull(runId);
        this.flowId = Objects.requireNonNull(flowId);
        this.chatId = Objects.requireNonNull(chatId);
        this.callback = Objects.requireNonNull(callback);
        variables.restore(initialVariables.snapshot());
        histories = new LinkedHashMap<>(LlmChatHistory.snapshot(chatId));
    }

    // 返回不可变快照，调用方无法改到内部变量池
    public synchronized Map<String, Map<String, Object>> variables() {
        return variables.snapshot();
    }

    // 按激活顺序返回全部执行快照
    public synchronized List<NodeActivation> executions() {
        return executionOrder.stream().map(NodeExecutionScope::snapshot).toList();
    }

    // 单个执行的快照；未知 ID 直接抛异常
    public synchronized NodeActivation execution(String executionId) {
        return scope(executionId).snapshot();
    }

    // 全部节点的历史快照（深拷贝）
    public synchronized Map<String, List<ChatItem>> histories() {
        return LlmChatHistory.copyHistories(histories);
    }

    public int physicalAttempts() {
        return physicalAttempts.get();
    }

    // 预算校验用的计数器：全局激活次数 / 指定节点的激活次数
    public synchronized int activationCount() {
        return executions.size();
    }

    public synchronized int activationCount(String nodeId) {
        return activationCounts.getOrDefault(nodeId, 0);
    }

    // 是否还有处于 READY/RUNNING 的节点执行
    public synchronized boolean hasPendingExecutions() {
        return executionOrder.stream().anyMatch(scope -> scope.status() == NodeActivation.Status.READY
                || scope.status() == NodeActivation.Status.RUNNING);
    }

    void setExecutionDeadline(long deadline, Clock clock) {
        executionClock = Objects.requireNonNull(clock);
        executionDeadline = deadline;
    }

    // 是否仍在 run 的时间窗口内（节点执行期间会反复检查）
    boolean executionAllowed() {
        return executionClock.millis() < executionDeadline;
    }

    // 取消指定执行：标记作用域取消，若它正占用执行位则一并释放
    public synchronized void cancel(String executionId) {
        NodeExecutionScope scope = scope(executionId);
        scope.cancel();
        if (executionId.equals(runningExecution)) {
            runningExecution = null;
        }
    }

    // 按 activationId（令牌 ID）查回已存在的执行作用域，用于重放时去重
    synchronized NodeExecutionScope activation(String activationId) {
        String executionId = activations.get(activationId);
        return executionId == null ? null : scope(executionId);
    }

    // 节点激活序号自增，返回本次是该节点的第几次激活
    synchronized int nextActivation(String nodeId) {
        return activationCounts.merge(nodeId, 1, Integer::sum);
    }

    // 登记新创建的执行作用域
    synchronized void register(NodeExecutionScope scope) {
        executions.put(scope.getExecutionId(), scope);
        executionOrder.add(scope);
        activations.put(scope.getActivationId(), scope.getExecutionId());
    }

    synchronized NodeExecutionScope scope(String executionId) {
        NodeExecutionScope scope = executions.get(executionId);
        if (scope == null) {
            throw new IllegalArgumentException("Unknown node execution: " + executionId);
        }
        return scope;
    }

    // READY -> RUNNING：抢占执行位，保证同一 run 同时只有一个节点在跑
    synchronized boolean claim(NodeExecutionScope scope) {
        if (scope.status() != NodeActivation.Status.READY) {
            return false;
        }
        if (runningExecution != null) {
            throw new IllegalStateException("Run already has an active node execution");
        }
        runningExecution = scope.getExecutionId();
        scope.start();
        return true;
    }

    /**
     * 提交一次节点执行的结果：成功（或带提示信息的错误码）才回写变量池，
     * 并更新该节点的最近执行 ID 与对话历史，最后释放执行位。
     */
    synchronized void complete(NodeExecutionScope scope, NodeRunResult result, List<ChatItem> history) {
        if (!scope.isActive()) {
            return;
        }
        NodeRunResult committed = scope.complete(result);
        if (committed.getStatus() == null || committed.getStatus().isSuccess()
                || committed.getStatus() == NodeExecStatusEnum.ERR_CODE_MSG) {
            variables.replaceNodeOutputs(scope.node().getId(), committed.getOutputs());
            lastExecutions.put(scope.node().getId(), scope.getExecutionId());
        }
        if (history != null) {
            histories.put(scope.node().getId(), LlmChatHistory.copyHistory(history));
            LlmChatHistory.restore(getChatId(), scope.node().getId(), history);
        }
        runningExecution = null;
    }

    // 指定节点的历史快照 / 最近一次成功执行的 ID
    synchronized List<ChatItem> history(String nodeId) {
        return LlmChatHistory.copyHistory(histories.getOrDefault(nodeId, List.of()));
    }

    synchronized String lastExecution(String nodeId) {
        return lastExecutions.get(nodeId);
    }

    WorkflowMsgCallback callback() {
        return callback;
    }

    // 在途物理调用计数：节点每次真实调用（含重试）前后增减，finishIfIdle 依赖它判断是否真空闲
    void enteredPhysicalAttempt() {
        physicalAttempts.incrementAndGet();
    }

    void exitedPhysicalAttempt() {
        physicalAttempts.decrementAndGet();
    }
}
