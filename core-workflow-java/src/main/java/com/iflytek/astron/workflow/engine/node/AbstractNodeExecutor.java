package com.iflytek.astron.workflow.engine.node;

import cn.hutool.core.util.BooleanUtil;
import com.alibaba.fastjson2.JSON;
import com.iflytek.astron.workflow.engine.VariablePool;
import com.iflytek.astron.workflow.engine.constants.ErrorStrategyEnum;
import com.iflytek.astron.workflow.engine.constants.NodeExecStatusEnum;
import com.iflytek.astron.workflow.engine.constants.NodeTypeEnum;
import com.iflytek.astron.workflow.engine.domain.NodeRunResult;
import com.iflytek.astron.workflow.engine.domain.NodeState;
import com.iflytek.astron.workflow.engine.domain.chain.Node;
import com.iflytek.astron.workflow.engine.domain.chain.RetryConfig;
import com.iflytek.astron.workflow.engine.node.callback.WorkflowMsgCallback;
import com.iflytek.astron.workflow.engine.util.AsyncUtil;
import com.iflytek.astron.workflow.engine.util.FlowUtil;
import com.iflytek.astron.workflow.engine.util.JsonSnapshot;
import com.iflytek.astron.workflow.exception.ErrorCode;
import com.iflytek.astron.workflow.exception.NodeCustomException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class for node executors
 * Provides common functionality for all node types
 */
@Slf4j
public abstract class AbstractNodeExecutor implements NodeExecutor {

    @Override
    public NodeRunResult execute(NodeState nodeState) {
        Node node = nodeState.node();

        // 执行次数
        int executeTime = node.getExecutedCount().addAndGet(1);
        RetryConfig retryConfig = node.getData().getRetryConfig();
        if (retryConfig == null) {
            // 没有重试相关配置，直接执行（无超时控制）
            return nodeState.execution() == null ? this.doExecute(nodeState)
                    : nodeState.execution().invoke(nodeState, null, this::doExecute, this::executionFailure);
        }

        // 有配置就使用超时控制（无论是否重试）
        if (!BooleanUtil.isTrue(retryConfig.getShouldRetry())) {
            // 不支持重试，但支持超时控制
            return this.doExecuteWithTimeout(nodeState, retryConfig);
        }

        // 支持重试 + 超时控制
        while (true) {
            NodeRunResult res = this.doExecuteWithTimeout(nodeState, retryConfig);
            NodeExecStatusEnum executeRes = res.getStatus();
            if (executeRes.isSuccess()) {
                return res;
            }

            if (nodeState.execution() != null && !nodeState.execution().isActive()) {
                return res;
            }

            if (executeTime > retryConfig.getMaxRetries()) {
                // 超过重试次数
                return res;
            }
            // 退避等待
            this.handleRetryWait(retryConfig, executeTime);
            if (nodeState.execution() != null && !nodeState.execution().isActive()) {
                return res;
            }
            executeTime = node.getExecutedCount().addAndGet(1);
        }
    }

    private void handleRetryWait(RetryConfig retryConfig, int retryCount) {
        if (retryConfig.getRetryInterval() == null || retryConfig.getRetryInterval() <= 0) {
            return;
        }

        long intervalMillis = (long) (retryConfig.getRetryInterval() * 1000);
        long waitTime;

        Integer strategy = retryConfig.getRetryStrategy();
        if (strategy == null) {
            strategy = 0; // Default to fixed
        }

        switch (strategy) {
            case 0: // 固定间隔
                waitTime = intervalMillis;
                break;
            case 1: // 线性退避
                waitTime = intervalMillis * retryCount;
                break;
            case 2: // 指数退避
                waitTime = (long) (intervalMillis * Math.pow(2, retryCount - 1));
                break;
            default:
                waitTime = intervalMillis;
        }

        if (waitTime > 0) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                log.warn("Retry wait interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    protected NodeRunResult doExecuteWithTimeout(NodeState nodeState, RetryConfig retryConfig) {
        if (nodeState.execution() != null) {
            return nodeState.execution().invoke(nodeState, retryConfig, this::doExecute, this::executionFailure);
        }
        if (retryConfig.timeOutEnabled()) {
            // 设置了超时时间的场景
            try {
                return AsyncUtil.callWithTimeLimit(retryConfig.toMillis(), TimeUnit.MILLISECONDS,
                        () -> this.doExecute(nodeState));
            } catch (TimeoutException | InterruptedException e) {
                // 返回超时异常
                NodeRunResult result = new NodeRunResult();
                result.setError(new NodeCustomException(ErrorCode.TIMEOUT_ERROR));
                return errorResponse(nodeState, result);
            } catch (Exception e) {
                // 节点执行出现了非预期的异常
                NodeRunResult result = new NodeRunResult();
                result.setError(new NodeCustomException(ErrorCode.NODE_RUN_ERROR, e.getMessage()));
                return errorResponse(nodeState, result);
            }
        } else {
            return this.doExecute(nodeState);
        }
    }

    protected NodeRunResult doExecute(NodeState nodeStage) {
        Node node = nodeStage.node();
        WorkflowMsgCallback callback = nodeStage.callback();
        VariablePool variablePool = nodeStage.variablePool();
        String nodeId = node.getId();
        NodeTypeEnum nodeType = node.getNodeType();

        log.info("Executing node: {} (type: {})", nodeId, nodeType);

        // 开始执行节点
        callback.onNodeStart(0, node.getId(), node.getData().getNodeMeta().getAliasName());

        // Resolve inputs
        Map<String, Object> resolvedInputs = nodeStage.execution() == null ? prepareInputs(nodeStage) : nodeStage.execution().inputs();
        try {
            // Execute node
            if (log.isDebugEnabled()) {
                log.debug("Executing start nodeId: {}, req: {}", node.getId(), JSON.toJSONString(resolvedInputs));
            }
            NodeRunResult executeRes = executeNode(nodeStage, resolvedInputs);

            // Store outputs to variable pool
            storeOutputs(node, executeRes.getOutputs(), variablePool);

            // Node 执行结束，结果回传
            if (executeRes.getStatus() == null || executeRes.getStatus().isSuccess()) {
                successResponse(nodeStage, executeRes);
            } else {
                errorResponse(nodeStage, executeRes);
            }
            return executeRes;
        } catch (NodeCustomException e) {
            log.error("NodeCustomException executing node {}: {}", nodeId, e.getMessage(), e);
            NodeRunResult result = new NodeRunResult();
            result.setInputs(resolvedInputs);
            result.setError(e);
            return errorResponse(nodeStage, result);
        } catch (Exception e) {
            log.error("Exception executing node {}: {}", nodeId, e.getMessage(), e);
            NodeRunResult result = new NodeRunResult();
            result.setInputs(resolvedInputs);
            result.setError(new NodeCustomException(ErrorCode.NODE_RUN_ERROR, e.getMessage()));
            return errorResponse(nodeStage, result);
        }
    }


    /**
     * Execute the node-specific logic
     * Subclasses must implement this method
     *
     * @param nodeState workflow nodeState
     * @param inputs    input values
     * @throws Exception if execution fails
     */
    protected abstract NodeRunResult executeNode(NodeState nodeState, Map<String, Object> inputs) throws Exception;

    @Override
    public Map<String, Object> prepareInputs(NodeState state) {
        return state.node().getNodeType() == NodeTypeEnum.START ? state.variablePool().get(state.node().getId())
                : resolveInputs(state.node(), state.variablePool());
    }

    /**
     * Resolve all inputs for this node
     * Handles both literal values and variable references
     *
     * @param node         workflow node
     * @param variablePool variable pool
     * @return map of resolved input values
     */
    protected Map<String, Object> resolveInputs(Node node, VariablePool variablePool) {
        return NodeInputResolver.resolve(node, variablePool);
    }

    /**
     * 将node的输出结果，保存到变量池，用于初始化其他node启动的输入参数
     *
     * @param node         workflow node
     * @param outputs      output values produced by the node
     * @param variablePool variable pool
     */
    protected void storeOutputs(Node node, Map<String, Object> outputs, VariablePool variablePool) {
        String nodeId = node.getId();

        for (Map.Entry<String, Object> entry : outputs.entrySet()) {
            String outputName = entry.getKey();
            Object outputValue = entry.getValue();

            if (outputValue == null) {
                continue;
            }

            variablePool.set(nodeId, outputName, outputValue);

            if (log.isDebugEnabled()) {
                log.debug("Stored output: {}.{} = {}", nodeId, outputName, outputValue);
            }
        }
    }


    /**
     * 构建节点执行的成功状态
     *
     * @param nodeState 节点状态
     * @param result    节点运行结果
     * @return 节点执行状态枚举值
     */
    protected void successResponse(NodeState nodeState, NodeRunResult result) {
        Node node = nodeState.node();
        WorkflowMsgCallback callback = nodeState.callback();
        NodeRunResult response = presentation(nodeState, result);
        switch (nodeState.node().getNodeType()) {
            case START ->
                    callback.onStartNodeExecuted(node.getId(), node.getData().getNodeMeta().getAliasName(), response);
            case END -> callback.onEndNodeExecuted(node.getId(), node.getData().getNodeMeta().getAliasName(), response);
            default -> callback.onNodeEnd(node.getId(), node.getData().getNodeMeta().getAliasName(), response);
        }
    }


    /**
     * 构建节点执行的错误状态
     *
     * @param nodeState 节点状态
     * @param result    节点运行结果
     * @return 节点执行状态枚举值
     */
    private NodeRunResult errorResponse(NodeState nodeState, NodeRunResult result) {
        Node node = nodeState.node();
        RetryConfig retryConfig = node.getData().getRetryConfig();
        VariablePool variablePool = nodeState.variablePool();
        NodeCustomException e = result.getError();
        if (e == null) e = new NodeCustomException(ErrorCode.NODE_RUN_ERROR);
        WorkflowMsgCallback callback = nodeState.callback();
        log.warn("节点执行异常，进入异常分支流程: {}", node.getId(), e);
        if (retryConfig == null) {
            // 直接进入中断流程
            result.setError(e);
            result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
            callback.onNodeInterrupt(FlowUtil.genInterruptEventId(), Map.of(), node.getId(), node.getData().getNodeMeta().getAliasName(), e.getCode(), "interrupt", false);
            return result;
        }

        ErrorStrategyEnum errorStrategy = ErrorStrategyEnum.fromCode(retryConfig.getErrorStrategy());
        Map<String, Object> customOutput = retryConfig.getCustomOutput();
        if (errorStrategy == ErrorStrategyEnum.ERR_CODE) {
            // 错误码的场景
            storeOutputs(node, customOutput, variablePool);
            result.setOutputs(customOutput);
            result.setError(e);
            result.setErrorOutputs(customOutput);
            result.setStatus(NodeExecStatusEnum.ERR_CODE_MSG);
            callback.onNodeEnd(node.getId(), node.getData().getNodeMeta().getAliasName(), presentation(nodeState, result));
            return result;
        } else if (errorStrategy == ErrorStrategyEnum.ERR_CONDITION) {
            // 错误分支
            result.setError(e);
            result.setErrorOutputs(customOutput);
            result.setStatus(NodeExecStatusEnum.ERR_FAIL_CONDITION);
            callback.onNodeEnd(node.getId(), node.getData().getNodeMeta().getAliasName(), presentation(nodeState, result));
            return result;
        } else {
            // 异常中断流程
            result.setError(e);
            result.setErrorOutputs(customOutput);
            result.setStatus(NodeExecStatusEnum.ERR_INTERUPT);
            callback.onNodeInterrupt(FlowUtil.genInterruptEventId(), customOutput, node.getId(), node.getData().getNodeMeta().getAliasName(), e.getCode(), "interrupt", false);
            return result;
        }
    }

    private NodeRunResult presentation(NodeState state, NodeRunResult result) {
        return state.execution() == null ? result : JsonSnapshot.result(result);
    }

    private NodeRunResult executionFailure(NodeState state, NodeCustomException error) {
        NodeRunResult result = new NodeRunResult();
        result.setError(error);
        return errorResponse(state, result);
    }
}
