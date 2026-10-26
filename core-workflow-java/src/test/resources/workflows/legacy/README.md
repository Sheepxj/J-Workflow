# 原 DSL 兼容样例

这些 JSON 的根对象就是原 `WorkflowDSL`：`flowId`、`uuid`、`nodes`、`edges`。它们不是 V2 的 `{definition, inputs}` 请求，也不包含 LOOP/BACK 声明。运行输入由测试或调用方另外提供。

除特别说明外，运行输入为 `{"text":"seed"}`。LLM、插件和知识库在契约测试中使用替身服务；配置中的模型与工具标识仅用于测试，不用于外部请求。

| 文件 | P0 时点验证 | P2 当前结果与后续目标 |
| --- | --- | --- |
| [sequential.json](sequential.json) | 原 START → MOCK → END 成功，结束变量 `content = Result: seed`，顺序发送流事件 | 增强路径保持结果及开始/结束展示契约，已通过 |
| [failure-branch.json](failure-branch.json) | MOCK 必然失败，normal 跳过，handler 执行，结束内容 `Result: recovered` | 增强路径保持错误分支语义，已通过 |
| [custom-error-output.json](custom-error-output.json) | 错误策略 1 发布 fallback 并继续正常边，结束内容 `Result: fallback` | 增强路径保持自定义错误输出语义，已通过 |
| [interrupt.json](interrupt.json) | 错误策略 0 中断，发送中断帧、工作流结束帧并抛出异常；END 不执行 | 旧入口契约保留；增强入口返回 FAILED 和错误码，END 不执行，已通过；暂停是 P3 的另一种状态 |
| [llm-history.json](llm-history.json) | 原 LLM 模板、输出名、reason、历史开关与历史轮数通过替身模型验证 | P1 隔离与 P2 循环历史已验证；跨进程重建待 P4 |
| [plugin.json](plugin.json) | 原插件执行器接收原 NodeState 和已解析输入 | 增强完整流程经过原 PluginNodeExecutor，已通过 |
| [knowledge.json](knowledge.json) | 原知识库执行器使用过滤条件，输出 context、sources、results | 增强完整流程经过原 KnowledgeBaseNodeExecutor，已通过 |
| [branch-cycle.json](branch-cycle.json) | 两种 JSON 解析器均保留 cases、输入 ID 与 sourceHandle；P0 注册表缺少 IF_ELSE | P2 用原 LLM 的替身返回 RETRY、RETRY、PASS，LLM 激活 3 次，共 8 次激活，最终 `Result: PASS`，已通过 |
| [exclusive-merge.json](exclusive-merge.json) | 原格式可解析；P0 引擎因缺少 IF_ELSE 拒绝 | P2 输入 `left` 时只走左路，其他输入走默认右路，合并节点只执行一次，已通过 |
| [parallel-fork.json](parallel-fork.json) | 原 ParallelWorkflowEngine 执行两路并汇聚，merge 只执行一次 | P2 增强路径预检明确拒绝，旧 DAG 并行路径保留，两项均已通过；P6 再扩展 |

分支回跳样例从结束条件 `answer == PASS` 的分支走 END，默认分支返回原 LLM。第一次与后续调用都使用开始节点 text，开启的原模型历史可携带此前对话，因此这个样例不依赖隐式读取尚未产生的节点输出。

P0 固定当时行为；P2 已使用真实 IfElseNodeExecutor、原 LLM 与原节点桥接器验收分支和回跳，没有使用测试专用路由器代替实际调度。模型、插件和知识库客户端仍使用替身，不触发外部服务调用。

源码与已运行测试见 [P0 兼容性基线与验收记录](../../../../../../docs/P0兼容性基线与验收记录.md) 和 [P2 原 DSL 有环执行与分支路由验收记录](../../../../../../docs/P2原DSL有环执行与分支路由验收记录.md)。原 HTTP 尚未切换到增强入口，Java 调用方式见 P2 文档。
