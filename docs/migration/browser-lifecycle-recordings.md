# 浏览器生命周期、语义快照与录制消费边界

本次修复使用现有 Rust 执行监督器、SQLite `execution_resources` 和本机 Python UDS，不新增执行引擎、录制下载接口或数据库表。

## 语义快照

- 语义来源为 `tree.safe_dom`，`tree.source=safe_dom_v1`。同一同步 DOM 读取中保留角色、名称、正文和普通表单值；密码控件不读取 `value`，包括采集前由 text 动态变成 password 的控件。
- 不调用 Playwright 原生 ARIA 快照；`components.aria` 为 `not_requested`，原因 `PASSWORD_SAFE_PROJECTION`。安全投影正常完成可以是 `capture_status=complete`，不因此永久降为警告。
- 投影、交互清单、截图分别报告组件状态。截断或可选截图失败为 `partial`；必需投影失败为 `failed`。这些状态描述采集结果，不代表用户旅程验证通过。
- 选择器在当前 document 中确定采集根；投影遍历根内的 light DOM 和可访问的 open shadow root（含嵌套），使用同一份安全元素投影。`aria-labelledby` 在元素所属 document/shadow root 内解析；不重排 slot、不构造完整无障碍树，不进入 iframe 或 closed shadow root。
- 保留独立的 light DOM 交互查询：按原顺序采集前 200 项，不因前方大量文本、隐藏节点或安全树截断而遗漏后方控件。安全树/open shadow 发现共用 2000 个遍历节点预算，发现的 shadow 控件补入剩余交互容量；不重复展开 slot 节点。树文本共用 64 KiB UTF-8 上限，文本超限不停止节点预算内的交互采集；截断时报告 `partial`。
- 安全树继续整体跳过 `hidden`、script/style/noscript/template 子树，以及 textarea 的原始子文本；这些被排除的后代不消耗树节点预算。
- `node_count` 沿用当前采集根的 light DOM 元素统计（包括根及隐藏后代），不计新增遍历的 shadow 内容，不声称完整跨 shadow 树总量；它独立于安全树的采集预算。无法采集如实呈现。`complete` 仍表示已声明投影范围正常完成。
- Rust Replay 顶层字段为 `captureStatus`；`components` 与 `tree.safe_dom` 保持 Python 字段形状。回放因容量限制省略截图时记录 `REPLAY_BYTE_BUDGET_EXCEEDED`，不继续声称截图已保留。

## 所属关系及创建收敛

- 创建绝对期限为 30 秒，并受 Run 剩余期限约束。超时不丢弃唯一分配 Future；保留精确创建预约、初始化与文件准备责任，迟到的 context 只被回收，不再发布。
- 文件准备线程的完成不能由浏览器进程关闭推导。即使浏览器祖先进程确认退出，也要保留未结束的录制准备预约，防止随后落盘被误判为“从未创建”。
- 清理未确认时拒绝新分配。显式恢复只有在其他正常 Session 都已安全关闭后才可回收共享 browser/driver；不自动重启共享服务或中断其他会话。
- 普通会话使用 Session 所属 context；首次实际使用后建立 Run 使用租约，15 秒续约、最长 60 秒 TTL，并受 Run 总期限约束。活跃使用租约保护长时间思考，不曾使用浏览器的 Run 不保护空闲 context。Run 结束只释放使用租约，保留普通会话跨轮次状态。
- 未确认 generation 的预约只承担对账责任，不允许执行浏览器动作。重试前先释放原 usage identity，再使用新 epoch 获取租约；释放未确认时继续拒绝动作。Python 对宿主动作同时复核 Session、Run、epoch、generation、期限及实际 context 身份；严格查找不能隐式创建 context。
- 临时会话 context 为 Run 所属，终态清理且禁止录制。删除或合并普通 Session 先封闭执行准入，再按真实 Session 所属关系关闭 context；活跃租约返回 busy。
- 父会话删除检查完整后代 Session，录制门控位于最终删除事务内；普通浏览器 owner close 使用同一后代范围，避免外键级联遗漏子任务录制。

## 录制与 ACK

1. Rust 在实际调用前注册 browser resource，写入 `recordingFinalization={version:1,phase:reserved,identity}`。identity 包含宿主随机 batch、Session、Run、Invocation。
2. Python 只写私有 spool（默认 `~/.zkcode/browser-recordings`，宿主可配置 `ZK_BROWSER_RECORDING_SPOOL`）。目录 0700，元数据 0600；批次身份不使用模型文件名。最多 10 个未确认批次，满后拒绝新录制。
3. context 确认关闭后封存 manifest，列出文件路径、类型、大小、设备/inode、mtime、摘要或明确缺失原因。单文件 10 MiB、整批 20 MiB；超过限额记录 `omitted_budget`，不假称已归档。
4. Rust 在目录描述符下 O_NOFOLLOW 打开私有文件，验证实际 manifest 与文件身份、大小及内容摘要后写入既有 blob 存储。任意读取或保存异常保留录制责任并如实报错，不能伪装成预算遗漏。
5. 浏览器物理资源 `status=released` 与录制消费独立。`sealed → ackEligible` 的窄 CAS 必须在真实 SQLite 中确认同一所属关系的 machine evidence、全部 dispositions、manifest 身份及 tool postprocessing 已完成。持有 tool receipt 或 blob 本身不能触发 ACK。
6. 现有维护任务每 15 秒扫描有界批次。对原 Invocation 已失败、取消或中断的批次，重用原身份推进 `reserved → sealed → ackEligible → acknowledged`：确认物理关闭、获取 manifest、安全归档、保存录制收尾证据，再发送幂等 ACK。活跃 reserved 批次不被维护任务关闭；单批次失败不阻断本批其他条目。
7. 只有 physical context、所有保留创建/文件准备任务以及私有 batch **三者均明确不存在**，才允许宿主 `not_created` 证明将 reserved 置 acknowledged；此路径没有消费现存文件。超时、权限错误、未知传输结果不能推导不存在。
8. 超额文件只有明确遗漏已成为权威 evidence 且 postprocessing 完成后才可消费。未 ACK 的资源元数据阻止会话级级联删除。24 小时仅产生孤儿候选；Rust 必须先确认数据库没有保护引用，Python 再核对年龄及活动所属关系后才删除。

取消/失败路径的录制收尾使用专用 DB 事务生成 `browser_recording_finalization` machine evidence，结论固定为 `inconclusive`，明确关联原 resource、Invocation、Run、Session、manifest 和实际 blob；不绑定为原 Invocation 的成功证据。事务写入独立收尾证明，ACK 仅在该证明或原成功路径的 postprocessing 门控通过后允许。原工具结果及原 postprocessing 不被改写。外部传入一份同名 metadata 不能替代事务证明。

该专用 bundle 的 `producer_invocation_id` 和 `run_id` 字段为空，来源保存在 DB 自行构造、ACK 精确核验的 `source_identity` 内；它不是成功 Invocation 的验证结果。后续[会话证据删除修复](evidence-session-deletion-2026-10-08.md)将普通成功证据指向可删除执行记录的三个外键改为写入时校验的逻辑来源 ID，保留不可变证据及原始身份。录制未完成消费时仍阻止删除；成功 ACK 只解除录制门控，活跃执行、合并占用等其他检查继续生效。

恢复的范围是有来源的清理、归档和 ACK，不重跑任何旅程动作。无法取得真实 manifest、归档或证据保存失败的批次继续受保护，并占用有界容量；不得通过年龄清理把这些失败伪装为完成。ACK 失败只重试消费，数据库重开后仍使用原有身份。

## 验证入口

- Python：`test_browser_service_lifecycle.py`、`test_real_browser_service.py`、`test_browser_recordings.py`、`test_browser_resource_native.py`，以及既有 Journey/HTTP/临时证据回归。
- DB：`browser_recordings` 模块验证真实 evidence/postprocessing 门控、CAS、删除门控与 not_created 所属关系。
- Rust 原生：`cargo test -p zk-server --test verify_journey_native -- --ignored --nocapture`。普通录制经过真实 ExecutionSupervisor、SQLite、Python UDS、Chromium、证据保存及 ACK；测试自身 fork 隔离 spool，不读取用户录制。普通 Session 两轮 scope 保留页面状态，随后显式 Session 清理。原生 fixture 还覆盖 sidecar 停止导致 ACK 失败、重启 sidecar 与重开 SQLite 后只消费已有批次；实际 Journey 请求计数和 evidence 数量必须保持不变。密码页从本地 HTTP fixture 提供，检查 Rust 工具结果、Replay、临时证据 JSON、SQLite/WAL 和 sidecar 日志没有 canary，同时普通表单值仍可读取。
- Rust 安全读取、Replay 丢图原因及文件名回归由相应模块单元测试覆盖。实际运行结果以本次最终门禁记录为准；本文不把未运行项算作通过。
