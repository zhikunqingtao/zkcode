# 独立审查 R1–R7 修复与验证

本记录对应 R1–R7 验收时的源码。此后用户另行授权修复成功证据会话删除缺陷，并确认调整 zkcode 新库基线；新版范围和验证见[会话证据删除修复](evidence-session-deletion-2026-10-08.md)。以下“不改 schema”等描述及测试数量是该阶段历史事实。

基线仍为 `298d2b30b3d310420c12d953bb4382ec8064d1bb`，在既有本地修改上增量修复。没有切换分支、提交、推送、创建 PR、重启用户服务、修改数据库 schema 或访问真实用户数据库。未知价格与计费策略不在范围内。

外部审查的七项问题已独立核验；§4.1 的慢清理提示缺口也纳入本次修复。此前门禁结论只代表当时已运行的场景，不能覆盖这些新反例。历史通过与失败记录保留，当前结果单独记录在 [necessary-fixes-gates.json](necessary-fixes-gates.json) 的后续验收中。

与上一次验收源码逐文件比较，本次调整 33 个源码、测试及测试配置文件：17 个生产文件（其中 MCP API 文件仅纠正删除语义注释），16 个独立测试/fixture/测试配置文件；Rust 文件内的回归与对应实现一起计数。另更新本组文档及门禁记录。没有修改依赖锁文件、数据库迁移文件或安装策略。逐文件前后摘要保存为 `target/review-followup/final/followup-source-changes.json`。

## 修复范围

| 项目 | 生产修复 | 必须保留的行为及验证 |
|---|---|---|
| R1 子会话录制删除保护 | 删除事务递归检查全部后代 Session；浏览器 owner close 同样使用后代集合 | 三层父子关系，reserved/sealed/ackEligible 阻断真实删除；ACK 后解除本项门控；正常资源归属检查保持 |
| R2 失败录制收尾 | 现有维护任务重用 resource identity，确认物理关闭、取回 manifest、安全归档、保存来源证明、幂等 ACK | 活跃 reserved 不关闭；取消/超时不改成成功；close 丢响应、seal/blob/DB 失败及重启后收敛；真实浏览器动作不重放 |
| R3 浏览器租约首次失败 | 无 generation 的 slot 仅保留清理责任；原 usage 释放确认后才重新 acquire；Python 动作复核租约并绑定已有物理 context | 首次拒绝、响应丢失、清理未确认、取消与获取同时发生均不能执行无租约动作；合法租约复用及普通 Session 跨轮状态保留 |
| R4 合并取消保存失败 | 停止执行和取消协调责任分别保留；暂态错误只重试取消持久化与来源锁释放；原幂等键受本地停止屏障保护 | 真实 Router/SQLite trigger；无需再次点击即收敛；永久错误不热循环；清理未确认仍可发现、可取消，不能重新收费执行 |
| R5 MCP 删除名称空间 | 明确删除与暂时断开分开；清理确认且代际仍匹配才释放动态配置预约；首次握手也纳入受管 owner | 同规范名称可重新添加；cleanup pending 不释放；旧启动或迟到清理不能影响新代际；刷新和停用保留有效配置 |
| R6 E2E 隔离 | 默认配置排除专用真实后端用例；自动 fixture 在首个测试动作前验证配置、标记和分配地址 | 专用 runner 继续使用独立 DB/配置目录及禁止服务复用；默认入口不接触当前开发服务 |
| R7 取消告警恢复 | 以 Session/Run 保存内存告警；重连、切换再返回保持；手动关闭只隐藏身份，权威结束才退休 | 同 Run 非终态不丢告警；终态及新 Run 恢复清除旧提示；迟到通知不复活；不从普通 cancelling 状态伪造异常 |
| §4.1 慢取消清理提示 | 同一个 Run 的首次停止时钟覆盖整个执行收尾，包括 Hook/scope cleanup | 三秒提示为非终态；Query 结束 HTTP 等待不释放 worker；正常慢请求和已完成取消不产生迟到告警 |

## 证据及范围边界

失败录制专用 evidence 的 verdict 为 `inconclusive`。DB 根据真实终态 Invocation、资源、Session、Run、manifest 及实际归档字节构造来源证明；原工具结果和原 postprocessing 不被改写。ACK 不接受孤立 blob 或任意客户端 metadata。为避免永久不可变证据被 FK 删除动作隐式更新，专用 bundle 的可空关系字段不指向原 Run/Invocation，来源由不可变 `source_identity` 精确保留及核验。

真实原生测试另外暴露并确认一个**基线既有边界**：普通成功 machine evidence 的 `producer_invocation_id ON DELETE RESTRICT`，以及 `run_id ON DELETE SET NULL` 与不可变约束，会阻止部分含成功证据的 Session 删除。HEAD 与当前 schema 的内存对照均可复现。本次没有扩大为修改证据保存和删除策略；“ACK 后可以删除”只适用于本项门控已解除且不存在其他既有阻断的会话，不能泛化为所有会话。证据位于 `target/review-followup/recordings/preexisting-evidence-delete-boundary.log`。

R7 缓存为当前页面的内存状态，不承诺新标签页或页面进程重启后补发历史诊断。浏览器安全语义投影、录制磁盘峰值、单 BLOB、真实供应商和长期 Node/Ruby REPL 的既有验证边界继续适用，不随本次回归数量扩大。

## 验证记录

新增反例先验证失败，再修改实现。生成日志位于 `target/review-followup/`；这些是本地可访问产物，不冒充已上传的 CI 附件。固定命令及日志摘要会写入机器台账，干净 checkout 可按命令重跑。

- DB：`cargo test -p zk-db --lib browser_recordings::tests`。
- 浏览器租约：`cargo test -p zk-server --lib python::tools::browser_session_scope::tests --locked`。
- 录制恢复：`cargo test -p zk-server --lib python::tools::browser_recordings::tests --locked`。
- 原生 Chromium：`cargo test -p zk-server --test verify_journey_native --locked -- --ignored`，使用仓库私有 Playwright 浏览器，UDS、SQLite、spool 和页面均为测试专属。
- 合并：`cargo test -p zk-server --lib api::session_merge::tests --locked`；Engine `runtime_migration` 的 `cancellation_notice` 专项，以及 Query 取消持久化故障回归。
- MCP：`cargo test -p zk-mcp --lib manager::tests:: --locked` 与 connection 生命周期专项。
- React：`npm run test:run`，包含 cancellation recovery、E2E isolation 和 SessionMergePanel。
- Python：`.venv/bin/python -m pytest --cov=src --cov-fail-under=70`。

历史失败没有删除：包括新增红测、未补齐准入接口的旧测试替身、真实终态事件契约/父子 Task fixture 错误、不可变证据 FK 冲突，以及并发原生编译时 50ms 测试清理预算过短。fixture 修正保留原断言目的，生产权限、身份和失败语义没有为了通过测试放宽。

## 最终验收结果

本次授权的 R1–R7 及 §4.1 已修复，适用发布门禁完成。源码冻结后未再修改生产或测试源码；不同批次之间的重复用例不累加为独立总数。

| 检查 | 实际结果 |
|---|---|
| Cargo fmt / Clippy | 全 workspace、all-targets、all-features 通过，Clippy warnings 作为错误 |
| Cargo workspace | 3,291 通过，10 ignored；包含真实 Git 专项 |
| Engine no-default-features | 725 通过，与 workspace 有重叠 |
| 原生 LSP / 浏览器 / Keychain | 五类 LSP 1 组、真实浏览器 3 场景、macOS Keychain 1 项通过 |
| Rust release build | workspace / release / locked 通过 |
| React | lint/build 通过；151 文件、1,334 项单元测试通过 |
| 主题与 Jelly | 20 个主题场景、42 个 Jelly 场景通过 |
| 真实 Rust 页面 E2E | production 10、analysis 3，共 13 通过，0 跳过；含移动端、键盘及无障碍 |
| 默认 E2E 收集隔离 | 261 项只读收集，不含 production/mobile/analysis 专用文件；不声称执行了这 261 项 |
| Python | 373 通过，覆盖率 76.77%；依赖警告保留 |
| 安装边界 / 原生 Office | 11 项 / 41 项通过 |
| 隔离 doctor | deep 37/37 通过；固定 5273 健康检查不作为新后端运行证明 |
| 契约 / 密钥 / 依赖 | 契约通过、gitleaks 无命中、cargo deny 通过、npm audit 零漏洞 |

workspace 默认忽略的 10 项中，5 个本机专项已显式执行；剩余 5 个真实供应商/外部搜索测试按本地 fixture 范围未运行，不计通过。默认过时 E2E 用例未机械全开。初次前端测试有一项既有 Git 面板 5 秒超时，未改源码或放宽期限；该用例单独复跑和随后整套 1,334 项均通过，初次失败日志保留。

- 最终源码 SHA-256：`c4294842c825b694d84f6c77e0f4062757940205e084cd06febe5401be5ad4b9`，共 1,372 个 tracked/未忽略文件（排除 `docs/`）。
- release 后端 SHA-256：`2efadf68c55d7e93f022f2d6fdd3580889a356892eb335295ed44a3527e031cd`；两套 E2E 前后均核对一致。
- 原 `.runtime/lsp/current.json` 与 `.runtime/dev/dev-state.json` 的摘要保持不变；doctor 使用已有隔离候选目录及实际新构建记录。

测试过程中按用户要求清理 debug：在 Cargo 结束后移除可重建依赖、测试产物与缓存，保留正在运行的 `target/debug/zk-server`、release、私有工具链和全部验收日志。释放约 50.3 GiB；debug 从约 51 GiB 降至 226 MiB，可用空间约 60 GiB。后续 E2E 使用 release，没有重建 debug。细节见机器台账的 `authorizedDebugCleanup`。

本次修复具备其授权范围内的提交依据，尚未提交、推送或创建 PR。这是本次跟进修复时的历史结论；证据删除缺陷的后续修复与新版验证以[独立记录](evidence-session-deletion-2026-10-08.md)为准。本文源码及通过数量保留为修复前证据，真实供应商、远端 OAuth 等未执行项目不计完成。
