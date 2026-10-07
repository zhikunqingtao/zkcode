# F1–F5 与账本回归定位表

> **后续范围变更（2026-10-07）：** 用户明确删除简洁工作台，仅保留开发工作台，不处理简洁模式历史数据。最新前端验证为 145 文件／1289 项及 9 项真实后端 E2E；见 [单一工作台记录](development-workbench-only.md)。本页此前 F1–F5 全量计数与源码身份属于该次冻结，不能代替后续前端验证。


本文供审阅者定位本轮修复的生产入口、关键反例和复现命令。它记录测试所能证明的范围，不代替冻结源码上的完整发布门禁，也不作可提交结论。实际运行、失败、跳过、源码及构建身份以 [修复说明](f1-f5-fixes.md) 和 [门禁记录](full-alignment-gates.json) 为准；下文列出测试不等于宣称本次已运行。

本轮专项使用临时目录、独立 SQLite 和本地 fixture，不调用付费供应商，不使用用户正在运行的服务。`ProviderEvent` 事件 fixture、Router 进程内请求、真实 loopback HTTP/WS、真实浏览器是不同验证层级，下文分别标明。

## F1：Hook 权限、启动和清理责任

**生产入口。** [宿主端口](../../crates/zk-engine/src/hook/admission.rs) 的 `HookAdmission` / `HookStartPermit` 接入 [HostHookAdmission](../../crates/zk-server/src/hook_admission.rs)，复用 [授权服务](../../crates/zk-authz/src/service.rs)、[执行准入](../../crates/zk-authz/src/gateway.rs) 及原有 grants。模型工具不能构造宿主 `hook-v1` 身份。命令和 HTTP 分别由 [HookService](../../crates/zk-engine/src/hook/service.rs) 和 [HTTP 执行器](../../crates/zk-engine/src/hook/http_executor.rs) 在实际启动前复查。删除/手动压缩入口位于 [session_hooks](../../crates/zk-server/src/api/session_hooks.rs)，其所有权使用 [TaskRuntime 宿主任务](../../crates/zk-engine/src/task/external_root.rs)。

| 关键约束 | 测试定位与断言重点 | 验证层级 |
| --- | --- | --- |
| PLAN 不可被旧授权绕过；五种模式；命令与 HTTP | [hook_authorization.rs](../../crates/zk-authz/tests/hook_authorization.rs)：`hook_mode_matrix_for_command_and_http`、`hook_grant_exact_identity_plan_override_and_revocation` | 生产授权服务、独立 SQLite；不等同于每种模式都实际发 HTTP |
| Bash 批准、假冒 Hook 名称无宿主特权；硬性禁止先于自动批准 | 同文件：`model_hook_name_cannot_select_host_analyzer_and_bash_grant_cannot_authorize_hook`、`absolute_command_denial_precedes_auto_approval` | 授权事实和准入边界 |
| 精确批准、语义变化、撤销、单次消耗和启动前竞态 | [hook_host_admission.rs](../../crates/zk-server/tests/hook_host_admission.rs)：`session_grant_reuses_only_unchanged_semantics_and_revocation_blocks`、`config_change_while_waiting_never_executes_approved_old_command`、`once_permission_is_not_reused_and_async_hook_retains_owned_cleanup`、`revocation_after_resource_bind_blocks_physical_command_start`、`plan_mode_after_resource_bind_blocks_physical_command_start` | 真实临时工作区、命令及 SQLite；资源绑定阶段通过 SQL 触发器注入撤权/模式变化 |
| 必要 security Hook 与可选 Hook 分流；审计失败不启动 | 同文件：`plan_and_dont_ask_skip_optional_hook_but_required_security_denies`、`failed_admission_audit_never_starts_command_and_hook_does_not_count_as_model_tool` | 宿主授权到真实命令启动链 |
| PRE 改写后仍需按实际输入授权；外部 MCP 能力上限不能扩大 Hook 权限 | [hook_admission_pipeline.rs](../../crates/zk-server/tests/hook_admission_pipeline.rs)：`transformed_input_is_readmitted_before_real_tool_execution`；[mcp_external_capabilities.rs](../../crates/zk-server/tests/mcp_external_capabilities.rs)：`write_only_mcp_cannot_turn_workspace_hooks_into_process_or_http_authority`、`preconfigured_http_hook_requires_external_network_approval_before_dispatch` | 真实工具/命令；HTTP 案例通过 loopback 监听验证被拒 Hook 没有发起连接 |
| POST 不抹掉失败；首次/续接 SessionStart、异步清理、期限与安全拒绝 | [session_hook_ownership.rs](../../crates/zk-engine/tests/session_hook_ownership.rs)：`first_root_defers_session_start_but_resume_does_not_repeat_it`、`cancelling_blocking_session_start_stops_owned_group_before_any_model_call`、`asynchronous_end_notifications_drain_before_terminal`、`security_session_start_denial_prevents_optional_memory_and_main_requests`、`security_run_end_failure_is_truthful_and_keeps_persisted_answer`；[MCP POST 回归](../../crates/zk-server/tests/mcp_external_capabilities.rs)：`external_post_hooks_preserve_failed_tool_facts_with_or_without_capabilities` | 引擎/TaskRuntime、SQLite、真实受管进程；模型侧使用 fixture |
| 无 Hook 保留既有行为；SessionEnd/PreCompact 拒绝；PostCompact 反映实际保存事实 | [session_hook_lifecycle.rs](../../crates/zk-server/tests/session_hook_lifecycle.rs)：`no_hooks_preserve_compact_and_delete_without_allocating_empty_tasks`、`required_session_end_hook_denial_preserves_session_and_has_clean_owner`、`precompact_security_denial_does_not_save_summary_and_post_error_does_not_undo_save`、`postcompact_is_not_fired_without_a_saved_summary` | 真实 Router、SQLite 和 Hook 进程 |
| HTTP 请求消失、读库失败不能提前释放修改租约；期限与持久权限保持权威 | 同文件：`dropped_rest_request_cancels_owned_process_and_retains_mutation_lease_until_cleanup`、`sqlite_read_failure_retains_cleanup_owner_and_session_lease_until_recovery`、`lifecycle_uses_stricter_root_deadline_and_preserves_timeout_after_cleanup`、`rest_created_persistent_session_observes_authoritative_permission_changes` | 前者取消 Router 请求 Future，非 TCP 断网；读库故障使用真实文件 SQLite 的 TEMP VIEW/UDF 注入，恢复后核对终态和租约 |

**边界。** 精确批准声明不是其全部脚本依赖的沙箱。上述专项不是“所有 Hook 事件 × 所有模式 × 同步/异步 × 命令/HTTP”的笛卡尔积穷举；启动前撤权的 SQL 触发器案例集中在命令路径。HTTP 上限与 POST 原始事实有独立案例，不能借此扩大为所有网络失败及重定向时序都已验证。

## F2：Skill 的真实来源和有效快照

**生产入口。** [filesystem.rs](../../crates/zk-server/src/skill/filesystem.rs) 的 `SourceRoot` / `ReadAuthority` 将词法缓存路径与实际授权身份分开；[safe_file.rs](../../crates/zk-tools/src/safe_file.rs) 的 `open_bound_directory` 和相对目录描述符访问防止检查后重定向。[loader](../../crates/zk-server/src/skill/loader.rs) 原子发布完整候选或保留最近有效快照；[registry](../../crates/zk-server/src/skill/registry.rs) / [catalog](../../crates/zk-server/src/skill/catalog.rs) 在发现、查询、渲染和执行时共享校验。[REST](../../crates/zk-server/src/api/skill.rs) 与 [Skill 工具](../../crates/zk-server/src/skill/tool.rs) 使用同一来源视图。

| 关键约束 | 测试定位与关键名称 |
| --- | --- |
| 授权内根别名可用；子链接不扩大扫描；扫描后置换不能读到新目标 | [filesystem.rs](../../crates/zk-server/src/skill/filesystem.rs)：`valid_root_alias_is_supported_but_child_symlinks_stay_ignored`、`source_replacement_after_scan_never_reads_the_new_target`、`ancestor_replacement_after_scan_cannot_redirect_fd_walk` |
| PROJECT/PLUGIN 不因另一项目已登记而获得权限；首次接纳不能重新授权被换掉的根 | [catalog.rs](../../crates/zk-server/src/skill/catalog.rs)：`project_root_aliases_cannot_import_another_registered_project`、`ancestor_and_plugin_root_aliases_follow_the_same_project_authority`、`persisted_workspace_rebound_before_first_lookup_is_rejected` |
| 根变文件或循环链接应撤销旧快照；原身份暂失/恢复保留正常回退 | 同文件：`persisted_workspace_file_replacement_revokes_cached_skill`、`persisted_workspace_symlink_loop_revokes_cached_skill`、`persisted_workspace_temporarily_missing_preserves_verified_snapshot` |
| USER/MANAGED 明确来源；旧路径、优先级、热重载和全局禁用保留 | [filesystem.rs](../../crates/zk-server/src/skill/filesystem.rs)：`configured_external_source_is_pinned_and_missing_roots_can_appear_later`；[loader.rs](../../crates/zk-server/src/skill/loader.rs)：`load_and_register_applies_source_priority`、`compatible_paths_reload_deterministically_and_keep_global_disable`、`failed_candidate_read_retains_last_valid_registry_and_retries`；[catalog.rs](../../crates/zk-server/src/skill/catalog.rs)：`watcher_updates_active_view_without_another_request_and_preserves_disabled_state` |
| 源失效后发现和模型调用不能使用旧快照 | [catalog.rs](../../crates/zk-server/src/skill/catalog.rs)：`revoked_skill_source_disappears_from_discovery_and_model_invocation` |
| REST 项目隔离、源别名和全局开关 | [skill_api.rs](../../crates/zk-server/tests/skill_api.rs)：`rest_skill_sources_cannot_escape_through_root_aliases`、`project_scope_rest_isolation_global_switches_and_unknown_scope_are_authoritative` |
| WS 项目隔离和共享禁用状态 | [skill_ws.rs](../../crates/zk-server/tests/skill_ws.rs)：`project_aliases_do_not_cross_ws_sessions_and_global_disable_applies_to_both` |

**验证层级与边界。** 文件系统测试使用真实临时目录、链接、重命名和目录描述符；置换点由测试控制，不是穷举全部操作系统调度。REST 使用 Router/SQLite，WS 使用真实 loopback WebSocket，但执行接收端为 `RecordingEngine`。来源失效由全入口共享的 registry/SkillView 校验保障；已有独立模型发现/调用撤销回归，**WS 未另列“加载后撤销来源”用例**，不能将 WS 隔离用例表述为该撤销专项。

读取暂时失败与结构性授权失效分别处理。整个已绑定根被替换后不自动重新授权；需重新建立有效来源身份。这是保留安全边界的行为，不等于禁止授权内合法别名或普通文件热更新。

## F3：草稿只随明确的新建操作转移

**生产入口。** [App.tsx](../../frontend/src/App.tsx) 捕获明确创建操作及原 `draft.id`；[sessionActivation.ts](../../frontend/src/services/sessionActivation.ts) 关联 activation generation；[dispatch.ts](../../frontend/src/api/dispatch.ts) 在匹配 requestId/epoch 的恢复提交点执行转移；[promptDraftStore.ts](../../frontend/src/store/promptDraftStore.ts) 比较源身份与目标占用；[usePromptDraftKey.ts](../../frontend/src/components/input/PromptInput/usePromptDraftKey.ts) 不再从普通会话变化推断首页转移。

| 关键约束 | 测试定位与关键名称 |
| --- | --- |
| 目标已有草稿、源换代、重复提交和异步所有权 | [promptDraftStore.test.ts](../../frontend/src/store/promptDraftStore.test.ts)：`keeps both drafts if a confirmed target already owns another draft`、`does not move a replacement home draft using an old creation identity`、`keeps an operation attached to its draft through migration and later fallback reuse` |
| 匹配提交点先转移再展示；普通恢复不迁移；失败/迟到创建不夺回选择 | [sessionActivation.test.ts](../../frontend/src/services/sessionActivation.test.ts)：`commits a captured new-session draft before authoritative session publication`、`ordinary restore preserves independent home and existing-session drafts`、`failed and superseded new binds cannot transfer the captured home draft`、`discarded new-session activation cannot commit a late restore after returning home` |
| App 的首页发送/显式新建捕获身份；创建期间又选首页；取消项目选择 | [App.review-command.test.tsx](../../frontend/src/App.review-command.test.tsx)：`captures the home draft before asynchronous creation: %s`、`ignores a created session after a newer home selection: %s`、`cancelled project selection preserves the home draft and never requests a bind` |
| 图片/本地附件、移动卸载重挂、发送被拒仍保留草稿 | [promptDraftPersistence.test.tsx](../../frontend/src/components/input/PromptInput/promptDraftPersistence.test.tsx)：`delivers a pending image to its confirmed new draft after switching elsewhere`、`keeps each session's local file references when switching sessions`、`restores the draft text after an unmount/remount cycle (mobile tab switch)`、`keeps the draft when a mid-submit session creation ends in a rejected send` |
| 桌面/移动导航与两工作台选择历史会话均不搬走首页草稿 | [Sidebar.drafts.test.tsx](../../frontend/src/components/layout/Sidebar.drafts.test.tsx)：`%s %s history navigation binds without transferring home drafts`，参数为 desktop/mobile × development/simple |

**验证层级与边界。** 上述为真实 Store/恢复调度和 React 组件测试，HTTP/STOMP 传输用 mock。导航矩阵不是浏览器触摸设备 E2E；图片异步测试也不代表每种操作系统文件选择器都已验证。

[production-backend.spec.ts](../../frontend/e2e/production-backend.spec.ts) 的 `real backend: browser chat is durable before message_complete` 使用真实 Chrome → Rust HTTP/WS → SQLite 和本地模型协议服务，验证既有会话发送、终态持久化及刷新恢复。它**不覆盖首页草稿迁移的完整失败/迟到/附件矩阵**，不能用真实聊天 E2E 替代这些组件反例，也不能反过来把组件反例称为真实后端 E2E。旧 `prompt-draft-first-session.spec.ts` 的模拟协议及 OSS 案例不作为本轮真实后端或排除能力的验收证据。

## F4：复杂度分析输入与缓存指纹一致

**生产入口。** [complexity_analyzer.py](../../python-service/src/services/complexity_analyzer.py) 的 `complexity_files` 同时供分析和 `complexity_fingerprint` 使用；[analysis_jobs.py](../../python-service/src/analysis_jobs.py) 的 worker 在任务前后计算同一指纹并控制缓存，保留 owner 隔离；[code_quality.py](../../python-service/src/routers/code_quality.py) 提供实际路由。

主要定位：[test_complexity_workers.py](../../python-service/tests/test_complexity_workers.py)。

- `test_real_worker_metrics_cache_and_changed_bytes`：FastAPI 路由到真实多进程 worker，验证 Python 结果、未修改命中、修改失效及读取失败如实返回。
- `test_javascript_content_changes_invalidate_real_worker_cache`：真实路由/worker，覆盖 **JS/JSX × 缺省/显式 javascript** 的组合；各组验证同长度且保留 mtime 的修改、缓存命中和 owner 隔离。
- `test_real_worker_cache_tracks_target_add_delete_and_rename`：真实路由/worker 验证目标路径的结果/缓存隔离、目标外修改不误失效、重命名/新增/删除实际失效，并核对返回文件名和数量。
- `test_complexity_selection_and_hash_share_languages_target_filters_and_limit`：直接验证语言、目标、忽略目录、链接、显式文件与上限共享选择；不参与分析的文件变化不误失效；删除影响截断状态会改变指纹。
- `test_complexity_worker_rejects_mutation_during_analysis_without_caching`：真实 `spawn` 子进程、双向 Pipe、worker 和分析器；子进程内测试包装器在实际分析后确定性修改文件，验证前后版本不一致返回 409、不发布结果/缓存且进程正常退出。修改时机是受控的，不是随机调度压力测试。
- `test_global_file_limit_is_truthful_and_does_not_reset_in_each_directory`、`test_complexity_cancelled_before_dispatch_never_starts_a_worker`：全局文件上限和启动前取消。

[analysis-backend.spec.ts](../../frontend/e2e/analysis-backend.spec.ts) 的 `complexity and Git panels use real authorized parsers and native Git` 另行验证真实 React → Rust → Python UDS 的复杂度页面，当前 fixture 为 Python 文件；它不证明所有语言的缓存失效。

**边界。** 前后指纹竞争通过真实子进程中的确定性变更验证，不是文件系统全部并发调度的穷举；真实浏览器案例仍仅使用 Python fixture。复杂度始终标记为启发式指标，不是代码正确或测试通过证据。

## F5：停止原因、期限与权威终态

**生产入口。** [engine.rs](../../crates/zk-engine/src/engine.rs) 的 `RunStopCause`、`ConversationCancellation`、`DeadlineTaskGuard` 竞争同一个停止事实；Query 的 [PreparedQuery](../../crates/zk-server/src/api/query.rs) 传入同一绝对期限。`interrupt_handle` 先传播停止再发 ACK；`commit_run` / `committed_stop_reason` 与 [conversation_service.rs](../../crates/zk-engine/src/conversation_service.rs) 从已提交 Run 投影终态。附属任务隔离与失败对账复用 [cancellation.rs](../../crates/zk-engine/src/task/cancellation.rs) / [runtime.rs](../../crates/zk-engine/src/task/runtime.rs)。

| 验证层级 | 测试与实际证明 |
| --- | --- |
| 内部确定性时序 | [engine.rs](../../crates/zk-engine/src/engine.rs) 的 `early_cancellation_tests`：`query_and_root_deadlines_preserve_the_same_winning_cause` 控制两保护入口先后；`an_explicit_user_stop_is_not_relabelled_by_a_later_deadline`、`cancellation_before_run_creation_is_synchronous_and_scoped_to_lease` 验证首因和旧 lease 隔离；`backpressured_interrupt_ack_cannot_delay_stop_propagation` 验证 ACK 背压。这里用记录型取消端口，不冒充真实 REST/SQLite 故障 |
| TaskRuntime 树和保存失败 | [runtime.rs](../../crates/zk-engine/src/task/runtime.rs)：`failed_cancellation_write_stops_owned_subtree_and_fences_new_work`、`deadline_stops_locally_and_reconciles_intent_before_terminal_result` 验证 root/attached 阻断、detached 隔离、禁止新工作与恢复对账；保存故障是事务前的受控 failpoint，不能称为磁盘损坏或真实 SQLite 写入 I/O 故障 |
| 真实 HTTP/SSE、引擎与 SQLite | [query_stream.rs](../../crates/zk-server/tests/query_stream.rs)：`query_deadline_is_timeout_in_rest_sse_and_sqlite`、`deadline_during_run_end_hook_projects_durable_timeout_everywhere`、`completed_query_is_not_rewritten_by_a_late_deadline`、`explicit_request_stop_has_error_terminal_and_does_not_replay` |

其中 RunEnd 边界在模型答复后仍有真实 Hook 进程存活时触发期限，核对 SSE `assistant_message`/最终结果和数据库，不修改已经保存的原始模型答复事实。

**边界。** 两计时器的精确先后在内部测试控制；真实 REST/SSE 测试证明端到端原因一致，没有强制穷举每一种调度。当前没有将“实际 REST 超时 + SQLite 写故障 + 全部 attached/detached 组合”合在同一个用例中。该分层覆盖不能描述为全部组合已通过。

## 账本：可结算才重试，未知费用保持阻断

**生产入口。** [llm_summarizer.rs](../../crates/zk-engine/src/llm_summarizer.rs) 的摘要请求/重试经 [llm_ledger.rs](../../crates/zk-engine/src/llm_ledger.rs) 中的 `DbLlmCallObserver` / `DbSummaryObserverFactory`；[ProviderRegistry](../../crates/zk-llm/src/registry.rs)、[运行账本](../../crates/zk-db/src/runtime_ledger.rs) 和 [任务预算](../../crates/zk-db/src/task_budget.rs) 负责物理请求准入、结算与未知 usage 门禁。

测试集中在 [llm_summarizer_ledger_tests.rs](../../crates/zk-engine/src/llm_summarizer_ledger_tests.rs)，普通请求和摘要均使用生产 observer、真实 SQLite、带期限和 token/金额上限的 Task/Run。

| 场景 | 关键测试 | fixture 的真实边界 |
| --- | --- | --- |
| 普通请求未知 usage 的 HTTP 429 | `real_http_ordinary_429_stops_before_second_request_and_keeps_null_cost` | 真实 loopback HTTP、生产 OpenAI 兼容适配器，`deepseek` 配置、虚拟 Key；核对 HTTP 计数停在一次、SQLite 费用仍 NULL、下一请求在网络调用前被拒绝 |
| 摘要收到 HTTP 429 后全压缩链本地回退，主调用仍受阻 | `real_http_summary_429_falls_back_locally_and_blocks_same_run_conversation` | 同一真实 HTTP 层；执行 `compact_messages_scoped`，保留原用户文本，纯本地摘要不会修复未知费用；同 Run 普通调用没有新增 HTTP 请求 |
| 已知 usage 失败可结算并允许一次摘要重试 | `real_ledger_known_usage_allows_one_summary_retry_and_accounts_both_calls`、`real_ledger_summary_retries_at_most_once_even_when_both_failures_are_settled` | 直接注入 `ProviderEvent` 的本地协议 fixture；不经过网络，不声称远端 HTTP 429 一定返回可结算 usage |
| 普通请求失败和成功分别计费；未知费用不冒充零 | `real_ledger_ordinary_settled_failure_and_success_charge_both_requests`、`real_ledger_ordinary_unknown_429_refuses_retry_without_inventing_zero_cost`、`real_ledger_unknown_429_refuses_a_second_physical_summary_request` | 事件 fixture + 真实 DB observer；检查活跃 Run 与终态 Task 的各自用量、预算拒绝及幂等终态不双记 |
| 保存失败不能伪造请求或成功；摘要可纯本地回退 | `real_ledger_ordinary_save_failures_never_publish_a_successful_finish`、`real_ledger_finish_failure_cannot_be_presented_as_a_successful_summary`、`real_ledger_start_failure_prevents_provider_dispatch_and_keeps_local_fallback`、`real_ledger_compaction_falls_back_locally_but_unknown_usage_still_blocks_payment` | SQLite 触发器拒绝请求开始/结束保存；区分未发请求、已发但保存失败、纯本地回退与后续付费阻断 |

**边界。** 本轮不调用真实付费供应商。已知 usage 的重试是生产 observer 集成证据，但不是某个供应商异常响应格式的网络兼容性证明。上述测试也不独立证明压缩 CAS 落库的全部竞争、进程崩溃后的 WAL 恢复或真实磁盘写满。`BUDGET_USAGE_INCOMPLETE` 保持生效；本地回退不等于整个聊天已恢复为可继续付费。

## 针对性复现

以下从仓库根目录执行；依赖和本机运行器需已按项目既有安装流程准备。可先按缺陷选择对应命令，再使用发布门禁执行全量。命令不连接用户服务，也不请求付费模型。

```sh
# F1：授权事实、宿主接线、生命周期、外部能力边界
cargo test -p zk-authz --test hook_authorization --locked
cargo test -p zk-server --test hook_host_admission --test hook_admission_pipeline --test session_hook_lifecycle --test mcp_external_capabilities --locked
cargo test -p zk-engine --test session_hook_ownership --locked

# F2：文件/来源状态、REST、真实 WS 传输
cargo test -p zk-server --lib skill:: --locked
cargo test -p zk-server --test skill_api --test skill_ws --locked

# F3：Store、绑定提交、App、新旧工作台导航、附件归属
(cd frontend && npm run test:run -- src/store/promptDraftStore.test.ts src/services/sessionActivation.test.ts src/App.review-command.test.tsx src/components/layout/Sidebar.drafts.test.tsx src/components/input/PromptInput/promptDraftPersistence.test.tsx)

# F4：实际 Python worker 与共享指纹
(cd python-service && .venv/bin/python -m pytest tests/test_complexity_workers.py)

# F5：确定性内部时序、TaskRuntime 失败对账、真实 HTTP/SSE
cargo test -p zk-engine --lib early_cancellation_tests --locked
cargo test -p zk-engine --lib failed_cancellation_write_stops_owned_subtree_and_fences_new_work --locked
cargo test -p zk-engine --lib deadline_stops_locally_and_reconciles_intent_before_terminal_result --locked
cargo test -p zk-server --test query_stream --locked

# 普通请求与摘要的真实 SQLite 账本；含 loopback HTTP 429
cargo test -p zk-engine --lib llm_summarizer::ledger_tests --locked
```

真实浏览器回归分别使用 [生产后端配置](../../frontend/playwright.production.config.ts) 和 [分析后端配置](../../frontend/playwright.analysis.config.ts)，不要把默认 `test:e2e` 的模拟后端用例替代它们。先针对同一冻结源码构建 Rust，显式传入该二进制；启动脚本使用独立目录/数据库、检查端口占用且不复用既有服务，并记录二进制身份。

```sh
cargo build -p zk-server --locked
(cd frontend && ZK_E2E_SERVER_BINARY="$PWD/../target/debug/zk-server" npm run test:e2e:production)
(cd frontend && ZK_E2E_SERVER_BINARY="$PWD/../target/debug/zk-server" npm run test:e2e:analysis)
```

这两组 E2E 的实际范围见前文 F3/F4；完整 Cargo/前端/Python/契约/依赖/安装和原生门禁的运行记录由发布门禁统一维护，不在此复制旧通过数。
