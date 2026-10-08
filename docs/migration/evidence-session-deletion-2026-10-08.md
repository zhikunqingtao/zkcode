# 含证据会话删除修复（2026-10-08）

## 范围与数据边界

修复此前已复现的 zkcode 基线缺陷：完成的会话包含机器证据时，删除会因证据外键及不可变约束失败。沿用当前分支及本地改动，不提交、不推送、不重启现有服务。

用户已确认：**zkcode 不需要历史数据兼容，本次只修新库基线；zhikuncode 的历史会话数据必须正确完整保留。** 本次不修改 zhikuncode，也不读取、升级或重建任何现有业务数据库。

zkcode 使用单份 `greenfieldOnly` 基线及 refinery checksum。本次基线变化后，旧数据库会拒绝由新版启动，不会自动升级或删除。切换新版须先备份、再按既有流程新建 zkcode 数据库，同时保留配置、密钥及正常用户文件；此部署操作未在本次执行。

## 根因及最小修复

证据表的 Session 归属原本是逻辑引用，证据包、条目及结论历史禁止改写和删除，需要保留原始来源。三个物理外键却把证据绑定到可随 Session 删除的执行记录：两处 Invocation `RESTRICT` 阻止删除，一处 Run `SET NULL` 触发不可变保护。这两种约束相互冲突。

- 将三处来源引用改为不可变的逻辑身份；会话删除后保留原来的 Session、Run、Invocation ID、内容、摘要和结论历史。
- 保留写入时成功 Invocation、所属 Run、所属 Session 及条目来源校验；增加非空 Run 必须存在且属于证据 Session 的 INSERT 校验，覆盖无 producer 的证据。没有关闭外键检查或不可变触发器。
- 证据读取先检查所属会话，删除后的旧链接返回会话不存在，不再把内容解码中的缺失会话包装为 HTTP 500。保留临时正文限制和跨会话访问检查。
- 文件产物的来源外键、录制 ACK、活跃执行、合并占用等删除保护不变；不删除工作区文件或 Blob，不更改费用与执行生命周期。
- 失败录制模块仅更正一处过时注释。它仍使用由数据库构造的 `source_identity`，不会伪装成成功工具证据。

本轮相较上次验收只改变四个源码/测试文件：建表 SQL、`zk-db/src/evidence.rs`（含回归）、`zk-db/src/browser_recordings.rs`（仅注释）、`zk-server/tests/evidence_api.rs`。没有新增依赖或修改锁文件。

## 回归与验证记录

新增六项 DB 回归、两项真实 Router＋SQLite 回归，覆盖：

1. `verified`／`failed` 机器证据、多条目及不同成功 producer；删除后原始 SQL 字段逐值不变，执行记录正常级联，仍拒绝篡改和删除证据。
2. 父会话删除包含成功证据的子会话；其他会话仍存在。
3. 无 producer 的 Run 关联证据正常删除；缺失或跨 Session 的 Run 来源拒绝写入。
4. 删除后的证据读取返回类型化 `SessionNotFound`；HTTP 元数据、审核、列表及 Blob 访问返回 403／404，其他会话正常访问不受损。
5. 合并捕获前来源仍受占用保护；封存、发布、删除来源后，目标 Handoff 仍可读取封存证据，其他 Session 无权读取。
6. 显式会话删除不移除用户文件或工作区 Blob。

初始红测、测试 fixture 修正及之后的验证日志保存在 `target/evidence-delete-fix/`。fixture 修正包括完成 Task 的正常生产路径、子任务真实身份与预算、按捕获阶段验证合并占用，以及接受不同不可变/来源 trigger 的正确拒绝结果；没有为通过测试放松生产约束。

本轮新增八项回归已通过，最终源码冻结后完成以下验证。不同批次重叠的测试不相加；子进程输出也不重复计数。

| 检查 | 本轮实际结果 |
|---|---|
| 证据 DB 专项 | 11 通过，含新增 6 项 |
| 真实 Router＋SQLite Evidence API | 7 通过，含新增 2 项 |
| Cargo 格式 / Clippy | workspace / all-targets / all-features、`-D warnings` 通过 |
| Cargo workspace 测试 | 3,299 通过，10 ignored；包含真实 Git 回归 |
| 真实 Chromium / Python UDS / HTTP / 临时录制 | 3 个原生场景显式执行通过，不重放浏览器副作用 |
| Cargo workspace release 构建 | 通过，独立产物目录 |
| 新 release 后端页面 E2E | production 10、analysis 3，通过；两次运行前后构建摘要相同 |
| 差异及运行中环境 | `git diff --check` 通过；旧后端二进制、LSP 配置和 dev-state 摘要未变 |

首次 workspace 执行有一项既有 Git 后置 Hook 的 4 秒等待超时。未修改该测试、超时设置或相关生产逻辑，原用例独立复跑及整套 workspace 复跑均通过。首次失败记录保留于 `workspace-first-attempt.log`，独立复跑为 `git-hook-rerun.log`；不隐藏时间敏感用例的不稳定表现。

默认忽略的 10 项中，本轮显式执行了 3 个浏览器场景；5 个真实供应商/搜索测试及 2 个与本次改动无关的原生 LSP/Keychain 测试没有重跑。前端/Python 全套单测、Office、依赖审计及 doctor 未重复执行，原阶段记录保留但不计为本轮执行结果。未调用付费供应商。

- 源码 SHA-256：`a2dfb106e543859039acc26ede9cfb97935ab916ade92ab68b1130272e319bc2`，1,372 个 tracked/未忽略文件，排除 `docs/`，冻结后未变。
- 新版后端：`target/evidence-delete-build/release/zk-server`；SHA-256：`9be53c8f103586deeb81fb69743b4ccb5ae7bbe3247004370e2587e97fa04d18`。
- 最终机器台账：[necessary-fixes-gates.json](necessary-fixes-gates.json)。此前完整台账保存在 `historicalValidationBeforeEvidenceDeletion`，不覆盖历史结果。
- 验证结束后删除本轮专用的 `target/evidence-delete-build/debug`，实际释放约 12.6 GiB；新版 release 与全部日志保留，原 `target/debug/zk-server` 未动。

结论：此遗留缺陷在新版基线上已修复并通过相应验收。本轮未操作真实数据库、未部署到原服务、未提交或推送；这不构成整个仓库绝无其他缺陷的保证。

## 可复现命令

```sh
export CARGO_TARGET_DIR="$PWD/target/evidence-delete-build"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
cargo test -p zk-db --locked
cargo test -p zk-server --test evidence_api --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
ZK_RUN_GIT_TESTS=true cargo test --workspace --locked
cargo build --workspace --release --locked
PLAYWRIGHT_BROWSERS_PATH="$PWD/.runtime/playwright" cargo test -p zk-server --test verify_journey_native --locked -- --ignored
ZK_E2E_SERVER_BINARY="$PWD/target/evidence-delete-build/release/zk-server" npm --prefix frontend run test:e2e:production
ZK_E2E_SERVER_BINARY="$PWD/target/evidence-delete-build/release/zk-server" npm --prefix frontend run test:e2e:analysis
```

隔离构建目录保护正在运行的旧 `target/debug/zk-server`，降低调试信息占用。原生浏览器和页面 E2E 使用现有隔离 runner、独立 DB/UDS/端口及本地 fixture；不调用付费供应商。
