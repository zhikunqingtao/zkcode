# 产物、修改行与维护实施记录

以下记录本轮生产接线与验收。思考参数修复后冻结工作区已通过 145 个独立 Rust harness / 3143 项（0 失败）；Python 335 项通过，覆盖率 75.83%；普通和临时真实浏览器/HTTP 原生专项另行显式 2 项通过。对应日志及内容摘要见 [统一门禁](full-alignment-gates.json)。下文早期 225/394/332 等计数保留为阶段历史；最终真实 release 页面 E2E 独立记录，不用阶段单测替代。

最终 release 的 SHA-256 为 `cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`。在该产物上重新执行产品页面 6 项、代码分析及 Git 页面 3 项，全部通过，两套执行前后摘要一致。真实浏览器链路、原生专项与单测各自独立记录，不重复累计。单次真实 DeepSeek Flash Query 验证了相同产物的实时 SSE、完整用量及费用账本，详见[脱敏报告](deepseek-flash-release-probe.json)；它不替代产物文件或浏览器证据验证。

## 终态产物完整性

TaskRuntime 在提交不可变终态后、发布通知前调用同一个只读 observer。重启维护扫描负责补偿未完成检查，不重跑用户命令、浏览器旅程或外部 Hook。

- `zk-db/src/artifact_terminal.rs` 只接受真正终态且清理已经确认的 Run。完整 manifest 采用 CAS；成功验证和依赖它的验收投影原子更新，原始路径、类型、大小、内容摘要及 TaskResult 不改写。
- `zk-server/src/artifact_integrity.rs` 通过目录描述符逐级拒绝 symlink，校验文件身份、类型、大小与摘要；检查前后再次比对 inode、设备、时间和大小。八秒及 1 GiB 上限，超限或无法读取都如实失败。
- 删除仅在实际不存在时验证成功；权限、I/O、悬空 symlink 等均不能当作不存在。临时证据失去 RAM 所属范围后标记 unavailable，不伪造空内容或摘要。
- `artifact_terminal_checks` 记录无正文检查收据。重复终态不再次检查或覆盖既有收据；维护仅补偿尚未完成的记录。

实际验证：DB 四项包括并发 CAS、清理未确认拒绝、内容过期、真正关闭/重开数据库后的补偿及不可变结果；`/tmp/zk-db-alignment-all-targets3.txt`。真实服务端终态 observer 一项和 artifact REST 三项通过；`/tmp/zk-server-alignment-combined3.txt`。声明 Bash 产物也通过实际生产 observer，未声明文件不会进入 manifest，重复通知不执行脚本。

## 修改行与辅助影响分析

GitDiff 继续使用受监管的只读 Git 进程，固定禁用 external diff/textconv。先从真实完整 stdout 提取新增/删除行、删除锚点、二进制和截断信息，再裁剪展示。引号和非 ASCII 文件名按 Git 实际格式解码；最多 1,000 个文件和 20,000 个行位置，截断明确可见。

`POST /api/analysis/change-impact` 从已授权 Session/Project 推导根目录；请求路径只作一致性检查。Python 复用隔离分析 worker、取消与实际进程回收，以文件内容版本为缓存身份。Rust 校验返回路径、节点、边和权重，拒绝越界或把辅助结果宣称为测试通过的响应。Python/Java/TypeScript/JavaScript 调用图依其真实分析器工作；Rust 语义查询使用 LSP，这里明确返回不支持。

实际验证：原生 Git 回归发现真实 Git 路径头结尾的 TAB 分隔符，修复后工具库 394 项通过（`/tmp/zk-tools-alignment-all-lib.txt`）。Python 全套 332 项通过，其中包含影响分析缓存、取消和边界测试（`/tmp/zk-python-alignment-full.txt`）。Rust API 四项通过。真实 React → Rust → UDS → Python 浏览器两项通过，非空函数关系在页面展示并明确标为辅助分析，不能替代测试证据（`/tmp/zk-analysis-browser-final3.txt`）。

## 有界维护与恢复游标

复用单一后台维护任务，每 15 分钟分批清理；一次最多八批、每批 250 项。仅清理无引用且超过 24 小时的 checkpoint、超过 30 天的已解决异常和可裁剪的终态展示增量。费用、证据、未消费结果及恢复引用仍保留；不删除用户生成的正常文件。

真正裁剪事件时才写入恢复下界，过期游标返回明确的重新获取快照错误。新 Run 没有裁剪标记时仍接受原 `-1` 初始游标。DB 两项维护专项在完整数据库 225 项中通过；先前错误地将缺失下界当作零而拒绝 `-1`，已修复并保留回归断言。
