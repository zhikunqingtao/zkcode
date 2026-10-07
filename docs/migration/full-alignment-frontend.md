# React / Python / Rust 分析链路补齐与最终验收记录

> **2026-10-07 F1–F5 修复更新：** F3 的首页草稿转移只由明确新建操作触发，并在匹配的会话恢复提交点比较身份；F4 的复杂度文件选择和缓存指纹已统一。前端/Python 本轮计数、真实新 release E2E 结果以修复门禁为准。 最新验证见 [修复记录](f1-f5-fixes.md)、[回归定位表](f1-f5-regression-map.md) 和 [门禁](full-alignment-gates.json)。下方较早的通过数量、状态和构建身份保留为历史，不代表本轮验证。

固定源：`zhikuncode 053adf90`；源差异基线 `3e536438`，目标迁移基线 `08cbdc45`。本轮在已有本地改动上增量实现；没有提交、推送或切换分支。本文件只记录本工作线的实际状态，不替代最终发布验收。

## F1–F5 修复前状态索引（历史，2026-10-07）

**后续 Rust adapter 再验证已通过：** 模型 thinking 适配修正后的新 release `cca0d0…` 已完整重跑 **production 6/6、analysis 3/3，均 exit 0**。前后 SHA256 一致；前端／Python 和已修正 fixture 未变。此前 `822ad…` 的结果保留为修正前历史记录，当前结果见文末“模型适配修正后的最终重验”。

下文按实施先后保留真实历史，旧章节中的“待实现／待复跑／最新”只指当时状态，**不能作为当前发布结论**。当前统一门禁见 [最新统一门禁](full-alignment-gates.json) 与 [最终实施总记录](full-alignment.md)；旧 `release-gates.json`、二进制身份和逐路径 target 指纹保留其历史快照。

- **当前新增链路**：复杂度面板首次分析／真实 Python worker，以及原生 Git 时间线固定 SHA 分页、首提交 Diff、精确文件 Blame 已接通；Git 六条真实 REST 回归已通过。在途取消／HTTP future drop 两项和复杂度 Rust/UDS 新专项已在主任务统一 workspace 实测通过；完整说明见本文末。
- **Python 当前完整门禁**：`/tmp/zk-python-alignment-final-coverage.txt`，335 passed、9 warnings、coverage **75.83% ≥ 70%**、exit 0；SHA256 `c36a07d7903e2ce700bbca684922e0081e01e62d1493faeb860e13be9ffc00ca`。已包含新复杂度工作进程、缓存实际命中、字节变更、非法编码、截断和取消回归。
- **前端当前完整门禁**：冻结后的生产／测试树 **156 files /1295 passed /0 skipped**，241.48 秒、exit 0；`/tmp/zk-frontend-alignment-frozen-unit.txt` SHA256 `6a9465fb91646482334658d565bb6e43781a22aa7dd2700a8a012f3e91681f83`。lint **0 warnings**、build **13.34 秒通过**（原有大 chunk 提醒）；日志 `/tmp/zk-frontend-alignment-frozen-{lint,build}.txt`。502 个 src 文件在执行前后聚合 SHA256 均为 `a3bb63bff6ef933a0648f2bc2299abe34aecaf18d42a63b8e66be55f94cea71c`，完整身份／退出码见 `/tmp/zk-frontend-alignment-frozen-evidence.json`。此前 155/1282 和首轮 156/1295 均为历史，未累计计数。
- **16 个旧 skipped**：全部为空占位，已经移除；有产品消费者的行为由真实 DAG、Replay、Coordinator 回归替代，无生产消费者的旧预留如实标记过时。具体映射见 `product-acceptance-matrix.json`，没有把 skip 计为通过。
- **最终 Rust binary E2E**：**production 6/6（29.1 秒）、analysis 3/3（30.6 秒），均 exit 0**。实际 release 前后 SHA256 均 `cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`；`/tmp/zk-post-adapter-browser-binary-evidence.json` 为当前身份记录。旧 `822ad…` 及首次 Qwen-low 夹具失败均为历史，不替代新产物验收。

## 初始批次已实现（历史快照）

- **真实代码分析链路**：React 使用源项目公开 URL `/api/code-diagrams/generate`、`/api/code-path/endpoints`、`/api/code-path/trace`；Rust 绑定保存的 Project／Session，校验请求根目录和入口文件，再以 snake_case DTO 通过既有 UDS 调用真实 Python 分析器。响应按明确 DTO 转为 camelCase；拒绝失败信封、缺失字段和越界路径，不返回固定图或空成功。
- **兼容与访问边界**：旧 `/api/analysis/{generate-diagram,api-endpoints,code-path}` 接入相同 Rust 授权适配，保留 Python 响应形状。目录字符串本身不能授权；Session 与 Project 不一致、会话上下文不一致、项目撤销、入口 symlink 逃逸均拒绝。返回前重新核对绑定，阻止删除／切换后的迟到结果发布。
- **取消和缓存**：每次分析有 UUID、所属 Session／Project 和取消入口 `/api/code-analysis/cancel`。React 参数变化、切会话、离开面板或重复请求会取消旧请求并丢弃迟到响应。Python 分析在至多两个可终止子进程内执行，含截止期限、父进程消失看门狗、终止／kill／join 回收；清理无法确认时不释放容量。取消 tombstone 阻止取消后迟到的首次执行。结果缓存仅在内存保存、按 owner／参数／实际文件字节指纹隔离，最多 32 条／32 MiB，五分钟到期；请求仍先经过可取消的真实文件指纹扫描。
- **解析真实性**：Python 路由装饰器中的路径、方法、静态 APIRouter 前缀及多方法路由传入调用图，避免按函数名覆盖真实路径。缓存含语言和字节指纹；扫描期间文件改变会报错要求重试。不存在的函数返回 404。
- **OpenAPI**：Rust 聚合其实际 `ApiDoc` 与 UDS Python `app.openapi()`；所有 Python component 类别、引用、security requirement、operationId 命名空间隔离，Rust 冲突项保持权威。Python 不可用时展示带明确警告的 Rust 文档；不回调 Java 端口。直接 Python `/openapi/merged` 明示仅 Python，`/openapi/java`／`backend` 返回 410；公共 Rust 旧 Java URL 作为本地 backend 文档兼容入口。前端标签改为 Rust Backend，切来源／关闭时取消旧请求。合并文档排除 Python 内部文档辅助 URL，别名分析路由请求体使用真实 Rust 授权 DTO，响应仍为 Python 原始形状；该初始批次 Rust 注册文档为 73 路径，不将它冒充全部 Axum 路由覆盖。
- **可见入口**：开发工作台桌面侧栏增加“侧栏面板”选择器，使用已有面板集合；图表和调用路径可以通过正常 UI 进入。简洁工作台、会话列表、侧栏收起／拖宽继续保留。
- **工作台与阶段**：迁移源的胶囊里程碑表现，使用 Rust 小写运行状态，保留失败／取消／中断、STALE 和未知核验状态的真实表述，不显示虚构百分比。源在固定区间内将缺省视图改为 development；本轮只修改未配置缺省值，保留已有有效全局／会话偏好和两个切换入口，不迁移源的清空偏好行为。
- **Tailwind 4**：升级 `tailwindcss`／`@tailwindcss/postcss` 至 4.3.3，`tailwind-merge` 至 3.7.0；移除 autoprefixer，保留本地 Tailwind 设计令牌配置并显式载入。废弃 utility 名称按原像素语义迁移，保留现有主题与强动效默认。修复 v4 cascade layer 对重要 hover 规则的影响；API Keys 使用主题令牌。新增专用按钮前景色及有实际浏览器证据的主题对比度修复，已覆盖 API Keys、里程碑和两种工作台切换。

## 已执行的验证（初始批次历史记录）

| 项目 | 真实结果 | 日志 |
|---|---|---|
| Python 实际分析、缓存、worker 回收、目录安全、现有分析器 | **55 通过**（9 条依赖警告），含连续取消不打断回收 | `/tmp/zk-analysis-python-integration.log` |
| Rust typed adapter／授权／UDS／OpenAPI 集成 | 主任务执行 **3 通过**；之后新增 schema/adapter 文档断言待再确认 | `/tmp/zk-alignment-server-focused.txt` |
| React 授权 store、面板行为、OpenAPI 竞态、工作台偏好、里程碑、页面退出 hook | **8 文件 22 通过** | `/tmp/zk-alignment-focused-frontend.txt` |
| React 全量单测 | **141 文件、1212 通过、16 跳过**；已包含 App 页面退出接线，后续键盘批次另行验证 | `/tmp/zk-full-alignment-frontend-tests.log` |
| 前端 lint | **通过**；覆盖最终样式变更 | `/tmp/zk-full-alignment-frontend-lint.log` |
| Tailwind 4 构建 | **通过**（原有大 chunk 提醒） | `/tmp/zk-full-alignment-frontend-build.log` |
| npm audit（完整依赖） | **0 漏洞** | `/tmp/zk-tailwind4-audit.json` |
| 主题 browser 扩展矩阵 | **20 场景／12728 断言通过**；包括 API Keys 编辑／撤回、里程碑、两种工作台切换和实际计算样式对比度 | `/tmp/zk-tailwind4-theme.log` |
| Jelly browser | **42 场景通过**，Chrome 154.0.8037.98 | `/tmp/zk-tailwind4-jelly.log` |
| 新真实分析 E2E | **2 通过，12.5 秒**，真实 React → Rust → Python 解析器；优雅停止后无 fixture／侧车残留 | `/tmp/zk-alignment-analysis-e2e.txt` |
| 原真实 Rust production E2E | **4 通过**，此为键盘批次之前的已执行结果 | `/tmp/zk-alignment-production-e2e.txt` |

该初始批次仍有 16 条 skipped 空占位，未计为通过；它们最终的替代验证／过时判定见本文末及 product-acceptance-matrix.json。Python 静态分析不执行项目代码，但跨模块动态路由挂载、反射与运行时分派仍属于静态分析局限，不声称穷尽实际运行调用链。

## 初始批次历史：当时仍需完成的事项

1. 最终发布 binary 仍由主任务统一门禁。两组已执行 E2E 的实际 binary SHA256 为 `8d71f04f3a6772837bd14da8f127b13189079adfaee48c1e8503d9e3e74e21f3`，运行前后校验一致。
2. 键盘／Vim／会话执行配置批次的前端 lint／全量测试／build 已通过；新增第 5 个 production E2E 等待包含本批 Rust 命令的新 binary。
3. 主任务统一 Cargo 格式／Clippy／全量测试和最终发布门禁；其他工作线仍在变化，本工作线通过不等于整体迁移已完成。
4. 键盘／Vim 批次已落盘，正在专项与整链验证；MCP 页面／OAuth 由另一工作线负责。

Tailwind 迁移依据：[官方升级指南](https://tailwindcss.com/docs/upgrade-guide)、[官方 PostCSS 安装说明](https://tailwindcss.com/docs/installation/using-postcss)。

## 键盘、Vim 和会话执行配置批次（历史状态：当时验证中）

- `editorPreferences` 为可选 Rust 全局用户配置；Vim 默认关闭，缺省 Enter 和 Ctrl／⌘K 保留。配置校验拒绝未知动作、裸字符、复制／粘贴／撤销／中断占用及前缀冲突；服务端确认落库后才启用，失败保留最近有效设置。
- 调度器实际消费 context、IME、已处理事件、双键和弦及超时；卸载和焦点变化清理等待状态。Vim 仅操作聊天 textarea，支持 insert／normal／visual、常用词和行编辑、寄存器及有界 undo／redo；不接管 Monaco，不发送按键网络事件。这不等于完整 Vim 的宏、Ex 命令和插件生态。
- `/vim` 与 `/keybindings` 有真实持久化和界面消费者；历史重放不会从消息重新写配置，界面只读取当前服务端事实。
- `/fast`、`/effort` 和 UI 共用当前会话执行偏好，调用 runtime 工作线的 revision／CAS 接口。快模型要求已显式配置；不改变主会话 model、新会话默认、Key 允许条件或费用预算。已替换原本只改内存的强度滑块。失效配置显示诊断，并提供显式恢复当前会话默认操作。
- 最新完整门禁：**146 文件／1229 项通过／16 项既有跳过**（`/tmp/zk-editor-final-tests.txt`）；lint **0 warnings**（`/tmp/zk-editor-final-lint.txt`），build **通过，12.70 秒**（`/tmp/zk-editor-final-build.txt`）。包含真实 Prompt 自定义提交、模态窗口拦截和 textarea 选区 Ctrl+C 复制回归。Rust validator／配置 API／命令测试交主任务集中执行；新第 5 个真实 E2E 等待新 binary，尚不计完成。

## SQLite 记忆检索和辅助调用（历史状态：专项通过，整体发布门禁当时待完成）

- 生产记忆注入从同一 SQLite scope snapshot 读取 revision 与条目；本地 BM25 优先相关条目，保留非候选条目及原 token 预算，不读取旧文件记忆。
- 只有显式设置 `ZK_MEMORY_RERANK_MODEL` 才启用最多 20 个候选的语义精排；没有配置时不新增 LLM 请求。模型必须已在当前注册表中，Key 热更新后每次重新检查，不能回落到其他默认模型或新增付费候选。
- 精排沿当前 Task／Run 的真实预算和物理请求账本；请求无工具，3 秒期限，候选正文有界。返回必须为同一 revision、完整且唯一的合法候选 ID 排列，不接受模型返回的内容、越界 ID 或额外字段。响应后重读 revision；编辑／删除竞态、供应商或格式失败回到最新本地排序。
- 新辅助调用收集器与现有摘要复用完整流、取消、大小上限及尾部 usage 消费；不把截断、工具调用、缺失结束事件或错误响应视为成功。现有摘要模型／thinking／预算配置保持原逻辑。会话合并保留其独立的事务状态轮询，未为统一名称而削弱该取消边界。
- 实际验证：辅助执行 **3 通过**（`/tmp/zk-auxiliary-focused.txt`），SQLite 检索／精排 **5 通过**（`/tmp/zk-memory-retrieval-focused.txt`）；同一新编译 engine 测试产物继续运行原摘要 **8 通过**（`/tmp/zk-summary-auxiliary-regression.txt`）。计费测试使用真实 SQLite／ProviderRegistry 和可控响应，校验 `llm_calls` 的真实金额、完整尾部用量及主任务预算拒绝，不调用收费 API。此前 fixture 缺主任务期限和错误读取尚未结算的 Task consumed 字段，已改为有效期限与物理调用账本断言后重跑通过。

## CLI 会话分叉的 SQLite 基础（历史状态：专项通过，Query 整链当时待完成）

- `Db::fork_session(request_id, SessionForkRequest)` 在同一写事务中校验持久 root 来源、空闲任务／资源／交互、无 merge 占用及闭合工具批次，封存源记录并创建新目标。模型、目录及权限模式继承来源；消息使用独立 ID，正文 JSON／图片数据保持原样，不复制 Task／Run 归属、一次性权限、消息用量或会话费用。
- `session_forks` 保存无正文的幂等记录；封存内容在 `session_fork_snapshots` 归目标所有，目标删除时一并清理。重复 requestId 返回原目标；参数不一致或原目标已删除均明确拒绝，不重新创建。封存上限 64 MiB／20000 条消息，超限返回 `FORK_SNAPSHOT_LIMIT`。
- 原 `steering`／Skill 指令／任务边界 metadata 仅嵌套为来源记录；被动图片身份 `referencedImages` 保留。回放到模型的历史 User 内容带明确引用、非新指令或授权前缀；原始存储正文不变。源合并摘要作为机器历史保留，但不复制旧合并操作的访问授权。
- 已执行五项 DB 场景 **5 通过**（`/tmp/zk-fork-db-focused.txt`）及历史回放 **1 通过**（`/tmp/zk-fork-replay-focused.txt`）：原文／图片／权限／零用量、并发幂等、故障回滚、忙碌／不闭合批次拒绝、临时来源拒绝、删除后不复活及合并摘要保留。Query／CLI 接线和临时 ContentStore 由主任务组合；尚未将这些底层测试计作 CLI 浏览器整链完成。

## Hook 展示投影（前端门禁通过）

- `hookPresentation.text` 在新旧工具结果详情共同使用的组件中展示为明确标注的纯文本备注；HTML 不执行。原始 Result、真实错误标签、结果结构和证据渲染继续可见，不以 Hook 文案改写工具成功状态。
- 组件 **4 项通过**（含两项新增回归，`/tmp/zk-hook-presentation-frontend.txt`）。包含本批修改的完整前端检查：**146 文件／1231 项通过／16 项既有跳过**（`/tmp/zk-hook-final-frontend-tests.txt`），lint 与 build 均 exit 0（`/tmp/zk-hook-final-frontend-lint.txt`、`/tmp/zk-hook-final-frontend-build.txt`；构建 14.57 秒，保留大 chunk 提醒）。Hook 后端的调用、授权及持久化由 runtime 工作线验证。
- 后续真实接线发现 canonical `message_complete` 会正确清除非事实的 Hook metadata，单靠组件读取会丢失备注。现已补独立内存投影：WS 以 Session／Run／ToolUse 身份记录，canonical 绑定真实 Assistant message ID；历史 REST 带当前 Session header 分页恢复，缺少 message ID 时不猜测关联。同会话重连重新加载，切会话或重连会中止旧请求并拒绝迟到结果。备注不写入原始 output／消息／浏览器持久存储，错误状态保持真实。
- **该批次**全量门禁：**147 文件／1237 通过／16 既有跳过**，lint、build 均 exit 0（`/tmp/zk-hook-overlay-final-tests.txt`、`/tmp/zk-hook-overlay-final-lint.txt`、`/tmp/zk-hook-overlay-final-build.txt`，build 16.98 秒）。实际 dispatch 回归覆盖 WS 错误结果到 canonical 替换、原文不含 Hook 备注；独立 store 回归覆盖历史归属、分页、跨会话迟到、恢复失败保留有效状态与同会话重连。测试日志 SHA256：`50a6dc4e466c79acf6fe311e27874e49b7be2000a32d69c472923642805e879a`。上述较早计数保留为历史验证，不与当前结果累加；后端 REST／持久化仍由 runtime 专项验证。

## Read 编码与原始字节撤销（专项通过）

- Read 支持 UTF-8 BOM、UTF-16LE／BE BOM 及显式 GB18030／ISO-8859-1。未标识的非 UTF-8 只能作为明确提示的 Latin-1 预览，不宣称可靠探测，也不赋予覆盖权限；二进制、非法选定编码、不可逆映射、冲突 BOM 均不能静默改写。
- 完整 Read 绑定原始字节 SHA256 和实际编码／BOM；Edit／Write 再核对字节版本，使用同一编码严格回写，无法表示的新字符在写入前拒绝。保留未修改的混合换行，不用 Unicode 替换字符掩盖损失。
- 非普通 UTF-8 的写前快照保存原始 bytes，授权 Undo 按 bytes 恢复；临时会话 bytes 与文本预览均只进入 MemoryContentStore，SQLite 只保存随机引用。用户实际文件不随临时 scope 清理而删除。
- 本轮实际验证均 exit 0：严格编码 **3 通过**（`/tmp/zk-read-encoding-codec-tests.txt`）、真实 Read→Edit→Write **2 通过**（`/tmp/zk-read-encoding-tools-tests.txt`，包含五编码组合）、既有文件工具回归 **36 通过**（`/tmp/zk-read-encoding-regression-tests.txt`）、扩展原始 bytes 的 DB 快照 **1 通过**（`/tmp/zk-read-retention-db-tests.txt`）、Engine 快照 **3 通过**（`/tmp/zk-read-retention-engine-tests.txt`，包含五编码×持久／临时共十种精确 Undo 组合）。DB 测试扫描原始字节及正文／原始 bytes SHA256 均不在数据库和 WAL 出现。

## 临时工作台与会话执行偏好（专项通过）

- 验收条款正文在 initialize／replace／read 中按 Run 对应 Session 的 retention 编解码；SQL INSERT／UPDATE 守卫拒绝临时条款原文旁路。criterion ID 仍使用随机 UUID，固定状态／类型／归属可以持久化。
- 工作台可审阅结果判断在同一 SQLite 读事务中解码候选 Assistant 内容，不对随机 RAM 引用执行原文 LIKE。会话执行偏好保持原 revision／CAS 语义，整个 metadata 经同一 ContentStore 保存和恢复，不丢失其他字段。
- 新 DB／WAL 集成 **1 通过**（`/tmp/zk-read-retention-db-tests.txt`，与快照测试合计 2 项），覆盖 initialize／replace／读取、真实 Assistant 文本检测、偏好 CAS、旁路拒绝、原文与 SHA256 不落盘、重启／lease 结束失效；既有工作台的原子读快照、持久恢复等 **3 通过**（`/tmp/zk-workbench-codec-regression-tests.txt`）。临时 Query 仍由主任务最后开放，以上不代表完整临时模式完成。

## 临时 Bash 无落盘执行（历史状态：工具／引擎专项通过，生产附着入口当时追加验证中）

- 新 `ShellMemoryScopeFactory` 作为 Run 默认作用域工厂，只为临时会话注册 RAM CWD；普通会话的 shell 初始化和文件行为保持原路径。授权分析与实际 Bash 执行读取同一会话 CWD，Run cleanup／Drop 清理 RAM。缺少作用域、跨 Run 使用或同会话并行执行均明确拒绝。
- 临时命令通过 `bash -c` 单次解析；内核 UnixStream pair 复用受监管的启动门控，先确认 PID 归属和取消状态才放行。专用 FD 仅返回有界 CWD，不创建 cmd／cwd／环境文件，不混入 stdout／stderr。真实 stdin 仍为 `/dev/null`。
- 若脚本主动替换 EXIT trap、关闭专用 FD 或被强制终止，不能确认最终 CWD 时如实显示错误，不自动重试有副作用的命令；后续命令要求显式重置工作目录。已真实运行工具 **2 通过**（`/tmp/zk-temporary-shell-tools.txt`）、全部 Bash **9 通过**（`/tmp/zk-temporary-shell-native-regression.txt`）、受管进程 **16 通过**（`/tmp/zk-temporary-shell-process.txt`，包含 memory-shell PID 绑定失败／提前取消不能执行）、工具注册适配 **16 通过**（`/tmp/zk-temporary-shell-registry.txt`）。真实根聊天和 attached Shell **2 通过**（`/tmp/zk-temporary-shell-engine.txt`），没有模型收费请求。

## 临时会话的检查点与自动快照（专项通过）

- `agent_checkpoints.messages_json`／`file_state_json` 与写前 `file_snapshots.content` 使用会话不可变 retention 策略：正常会话维持现有存储；临时会话的正文与文件状态 hash 仅在有界 MemoryContentStore 中，SQLite／WAL 只存随机引用。查询必须解析同一存活 scope，容量或作用域失效均报错；临时检查点不生成重启恢复 proof。
- 为两张表补齐 INSERT／UPDATE 的原文拒绝守卫，遗漏的 SQL 写入路径不能绕过 API 将正文存入临时记录。真实数据库测试同时扫描 DB／WAL 的唯一正文和 SHA256，并验证直接 SQL 旁路、重启及 lease 结束后的读取／新写入失败。
- 生产 `SessionSnapshotService` 绑定同一 Db；临时自动快照仅在内存引用索引中存在，不写 JSON／临时文件，也不进入可恢复快照列表。REST 显式导出与 resume 返回 `EPHEMERAL_OPERATION_UNSUPPORTED`；未知或失效 scope 不回退磁盘。正常持久快照行为保留，已删除会话遗留快照仍可显式删除。
- `FileHistoryService` 和 server SnapshotSink 复用同一 DB 快照仓储，所以写前备份自动遵循 retention；活跃临时 scope 可执行既有授权撤销。scope 结束只清理内存正文，用户授权写出的实际文件和产物不删除；失效后不能用空或猜测内容进行回写。
- 新专项实际通过：DB **1**（`/tmp/zk-ephemeral-db-snapshots.txt`）、Engine **2**（`/tmp/zk-ephemeral-engine-snapshots.txt`）、snapshot REST **3**，包括两项既有持久行为（`/tmp/zk-ephemeral-server-snapshots-editor.txt`）。均 exit 0。临时 Query 开关由主任务在所有正文路径验证完毕后开放，本段不表示整个 no-session 已完成。
- 同轮补齐此前待跑服务端验证：config API **11**（同上 REST 日志，含新 editor 配置落库与拒绝非法输入）、editor validator／真实命令 **4**（`/tmp/zk-editor-rust-focused.txt`）、辅助模型 strict route **1**（`/tmp/zk-auxiliary-server-focused.txt`）；新 ContentStore 基座下分叉 DB 五项再跑 **5 通过**（`/tmp/zk-fork-content-store-focused.txt`）。新增的第 5 个浏览器 production E2E 仍待新 binary，不以单元测试替代。


## Bash 后台、声明产物与 Brief（历史状态：多数专项通过，取消竞态复测当时待完成）

- 超时采用有意 Rust 适配：保留原缺省至少 120 秒，仅启用已有分类器将编译／安装扩至 300 秒、测试扩至 600 秒；显式值优先且不超过 600 秒，并继续受父 Run 更严格期限限制。源的读／搜索 30／60 秒缺省不用于缩短已有正常链路。
- `is_background=true` 映射为相同 TaskRuntime 的 attached Shell Task，返回真实 taskId；同一 tool-use 幂等，父 Run 期限／预算继续限制执行，普通与临时会话均走受管资源清理。`TaskGet`／`TaskList`／`TaskOutput`／`TaskStop` 常驻提供查询与停止，不开启 Agent／Swarm／Cron 配置。仅裸工具或 Shell 子执行目录缺少宿主后台端口时明确拒绝，不创建无归属进程。
- 声明产物仅支持前台；后台加非空 `declared_outputs` 在执行前拒绝。路径按实际 Bash CWD 冻结，经授权保护检查，命令执行前检查 created／modified／deleted 的真实前置状态；父目录身份变化、symlink、缺失、未改变及内容超限不能封存。单文件 50 MiB、合计 100 MiB、32 项有界。命令已执行但封存失败明确返回错误与 `retryability=NEVER`，实际文件保留。
- Engine 只消费可信绑定原生工具的产物能力，不信任远端名称或 metadata；SQLite 事务再次核验成功 Bash invocation、原始声明、Task／Run／Session 所属和完整批次，原子更新产物并使被修改的旧验证失效。requiredValidatorId 是待验条件，不冒充已通过。临时封存 digest／工具结果继续通过同一 MemoryContentStore；不声称完整 no-session 发布验收已完成。
- `Brief` 实际注册到正常工具目录，保留源的 project／session／custom 基础上下文与 Rust 追加文本裁剪，修复 smart 单行预算边界。不声称采集了 Git／历史或生成了主题分析。Bash 失败增加分类和结构化修复建议；建议不自动执行，信号退出不误当作已知超时。
- 已执行：Bash 工具 **25**、Brief **7**、CtxInspect 工具 **2**、授权分析集成 **20**、既有产物仓储 **3**、新增原子声明批次 **1**、实际 Engine 声明输出 **1** 均通过。对应日志：`/tmp/zk-bash-declared-tools-tests.txt`、`/tmp/zk-brief-native-tests.txt`、`/tmp/zk-context-tool-tests.txt`、`/tmp/zk-declared-authz-full-tests.txt`、`/tmp/zk-declared-artifact-db-tests.txt`、`/tmp/zk-declared-artifact-batch-tests.txt`、`/tmp/zk-declared-artifact-engine-tests.txt`。
- 实际 AppState／TaskRuntime 三项回归中，普通与临时后台真实执行、声明产物自动终态 integrity observer **两项通过**，后者验证 `verified`、原始封存 hash 不变、未声明文件不混入以及再次观察幂等。停止回归仍保留严格断言：之前出现 Shell future 先返回、资源确认稍后写入的竞态，实际 `partial/unconfirmed`，runtime 工作线已修但尚未复测，不能计为完成。日志 `/tmp/zk-background-bash-server-tests.txt` 为此真实失败记录，不伪称全部通过。
- 本轮补强仓储同事务验证：一个声明不能以重复 receipt 代替其他声明；除单文件限制外重新校验总批次 100 MiB。坏末项使全批回滚，不发布部分可信产物。机器默认工具目录已扩为 39，实际编译 schema hash 为 `49bf5dab997d0a126c0bf7eb55ecb7f2ada12aee49097b32d390000259a5223b`；黄金与机器 JSON 已同步，待新编译复核。

## REPL 会话服务界面（历史状态：组件专项通过，服务端整链当时验证中）

- 设置内当前会话增加真实服务状态和显式停止入口，GET／DELETE 都绑定 `X-Session-Id`；停止需普通确认。202／pending 只显示等待清理；只有服务端 `stopped+confirmed` 表示已停止，`cleanupUnconfirmed` 不冒充成功。读错误和停止错误分别保留，切会话／关闭取消旧请求。
- React 组件 **5 通过**（`/tmp/zk-repl-service-ui-focused.txt`），覆盖 absent、确认后取消、真正清理完成、未确认清理、跨会话迟到响应及停止失败。本轮完整前端门禁实际 exit 0：**148 文件／1242 通过／16 既有跳过**（`/tmp/zk-repl-service-ui-all-tests.txt`）、lint **0 warnings**（`/tmp/zk-repl-service-ui-lint.txt`）、build **13.78 秒通过**（`/tmp/zk-repl-service-ui-build.txt`，保留原有大 chunk 提醒）。Rust REPL 服务与新 binary 整链验证尚待协作工作线完成，不以组件测试替代。

## 变更影响分析真实入口（历史状态：专项通过，真实整链当时待新 binary）

- 发现旧 `changeImpactStore` 除无所属身份外，页面没有真实发起调用。现增加文件／行号／深度表单与 visualization hint 预填，只有用户明确发起才执行。复用 `AnalysisRequest` 的 Project／Session 绑定、UUID、Abort、服务端取消和迟到响应丢弃；切换会话与关闭面板立即取消。
- 请求使用 Rust typed adapter 的公开 camelCase DTO；保留 snake_case 响应 envelope。严格要求结果声明 `analysis_kind=advisory` 和 `is_verification_evidence=false`，显示截断状态，零节点仅表示静态分析未识别依赖，不能显示验证成功。
- 实际前端专项 **12 通过**（`/tmp/zk-change-impact-ui-tests.txt`），含已授权请求、错误形状、取消、乱序／会话切换、明确发起、关闭页面和非法行号。lint **0 warnings**、build **12.58 秒通过**、新增 analysis E2E typecheck 通过（`/tmp/zk-change-impact-ui-lint.txt`、`/tmp/zk-change-impact-ui-build.txt`、`/tmp/zk-change-impact-e2e-typecheck.txt`）。已扩展真实 React→Rust→Python E2E 为 `api.py` 的 fetch→get_user 影响路径，等待新 server binary，尚未计为执行通过。
- 同轮发现 `CtxInspectTool::new(None)` 原来把消息和 Token 显示为零成功。已改异步真实 DB 投影，并核对当前 Run／Session 归属、父 Run 深度；缺数据明确报错。`/context` 共用该事实源，累计用量明确不冒充当前模型上下文窗口占用；新 Rust 专项尚待 Cargo。

变更影响入口本轮全量前端门禁已实际完成：**148 文件／1246 通过／16 既有跳过**，`/tmp/zk-change-impact-ui-all-tests.txt`，exit 0，139.41 秒；SHA256 `3f2ddd46dfb34c45f3dc15270a0196dfa8e4313dca5a004459fc22b843eb4edf`。此结果保留为该批次证据；后续单测与真实 Rust/Python 影响分析 E2E 结果见下文，不累加旧计数。


## 本地命令与 Hooks 编辑（历史批次的实现与专项状态）

- `/theme` 打开现有主题／动效设置；`/skills` 打开全局 Skill 管理；`/tasks` 打开当前会话任务面板；`/export [json|markdown|md]` 打开实际下载选择器。只接服务端命令事件，消息历史中的相同文字不触发动作；切会话后的旧命令不打开新会话面板。`/changes` 是已授权只读 `/diff` 的别名。
- `/memory [show|init]` 保留命令兼容入口，统一打开 SQLite／scope revision 编辑器。原直读文件且静默吞错、`init` 覆盖 `zhikun.md` 的旁路已移除；用户已有 Markdown 文件不被修改。
- 当前会话导出由用户明确点击；JSON／Markdown 保留原格式。REST export 和 resume 在解码正文前核验 retention：临时会话即使活跃亦不能导出／恢复，通用详情与消息仍允许读取活跃 RAM。新增真实 REST 测试覆盖 scope 存活／销毁和缺失会话，已在 `/tmp/zk-server-alignment-combined3.txt` 通过。
- `/hooks [list|edit]` 打开真实 TOML 编辑器。GET／PUT `/api/sessions/{id}/hooks` 绑定 Session 与当前工作区，256 KiB／128 声明上限，严格配置验证、显式保存确认、内容 revision／CAS、会话 idle／mutation 门控。配置目录创建与读取拒绝 symlink，写入继续核对冻结目标身份；保存不运行 Hook。无效现有配置可显示并修复，保存失败和冲突保留草稿；退出／切会话取消请求。临时会话不允许保存持久 Hooks 配置。
- React 新专项 **9 通过**（`/tmp/zk-command-hooks-ui-tests.txt`），lint **0 warnings**（`/tmp/zk-command-hooks-ui-lint.txt`）及 TypeScript 检查通过（`/tmp/zk-command-hooks-ui-typecheck.txt`）。该批完整前端 **151 文件／1255 通过／16 既有跳过**（`/tmp/zk-command-hooks-ui-all-tests.txt`），build **22.39 秒通过**（`/tmp/zk-command-hooks-ui-build.txt`）。后续批次覆盖见下文，不累计较早计数。
- 新增 Fork **真实关闭文件数据库再重开**回归：重放 requestId 必须返回原目标，源会话后续正文与权限变化不能改变已封存快照、继承权限、消息 ID／图片身份；已在 `/tmp/zk-db-alignment-all-targets3.txt` 通过。此前五项内存库测试仍仅作为其已有范围的通过证据。
- 该批次时 `/rewind` 与 code-search 尚在实施；其后续实现和真实验证边界见下文。


## 文件回退、作用域与最终分析浏览器补验（2026-10-07）

- `/rewind` 现打开实际文件检查点选择器，用户明确选择文件后生成五分钟单次预览 token，再普通确认。服务端冻结实际工作区、选中文件、原始字节与快照身份，全批先检查再逐文件 CAS；后台无自动重试或覆盖并发新内容的回滚。中途 IO／并发失败如实返回已恢复、失败和未处理文件。临时预览正文与 hash 仅属于原会话 RAM scope，scope 失效后 token 不能复活。
- 原历史读取端点补匹配 `X-Session-Id` 和现有工作区绑定；当前完整集成日志中的 **15 项** history 测试为补读取门控之前结果。新增独立跨会话拒绝测试和既有 fixture 已适配，等待下一 Rust 批次。回退的 REST 预览／确认、确认前不写、全批预检、跨会话 token 不被消费、单次使用均已通过此前真实 REST。
- 文件变更面板不再请求不存在的 `toMessageId=current`；改为用户选择两个真实已保存检查点，明确展示整批快照差异，与 Git／实时工作区分开。请求绑定会话并取消迟到，HTTP 失败不伪装为空变更。
- `/code-search <pattern>` 注册为正常 Prompt 命令，JSON 封装用户表达式并请求现有 Grep；不加 path 扩大默认范围、不独立建执行器、不将请求说成已经搜索完成。当前总／可见命令均为 **44**，默认工具 **39**；实际编译黄金待下一轮复核。
- Skill 的列表、详情、开关与别名解析均跟随当前 Session；尚无会话时可选择已保存 Project，否则只看全局候选。所有请求只携一个范围，切换时同步清候选并取消请求；epoch 阻止旧开关或 A→B→A 迟到结果覆盖新视图。全局开关明确影响所有项目；未向全局注册项目正文。
- 新前端专项 **6 文件／56 项通过**，包含作用域迟到、全局开关、Rewind 确认／部分结果、真实检查点选择和历史读取失败；TypeScript 通过（`/tmp/zk-skill-history-targeted.txt`、`/tmp/zk-skill-history-typecheck.txt`）。此批完整前端 **154 文件／1264 通过／16 既有跳过**（`/tmp/zk-skill-history-final-tests.txt`，132.68 秒）、lint **0 warnings**、TypeScript／build **通过**（`/tmp/zk-skill-history-final-lint.txt`、`/tmp/zk-skill-history-final-build.txt`，11.74 秒）。后续 MCP 专用用途 UI 又有改动，其最终全量另行记录。
- 真实浏览器分析 E2E **2/2 通过，18.6 秒，exit 0**（`/tmp/zk-analysis-browser-final3.txt`），包含代码图、调用路径、变更影响 `fetch → get_user`、辅助分析非验证证据标签、页面退出、会话／项目越界、预启动取消与实际合并 OpenAPI。调用真实 Rust binary `target/debug/zk-server`，前后 SHA256 均 `a11f71dbd3efbc5a601e3386fd4275b2c7d7877b187c09b77ad32005862fddc0`；不代表后续改动或最终 release binary。
- 上述 E2E 前两次只因 OpenAPI 新 Rust change-impact 路径的冲突提示断言过时、继而测试误假设 native 操作带 Python 专用扩展而失败；最终断言 Rust `operationId=change_impact`，保留两条准确来源冲突提示，未削弱真实链路断言。
- 主任务 `/tmp/zk-server-alignment-combined3.txt` 已实际通过 **Hooks REST 3、ContextInfo 1、后台 Bash 3（含真实停止及终态产物）、typed analysis 4、snapshot API 2**；该完整批次仍有其他失败，不能整体标通过。本工作线 CWD 测试补真实 root budget；Rewind 测试将损坏元数据与有效但更换的快照分开验证，修正后待复跑，始终要求文件不被更改。

16 条既有 skipped 均为迁移前空占位：AgentDAGChart 4（phase／方向／fitView）、BrowserReplayTimeline 5（刷新／清理／帧详情／空态／缩略图）、coordinatorStore 7（旧 action／进度／清理／告警上限／收起）。不计为验证。当前有效回归及剩余第三方布局限制保留在 `product-acceptance-matrix.json`，并由全量执行验证现有真实测试；不为清零占位而新增机械测试。


## MCP 专用会话可见性（与服务端工作线协作）

- Session 的 `purpose=chat|mcp` 来源为持久根 Task 类型，由 DB／REST／匹配的 WS bind 确认投影；不根据标题或客户端 metadata 猜测。终态服务会话仍为 MCP 用途。
- 列表保留 MCP 会话以便用户查看实际活动／权限请求，明确用途并禁止选作合并来源。当前 composer 替换为服务说明和 Activity 入口；普通聊天和 slash 命令被前端提交入口再次拒绝，真实后端也拒绝普通 Query。当前模型、快模型／effort、permission 控件禁用，权限明确固定 DEFAULT；不把普通聊天已有 AUTO_APPROVE 偏好展示成 MCP 执行权限。普通会话、新会话默认模型和全局设置保持既有行为。
- React 专项 **4 文件／68 项通过**（`/tmp/zk-mcp-purpose-targeted.txt`），包含四项新增：service composer／Activity 可达、模型与权限控件不发包、可信 WS 用途恢复和普通聊天切回、合并候选排除。本轮完整前端 **154 文件／1268 通过／16 既有 skipped**，184.24 秒、exit 0（`/tmp/zk-purpose-final-tests.txt`，SHA256 `0a5469d7de26094666a749fed0958c1d9efa86398404c9df62150f7cad6f7fd6`）；lint **0 warnings**、独立 tsc、build **23.35 秒通过**（`/tmp/zk-purpose-final-lint.txt`、`/tmp/zk-purpose-final-typecheck.txt`、`/tmp/zk-purpose-final-build.txt`）。构建仍有既有大 chunk 提醒。Rust purpose API／DB／WS 由服务端工作线独立验证，最终稳定 binary 的 production E2E 尚待统一执行。

本轮严格 Clippy 收敛保持行为：Bash 有界读取缓冲改堆分配、声明路径逐级 parent 改 PathBuf.pop；Write／Edit 抽出元数据、编码版本、内容匹配及二次冲突检查；checkpoint 在同一事务中调用 recovery-proof helper。没有移除黄金断言或广泛关闭 lint。原始字节、授权路径、CAS 与真实错误状态保持。临时 FileHistory 日志不再输出路径／任意错误 Display，仅固定码和 IO ErrorKind；普通UI错误仍返回实际明确失败。


## 自动可视化建议与该批次前端门禁（2026-10-07；历史记录）

- `props.intentOnly=true` 在具体可视化组件挂载之前进入独立建议卡，明确显示“可视化建议，尚未执行分析”。不把分类生成的 hint 当作 Git 记录、Schema、Mermaid 源码或测试证据；原因作为有界纯文本显示。只有用户明确打开面板才填充已有筛选入口并导航，没有真实入口的 Schema／GitHub 仅展示建议。原有非 intentOnly 渲染保持。
- MCP 专用会话的 Activity 按钮使用实际 `apos` 页面；原 APOS 功能开关关闭时明确提示，不擅自开启。权限确认弹窗仍可使用，普通聊天控件不解禁。
- 新增可视化组件 **5 项**真实回归覆盖三类直渲染组件不会被挂载／不会请求、用户明确导航与普通 Mermaid 保留；与 MCP composer 合计专项 **2 文件／22 项通过**（`/tmp/zk-autovis-ui-targeted.txt`）。
- **该批次历史完整前端结果：155 文件／1273 通过／16 既有 skipped，152.46 秒，exit 0**（`/tmp/zk-autovis-final-tests.txt`，SHA256 `f40c4920aa9bdae89614902deb77b908046ccbd844e83b763c99cc0c4198d40a`）。lint **0 warnings**（`/tmp/zk-autovis-ui-lint.txt`，SHA256 `b60802a2486a38abed82876ac6a1e311e4fc7c1c79b586671b867ae4dab4a448`）、独立 tsc（`/tmp/zk-autovis-ui-typecheck.txt`）均 exit 0；build **17.32 秒、exit 0**（`/tmp/zk-autovis-final-build.txt`，SHA256 `c42dfb35c3d75c56474e202a853c2dd680ce318636d57e210227d379dad4a117`），仍有大 chunk 提醒。较早各批次记录是历史证据，不与最新计数累加；16 个空占位未计为通过。
- 后端可视化分类／辅助计费由 runtime 工作线验证；该前端结果不代表最新 Rust binary 的整链验证。最终 production E2E 仍等待主任务稳定产物。
- 本轮 Clippy 拆分仅将 Bash 声明路径事实和 DB 测试初始化抽到 helper，命令与声明资源仍进入同一个授权决定，批次原子性、幂等与跨会话拒绝断言保留。严格 Cargo 门禁由主任务统一执行，未因拆分预先标记通过。

服务端严格检查本轮还提取了会话创建请求解析、临时浏览器 create/reserve/失败清理和受管预览 readiness 等明确阶段；浏览器创建仍持有原作用域互斥锁，预览仍使用原资源 owner、取消令牌与期限。Hooks 保留 256 KiB 有界读取，ContextInfo 保留原诊断码；仅调整借用、等价分支与时长单位，尚待主任务统一 Clippy／测试确认，不记为新增已通过门禁。


## 最终前端核对与浏览器回放原型接通（2026-10-07）

- 对固定源 `053adf9071dc1996aa1ebfa30c0a23d587ffad5c` 的 touched React 文件再次核对：仍未导入的 11 个最终存在文件全部属于用户排除的发布卡片、装裱导出和仪式动画。该清点用于发现缺项，不把文件存在等同运行正确；此前 427 路径台账的 targetCurrent 指纹保留为历史快照，未盲刷新。
- 两仓旧 `BrowserReplayTimeline` 均未在产品中挂载，不能仅以已有仓储测试算作完成。现复用其视图并加入桌面／移动共用侧栏面板选择器；仅当前明确会话请求，GET／DELETE 带 `X-Session-Id`，切换／关闭取消并拒绝迟到数据。跨会话帧拒绝展示。清空需普通确认，仅真实 `status=deleted` 才清本地内容，失败保留旧帧；时间线不存在与权限失败分别处理。
- 原生 replay REST 核验同一存在会话、禁止 merge billing 内部会话、验证当前工作区绑定；临时会话明确使用 Run 所属 RAM evidence，不能读取／删除磁盘 replay。`/browser-snapshot` 也先拒绝临时落盘入口。新增 `browser_replay_api` 两项已在 `/tmp/zk-alignment-workspace-tests-first.txt` 实际通过；该全仓批次仍有其他失败，不表示整体门禁通过。
- 浏览器面板六项、DAG 真实挂载五项 **11 通过**（`/tmp/zk-browser-dag-actual-tests.txt`）：包括 scoped refresh、确认／失败保留／确认删除、真实截图与交互详情、缺失／临时边界、跨会话／迟到、Agent 状态与清理、实际 dagre 方向／推断边区别、fit 与卸载取消。DAG 只 mock ReactFlow canvas，保留真实生产组件、store 和布局计算，不宣称第三方 canvas 内部已测试。
- 对同一生产树，完整前端实际 **155 文件／1279 通过／7 旧空占位跳过，160.14 秒，exit 0**（`/tmp/zk-browser-replay-final-tests.txt`，SHA256 `583d525fd012761f0b206699d89cb6220b65242cd95e06835aa6460aedcb0aa2`）。随后仅测试文件补三项有意义的 Coordinator 状态流／跨会话清理／告警容量验证，该文件 **10/10 通过**（`/tmp/zk-coordinator-final-tests.txt`，SHA256 `dedcd04c7e7c6c1570bd2f658ee503f144fd1d388e545620ad43849ca5f5cccf`），同时删除七个空占位。两批共同覆盖当前测试树，但不能把 1282 宣称为一次全量命令结果。
- 16 个历史空占位的现状：Browser 五项与 DAG 四项已由上述真实挂载行为替代；Coordinator 的完成／进度／终态清理／告警有实际状态流覆盖；旧 clearCoordinatorEvents／panelVisible setter 在当前两仓均无生产消费者，其两个空预留删除并明确记为过时，未虚构产品验证。当前 src 无空 skip。历史名称与替代范围逐项保留在 product-acceptance-matrix.json。
- 此生产树 lint、独立 tsc **exit 0**（`/tmp/zk-browser-dag-lint.txt`、`/tmp/zk-browser-dag-typecheck.txt`）；build **21.89 秒通过**（`/tmp/zk-browser-replay-final-build.txt`，SHA256 `97702289b9048df7b45d2a21cbfde9e5168352725d8183ee90f6540ab4ef1116`）。最后 Coordinator 测试另执行 ESLint。没有为后续纯文档更新重复全量构建。

### 最终真实产物验收入口

工作目录为 frontend；实际普通 shell Node `22.14.0`／npm `10.9.2`，项目 Python `.venv/bin/python` `3.11.15`，桌面 Chrome channel `154.0.8037.98`。工具自行解析的受管 Node 身份与普通 shell 分别记录，不混写。

1. `ZK_E2E_SERVER_BINARY=/绝对路径/zk-server npm run test:e2e:production`：目前六项，真实 Rust／隔离 SQLite／本地 scripted provider，不访问收费模型。新增第六项验证真正侧栏入口及 session header／越界拒绝。
2. 同一显式 binary：`npm run test:e2e:analysis`：三项真实 React→Rust→UDS→Python parser 与原生 Git；实际工作目录、`.venv` 与短 UDS 路径由隔离 fixture 配置。
3. `npm run test:theme-regression` 与 `npm run test:jelly-regression`：真实组件与 Tailwind 4 CSS，独立 Chrome，禁止业务请求；不依赖 Python Playwright 下载目录。

最终二进制必须由主任务明确给出，记录绝对路径、版本、运行前后 SHA256 与退出码；当前源码仍在统一 Rust 门禁中，不能复用旧 release 身份。production harness 现对显式不存在／不可执行产物直接失败，不再偷偷换 release 或构建；未指定时保留开发 fallback。其输出前后摘要，并在退出时验证身份相同；Playwright 使用 SIGTERM 30 秒关闭以便真实清理。实际临时脚本测试确认了缺失产物失败、正常产物、原退出码保留、运行中产物变动拒绝与 fixture 清理（`/tmp/zk-production-harness-identity-tests.txt`）；E2E 类型检查通过，新 Rust 整链仍待稳定产物。

最新离线浏览器门禁均实际 exit 0：主题 **20 场景／12728 条实际颜色与隔离断言**（`/tmp/zk-alignment-final-theme2.txt`，SHA256 `f087a3dcd17f3642e89790806966eb39ee052868ada0af15c0a365d21b9ac9b6`）、Jelly **42 场景**（`/tmp/zk-alignment-final-jelly3.txt`，SHA256 `0f3f5e13033729148a98c05e331562f7811337abdc593f9d59c43e47caf43ca2`），均为 Chrome `154.0.8037.98`。构建服务在 finally 显式 stop；一轮观察到断言结束后退出延迟，最终自然 exit 0，未将仅打印 PASS 视作门禁完成。最后 Coordinator ESLint exit 0；机器契约 `python3 scripts/parity/check_contracts.py` exit 0（`/tmp/zk-frontend-final-contract-check.txt`）。


### 全仓首轮测试暴露的夹具与契约修正

`/tmp/zk-alignment-workspace-tests-first.txt` 中本域实际通过：BrowserReplay API 2、memory/history API 16、snapshot API 4、Hooks API 3、typed analysis API 4。首轮仍失败的 Rewind 单测是 SQL 将 BLOB 列污染为 TEXT 后又以 TEXT 恢复，现按真实仓储 BLOB 格式恢复，再按封存 ID 替换；保留损坏快照拒绝、有效但已更换快照拒绝和文件不变断言。Bash CWD 单测移除预建 child session，由 create_task_with_run 原事务创建并继承临时作用域，保留真实预算和授权 CWD 变化拒绝。上述修正尚待统一复跑。

工具 schema 逐项核对后与本次实际编译值 `9cf070a498aea955528188ea7b15fa1fd77a706ca30cb975b3d5d316c770a95c` 同步；Visualization 保留旧 diagram_type/content 三载体，并增加七个受限 viewType/props 契约，intentOnly 不冒充分析或新授权。默认工具仍为 39；四个受管 Task 查询／停止工具常驻，Agent／TaskCreate／TaskUpdate／Worktree 原门控不放开。帮助清单补已真实接线的 context／code-search；OpenAPI 固定 82 并显式核查新增路径。结构契约与 diff-check 通过，Rust 黄金与修改后专项等待下一执行批次。


## 复杂度与 Git 时间线真实入口收敛（2026-10-07）

- 复杂度组件新增明确首次分析表单，消费源提示仅预填；未指定目录使用当前会话，指定目录只能匹配已授权 Project。请求携 scope/requestId，重复、切会话、离开面板都会取消并丢弃迟到。刷新使用封存的真实请求参数，禁止将树显示名称误当目录。保留 Treemap、钻取、语言／风险筛选；明确启发式指标并显示 500 文件截断状态。
- Rust `/api/code-quality/complexity` 采用类型化授权适配，验证根目录、目标范围、四种实际支持语言，返回前重新核对 Session／Project。UDS 响应须有真实指标、有限层级／节点、根内路径、非验证证据标记；拒绝伪造结构和越界路径。
- Python 复杂度复用既有 owner-bound killable worker、取消 tombstone、内存字节指纹缓存及 deadlines。UTF-8／解析失败真实报错，不降为零复杂度成功；统一全树文件上限并报告 truncated；修复 radon block 类型与 tree-sitter UTF-8 字节位置处理。依旧是静态指标，不推断动态程序正确性。
- Git 的 log/diff/blame/cancel 改为 Rust 原生已监督进程，既有公开路径和 snake_case 结果兼容。只读范围来自保存的 Session／Project，旧客户端 repo_path 只能验证同根，不能自授权。rev-parse 固定提交 SHA，后续分页绑定首次 head，首个提交无需不存在的父提交；文件参数精确匹配，历史 symlink 不作普通文本读。禁 hooks、textconv、external diff、lazy fetch，输出／请求数／总时限有界；不新增 TaskRuntime、不自动提交或网络取回。
- Job RAII 在取消／HTTP future drop 时通知既有实际 process supervisor，取消接口只确认请求，不伪造收尾完成。最终响应再次验证 scope；结果不入 SQLite／日志正文。临时会话使用同内存请求注册表。旧无授权 Python Git／复杂度通用代理旁路被拒绝，其余 utility proxy 的 query、body、错误和 disabled 语义继续测试。
- React Git 面板显式展示 diff/blame 失败，切会话卸载旧详情；HEAD 在后续有新提交也不移动当前分页。调用路径／API Sequence 另外由协作工作线补真实 hint 表单、record 身份、移动入口和跨会话清理，纳入同次前端门禁。
- 本域新专项 **2 files /8 tests** 通过（`/tmp/zk-git-complexity-ui-focused.txt`，SHA256 `f1806cef6c7cb59195b4a91011abcda59325a60be716a4f5f36f152177aeb91d`）；Git 真实 REST 六项由主任务执行通过（`/tmp/zk-alignment-git-read-api-current.txt`）。主任务 `/tmp/zk-alignment-workspace-tests-final.txt` 已实际执行通过 Git 在途取消／HTTP future drop 两项（确认真实 PID 消失）和 typed analysis 五项（含复杂度）。此完整 workspace 门禁现已 exit 0：145 个独立 harness、3138 通过、0 失败、9 ignored（排除已计入外层的一个 self-reexec 子进程结果）。主任务另行运行其中四项本机验收通过；五项收费／凭证依赖测试仍未运行，不计为通过。日志 SHA256 `d7e0c4cc106318b9d13f2b82b5730d6211d3bc4d052372327130028c96c8dc98`；本域不替代主任务对整个发布矩阵的判断。

冻结后的离线 browser 再验收全部自然 exit 0：`/tmp/zk-frontend-alignment-frozen-theme.txt` **20 场景／12728 断言**，SHA256 `f087a3dcd17f3642e89790806966eb39ee052868ada0af15c0a365d21b9ac9b6`；`/tmp/zk-frontend-alignment-frozen-jelly.txt` **42 场景**，SHA256 `0f3f5e13033729148a98c05e331562f7811337abdc593f9d59c43e47caf43ca2`。均为 Chrome 154.0.8037.98，无业务请求；不是最终 Rust 产品 E2E 的替代。


## 首次发布二进制验收结果（2026-10-07；模型适配修正前历史）

- 固定 release 路径：`/Users/guoqingtao/Desktop/dev/code/zkcode/target/release/zk-server`。版本输出 `zk-server 0.1.0 (git 08cbdc45e12cc28f3f13aaafbd0b056b980dfb1a, built 1791332366, schema 2, ws 4)`；版本字段使用本地 HEAD，未将未提交源码伪称为新 Git commit。整个验收前后 SHA256 均为 `822adfed182e047c0cc31ac4b9e1c41a14d474f19d249f331f27aa83a92a6315`。
- 产品链路 **6/6 通过，24.8 秒，exit 0**：`/tmp/zk-alignment-release-production-e2e-final.txt`，SHA256 `ce95505eec2f4b4dc8ba5918e93a016b03b73cd7ca3e70afdb93fc2c1ad150ba`。覆盖真实 Rust 聊天落库／恢复、图表与公式、Skill／CAS 记忆／设置、费用账本下的合并与权限继承、真实键盘和弦／Vim／effort 设置与请求 wire，以及会话归属的 Replay 页面。
- 分析链路 **3/3 通过，58.0 秒，exit 0**：`/tmp/zk-alignment-release-analysis-e2e.txt`，SHA256 `470364627f8867e43dbbd0fa59b74217c10928963ea86d26ce275e68e6fe65af`。真实 React→Rust→UDS→Python 代码图／调用链／变更影响／复杂度，真实临时 Git 仓库 log／root commit Diff／Blame，以及授权别名／预取消／真实 OpenAPI 来源。创建 Git commit 的唯一目录是 `/tmp/zk-analysis-e2e-*` 中的测试 workspace。
- 第一次 production 运行 **5 通过／1 失败**（`/tmp/zk-alignment-release-production-e2e.txt`），原因是旧测试误要求 Qwen 支持 `low`；新真实能力策略明确不支持该组合。修正仅涉及 `production-backend.spec.ts`、`scripted-openai-provider.mjs` 和 `start-production-e2e-server.sh`：保留 Qwen-low 拒绝 400、设置不变断言；额外显式注册指向同一 loopback 的 DeepSeek 模型，确认 low 和 thinking=enabled 确实到达 HTTP wire，再验证持久化、auto 和新会话隔离。两个 fixture provider 均不访问远端，不降低生产能力判断。
- 修正后的整个六项套件重跑通过。独立 ESLint 使用项目相同规则实际匹配两个 fixture 文件（不是 ignored），**0 errors /0 warnings /exit 0**（`/tmp/zk-production-fixture-final-lint-applied.txt`）；shell／Node syntax 与 E2E TypeScript 通过。较早直接 ESLint 因默认 src 匹配而忽略 E2E 的调用不计为有效 lint。生产 src 树未变，因此此前冻结的 156/1295 unit、lint、build、theme/Jelly 结果继续适用。
- 身份、运行结果及首次失败的机器记录：`/tmp/zk-final-browser-binary-evidence.json`。上述测试覆盖具体本地产品链路，不替代其他工作线的实际模型凭证／费用网络验收，也不把静态分析说成项目测试成功。全部改动仍未提交或推送。


## 模型适配修正后的最终重验（2026-10-07）

运行时工作线修正真实模型 thinking 适配并完成新的 Rust 门禁后，使用新 release 原路径完整重验。前端／Python 源码与此前已修正测试夹具均未再改动；本轮没有跳过或重试失败案例。

| 套件 | 结果 | 日志 SHA256 |
|---|---|---|
| production | **6/6 通过，29.1 秒，exit 0**；`/tmp/zk-post-adapter-production-e2e.txt` | `cfbe74f16458554432cc4b94cd39d982c7a8ff41883c79b6733baa8920fdad7f` |
| analysis | **3/3 通过，30.6 秒，exit 0**；`/tmp/zk-post-adapter-analysis-e2e.txt` | `b52821d6e8d9eecbe3336bf29100912a0e8af0cfb43a4ac9a1dbe45eccd7728e` |

实际 `/Users/guoqingtao/Desktop/dev/code/zkcode/target/release/zk-server` 前后 SHA256 均为 **`cca0d0a64e2ab3bad9bf95fdc22fcb02f278d89c8b9e78286d4c0960a89e3251`**，两套 harness 也各自记录 unchanged=true。`--version` exit 0：`zk-server 0.1.0 (git 08cbdc45e12cc28f3f13aaafbd0b056b980dfb1a, built 1791332366, schema 2, ws 4)`；版本文字未变化时仍以实际可执行文件摘要区分产物。完整记录为 `/tmp/zk-post-adapter-browser-binary-evidence.json`。先前 `822ad…` 记录及首次过时 fixture 的失败原因继续保留为历史；真实收费模型验证由运行时工作线另行记录，不能与此处 loopback E2E 混同。
