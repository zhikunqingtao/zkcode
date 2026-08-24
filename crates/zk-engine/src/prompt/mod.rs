//! 项目提示（`PROJECT.md`）六层加载 + 5s 轮询热重载。
//!
//! 逐字对照旧仓库两处实现：
//!
//! - [`ProjectPromptLoader`] ← `com.aicodeassistant.config.ProjectPromptLoader`
//!   （六层加载 + `rules/*.md` 展开 + `@include` 展开 + 60s TTL 缓存 +
//!   合并纯文本产出）。
//! - [`ProjectPromptWatcher`] ← `com.aicodeassistant.service.SettingsWatcher`
//!   （配置变更自动触发重载）。
//!
//! # 六层来源与顺序（逐字对照旧 `loadAll`）
//!
//! 旧源把各层**按插入顺序拼接**（`loadMergedContent` 仅在段间插 `\n---\n`，
//! 无覆盖/替换语义——所谓「优先级」只是拼接先后，不是键级覆盖）。顺序为：
//!
//! 1. 用户级 `~/.zk/PROJECT.md`（label `user`）；
//! 2. 项目级 `{cwd}/PROJECT.md`（label `project`）；
//! 3. 项目级 `{cwd}/.zk/PROJECT.md`（label `project-local`）；
//! 4. 本地覆盖 `{cwd}/.zk/PROJECT.local.md`（label `local`）；
//! 5. 规则目录 `{cwd}/.zk/rules/*.md`（按路径升序，label `rule:<文件名>`）；
//! 6. 父目录向上遍历（最多 [`MAX_PARENT_DEPTH`] 层），每层先
//!    `PROJECT.md`（label `parent-<n>`）后 `.zk/PROJECT.md`
//!    （label `parent-local-<n>`）。
//!
//! 拼接完成后统一做 `@include` 展开（相对 `cwd` 解析、上限
//! [`MAX_INCLUDE_BYTES`]）。所有目录经 [`zk_core::paths`] 解析，`.zk`/`.zhikun`
//! 字面量不在本模块出现（旧源硬编码 `.zhikun`，Rust 侧改由单一事实源给出，
//! 因此新目录名为 `.zk`——这是全仓库既定迁移，不属本步臆造）。
//!
//! # CLI arg 层的处理（预留入口）
//!
//! 已批准计划的抽象六层写作「CLI arg > env var > project > parent > user >
//! default」，但**旧 Java 基线并无 CLI arg / env var / default 三层**——它只有
//! 上述六个文件来源。铁律要求 100% 还原旧实现，故本步不臆造这三层的合并语义；
//! 仅为将来（Batch 1 系统提示动态段框架接入 CLI 时）预留一个**惰性入口**：
//! [`ProjectPromptLoader::load_all_with_cli`] 接受 `cli_override: Option<&str>`，
//! 传 `None`（当前所有真实调用路径，因 Rust 侧尚无 CLI 层）时与
//! [`ProjectPromptLoader::load_all`] 逐字等价；传 `Some` 时把它作为最高优先级
//! 段（label `cli`）前置。env var / default 层同理暂不落地，待真实来源出现再补。
//!
//! # 与 [`crate::system_prompt`] 的关系
//!
//! [`ProjectPromptLoader`] 只负责「加载 + 热重载」，其合并文本作为系统提示的
//! `project_context` 动态段被消费。动态段组装框架（Batch 1 Step 1-3）落在
//! [`dynamic`] 子模块：它产出动态后缀，经
//! [`crate::system_prompt::build_system_prompt_segmented`] 与静态前缀拼为分段
//! 系统提示；静态段逻辑本身不改。

pub mod dynamic;
mod project_prompt;
mod watcher;

pub use dynamic::{
    CacheScope, DISCOVER_SKILLS_TOOL, DynamicSectionContext, NUMERIC_LENGTH_ANCHORS_FLAG, REGISTRY,
    SKILL_DISCOVERY_FLAG, SectionDescriptor, SectionState, render_all, render_suffix,
};
pub use project_prompt::{
    CACHE_TTL, MAX_INCLUDE_BYTES, MAX_PARENT_DEPTH, PROJECT_LOCAL_MD_FILE, PROJECT_MD_FILE,
    ProjectMdSection, ProjectPromptLoader, RULES_DIR_NAME,
};
pub use watcher::{ProjectPromptWatcher, WATCH_POLL_INTERVAL};
