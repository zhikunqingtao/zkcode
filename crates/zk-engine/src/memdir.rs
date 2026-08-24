//! Memdir 用户级长期记忆——跨会话持久化的 `MEMORY.md` 存储 + BM25 检索
//! （Batch 5 Step 2）。
//!
//! 逐字对照旧 `memdir/MemdirService.java`（599 行）、`memdir/MemorySearchEngine.java`
//! （198 行）、`memdir/MemoryCategory.java`（只读权威规格）。
//!
//! # 存储形态
//!
//! 单一入口文件 `{memory_dir}/MEMORY.md`，追加式写入；每条记忆前置一行 HTML
//! 注释头 `<!-- source:XXX time:ISO category:xxx -->`，解析时以该标记的**零宽
//! 前视**切段（旧 `content.split("(?=<!-- source:)")`）。
//!
//! 目录默认 `~/.zk/`（经 [`zk_core::paths::user_config_dir`]）——旧实现默认
//! `~/.ai-code-assistant/`，路径迁移属 Step 0-1 的 `.zk/` 统一裁定，非本步
//! 行为差异。
//!
//! # 三重体积护栏（旧常量逐字保留）
//!
//! - [`MAX_ENTRYPOINT_LINES`] = 200 行：注入提示前按行截断；
//! - [`MAX_ENTRYPOINT_BYTES`] = 25 000 字节：再按字节回退到最近换行处截断；
//! - [`MAX_MEMORY_SIZE`] = 50 000 字符：写入前若 `已有 + 新增` 越界，先压缩
//!   （保留最新 70%）。
//!
//! 注意旧注释写「25KB (25,600 bytes)」但常量实为 `25_000`——本移植以**常量值**
//! 为准（截断标记文案里也内插该值，故文案同为 `exceeded 25000 bytes`）。
//!
//! # 系统提示注入：本模块**不**参与
//!
//! `readMemoriesForPrompt` / `buildMemoryPrompt` / `searchMemories` /
//! `searchByCategory` / `purgeExpiredMemories` / `loadTeamMemories` /
//! `saveMemory` / `loadRelevantMemories` 在旧仓**全库零生产调用点**（只被
//! `MemdirGoldenTest` / `MemorySystemTest` 覆盖）：`SystemPromptBuilder` 的
//! `memory` 段只调 `ProjectMemoryService.loadMemory`，与 `MemdirService` 无关；
//! `MemdirService` 的生产入口仅 `MemoryTool`（read / write / delete）与
//! `MemoryController`（`listEntries`）两处。
//!
//! 依既有裁定「Java 死代码方法不得接入主循环 / 生产路径」，本模块**不**接系统
//! 提示 `memory` 段（该段数据源为 [`crate::project_memory`]，与旧仓一致）；
//! 能力本身全部实现并由 `Memory` 工具与 `/api/memory*` 端点驱动，不缩水。
//!
//! # 未移植（本批范围外）
//!
//! - `MemoryRerankService`（LLM 精排）：P2。旧 `searchMemories` 里的 rerank
//!   分支恒不生效（Bean 可选注入且默认缺失），故 BM25 候选集上限恒为 `topK`
//!   ——本移植直接取 `topK`，与旧实现在无 rerank 时**逐字等价**。
//! - 团队记忆（`loadTeamMemories` 扫 `.zhikun/team-memories/`）：P2。
//!
//! # Java 语义等价说明（一次性声明）
//!
//! 空白判定与 trim 用 Rust 的 Unicode 空白定义，旧仓用
//! `String.isBlank()` / `String.trim()`。两者仅在 U+00A0 等少数码位上不同，
//! 而这些码位不会出现在本模块生成的注释头里，故视为等价，不另造 Java 语义副本。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use zk_db::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};

// ==================== 常量（旧 MemdirService 逐字） ====================

/// 入口文件名（旧 `ENTRYPOINT_NAME`，必须全大写）。
pub const ENTRYPOINT_NAME: &str = "MEMORY.md";
/// 注入提示时的最大行数（旧 `MAX_ENTRYPOINT_LINES`）。
pub const MAX_ENTRYPOINT_LINES: usize = 200;
/// 注入提示时的最大字节数（旧 `MAX_ENTRYPOINT_BYTES`）。
pub const MAX_ENTRYPOINT_BYTES: usize = 25_000;
/// 触发压缩的字符数上限（旧 `MAX_MEMORY_SIZE`）。
pub const MAX_MEMORY_SIZE: usize = 50_000;
/// 压缩保留比例（旧 `COMPACT_KEEP_RATIO`）。
const COMPACT_KEEP_RATIO: f64 = 0.7;
/// 记忆最大存活天数（旧 `MAX_MEMORY_AGE_DAYS`）。
pub const MAX_MEMORY_AGE_DAYS: i64 = 90;
/// 条目切段标记（旧零宽前视 `(?=<!-- source:)` 的字面部分）。
const SECTION_MARKER: &str = "<!-- source:";
/// 一天的毫秒数（`purge_expired` 的 cutoff 计算）。
const MILLIS_PER_DAY: i64 = 86_400_000;

// ==================== 枚举 ====================

/// 记忆分类（旧 `MemoryCategory` 四值枚举）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemoryCategory {
    /// 情景记忆。
    Episodic,
    /// 语义记忆（缺省值）。
    Semantic,
    /// 程序性记忆。
    Procedural,
    /// 团队记忆。
    Team,
}

impl MemoryCategory {
    /// 注释头里的小写标签（旧 `tag()`）。
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::Episodic => "episodic",
            Self::Semantic => "semantic",
            Self::Procedural => "procedural",
            Self::Team => "team",
        }
    }

    /// 由标签解析（旧 `fromTag`）：`None` 与未知标签一律回落
    /// [`MemoryCategory::Semantic`]，匹配大小写不敏感。
    #[must_use]
    pub fn from_tag(tag: Option<&str>) -> Self {
        let Some(tag) = tag else {
            return Self::Semantic;
        };
        for candidate in [Self::Episodic, Self::Semantic, Self::Procedural, Self::Team] {
            if candidate.tag().eq_ignore_ascii_case(tag) {
                return candidate;
            }
        }
        Self::Semantic
    }
}

/// 记忆来源（旧 `MemdirService.MemorySource`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemorySource {
    /// LLM 自动记录。
    Auto,
    /// 用户手动编辑。
    User,
    /// 通过工具记录。
    Tool,
}

impl MemorySource {
    /// 枚举名（旧 `source.name()`，全大写，写入注释头用）。
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "AUTO",
            Self::User => "USER",
            Self::Tool => "TOOL",
        }
    }

    /// 由枚举名解析（旧 `MemorySource.valueOf`，**区分大小写**）；
    /// 旧实现捕获 `IllegalArgumentException` 后回落 `AUTO`。
    #[must_use]
    pub fn parse(name: &str) -> Self {
        match name {
            "USER" => Self::User,
            "TOOL" => Self::Tool,
            _ => Self::Auto,
        }
    }
}

// ==================== 记录类型 ====================

/// 解析出的记忆条目（旧 `MemdirService.MemoryEntry` record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryEntry {
    /// 来源标记。
    pub source: MemorySource,
    /// 写入时刻（epoch 毫秒；解析失败或无头部时为 0 = `Instant.EPOCH`）。
    pub timestamp_millis: i64,
    /// 正文（已 trim）。
    pub content: String,
    /// 分类。
    pub category: MemoryCategory,
}

/// 检索结果（旧 `MemdirService.Memory` record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Memory {
    /// 条目名（旧 `source.name() + "_" + timestamp.getEpochSecond()`）。
    pub name: String,
    /// 正文。
    pub content: String,
    /// 分类。
    pub category: MemoryCategory,
}

/// 写入失败（旧 `MemdirService.MemdirException`）。
#[derive(Debug, thiserror::Error)]
#[error("Failed to write memory: {0}")]
pub struct MemdirError(#[from] std::io::Error);

// ==================== 存储 ====================

/// Memdir 存储句柄（旧 `MemdirService`）。
///
/// 写路径由内部 [`tokio::sync::Mutex`] 串行化（旧 `ReentrantLock writeLock`）：
/// `read → compact? → write tmp → rename` 是读改写序列，并发写入必须互斥。
pub struct MemdirStore {
    /// 记忆目录（写入前 `create_dir_all`）。
    memory_dir: PathBuf,
    /// 入口文件（`{memory_dir}/MEMORY.md`）。
    memory_file: PathBuf,
    /// 写入互斥（旧 `writeLock`）。
    write_lock: tokio::sync::Mutex<()>,
}

impl MemdirStore {
    /// 用户级默认目录 `~/.zk/`（旧默认构造器的 `~/.ai-code-assistant/`）。
    #[must_use]
    pub fn new() -> Self {
        Self::with_dir(zk_core::paths::user_config_dir())
    }

    /// 指定记忆目录（旧可测试构造器）。
    #[must_use]
    pub fn with_dir(memory_dir: PathBuf) -> Self {
        let memory_file = memory_dir.join(ENTRYPOINT_NAME);
        Self {
            memory_dir,
            memory_file,
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// 入口文件路径（旧 `getMemoryFile`）。
    #[must_use]
    pub fn memory_file(&self) -> &Path {
        &self.memory_file
    }

    /// 读取全部记忆原文（旧 `readMemories`）。
    ///
    /// 文件不存在或读取失败一律返回空串（旧实现 `log.error` 后返回 `""`）。
    pub async fn read_memories(&self) -> String {
        match tokio::fs::read_to_string(&self.memory_file).await {
            Ok(content) => content,
            Err(error) => {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::error!(
                        file = %self.memory_file.display(),
                        %error,
                        "failed to read memory file"
                    );
                }
                String::new()
            }
        }
    }

    /// 读取并施加双重截断（旧 `readMemoriesForPrompt`）。
    ///
    /// 本方法在旧仓无生产调用点（见模块文档），保留以完整覆盖护栏语义。
    pub async fn read_memories_for_prompt(&self) -> String {
        truncate_for_prompt(&self.read_memories().await)
    }

    /// 全部条目（旧 `listEntries`）。
    pub async fn list_entries(&self) -> Vec<MemoryEntry> {
        parse_entries(&self.read_memories().await)
    }

    /// 条目数量（旧 `getEntryCount`）。
    pub async fn entry_count(&self) -> usize {
        self.list_entries().await.len()
    }

    /// 追加一条记忆（旧 `writeMemory(content, source, category)`）。
    ///
    /// 流程：建目录 → 读已有 → 越界则压缩 → 拼注释头条目 → 写
    /// `MEMORY.md.tmp` → 原子 rename 覆盖。
    ///
    /// # Errors
    ///
    /// 建目录 / 写临时文件 / rename 任一失败时返回 [`MemdirError`]
    /// （旧实现抛 `MemdirException`）。
    pub async fn write_memory(
        &self,
        content: &str,
        source: MemorySource,
        category: MemoryCategory,
    ) -> Result<(), MemdirError> {
        let _guard = self.write_lock.lock().await;
        tokio::fs::create_dir_all(&self.memory_dir).await?;

        let mut existing = self.read_memories().await;
        // 旧 `existing.length() + content.length() > MAX_MEMORY_SIZE`
        // （Java String.length 为 UTF-16 单元数；此处取字符数，BMP 内等价）。
        if existing.chars().count() + content.chars().count() > MAX_MEMORY_SIZE {
            existing = compact_memories(&existing);
            tracing::info!("memories compacted due to size limit");
        }

        let entry = format!(
            "\n<!-- source:{} time:{} category:{} -->\n{content}\n",
            source.name(),
            format_rfc3339_micros(now_millis()),
            category.tag()
        );
        self.replace_file(&format!("{existing}{entry}")).await?;
        tracing::info!(
            source = source.name(),
            category = category.tag(),
            length = content.len(),
            "memory written"
        );
        Ok(())
    }

    /// 追加一条 `SEMANTIC` 记忆（旧 `writeMemory(content, source)` 重载）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::write_memory`]。
    pub async fn write_semantic(
        &self,
        content: &str,
        source: MemorySource,
    ) -> Result<(), MemdirError> {
        self.write_memory(content, source, MemoryCategory::Semantic)
            .await
    }

    /// 按名称保存（旧 `saveMemory`：正文前置 `## {name}` 标题，来源恒 `TOOL`）。
    ///
    /// 旧仓无生产调用点，保留以完整覆盖写入形态。
    ///
    /// # Errors
    ///
    /// 同 [`Self::write_memory`]。
    pub async fn save_memory(&self, name: &str, content: &str) -> Result<(), MemdirError> {
        self.write_semantic(&format!("## {name}\n{content}"), MemorySource::Tool)
            .await
    }

    /// 删除正文包含 `pattern`（大小写不敏感）的全部条目（旧 `deleteMemory`）。
    ///
    /// 返回是否真的删掉了条目；文件为空、无匹配、或写回失败一律 `false`
    /// （旧实现 IO 失败时 `log.error` 后返回 `false`，不抛）。
    pub async fn delete_memory(&self, pattern: &str) -> bool {
        let _guard = self.write_lock.lock().await;
        let content = self.read_memories().await;
        if content.is_empty() {
            return false;
        }
        let updated = remove_matching_entries(&content, pattern);
        if updated == content {
            return false;
        }
        if let Err(error) = self.replace_file(&updated).await {
            tracing::error!(%error, "failed to delete memory");
            return false;
        }
        tracing::info!(pattern, "memory deleted");
        true
    }

    /// 清理超过 [`MAX_MEMORY_AGE_DAYS`] 的条目（旧 `purgeExpiredMemories`）。
    ///
    /// 返回被清理的条目数。无头部标记的条目（时间戳为 `EPOCH`）**永不**过期
    /// ——旧实现的 `|| e.timestamp().equals(Instant.EPOCH)` 豁免。
    ///
    /// 收紧（留痕）：旧实现此方法**不持** `writeLock` 且直写目标文件（非
    /// tmp + rename），与并发 `writeMemory` 交错会丢条目或写出半截文件。本
    /// 移植持同一把写锁并走原子替换；保留条目集与旧实现一致。
    pub async fn purge_expired(&self) -> usize {
        let _guard = self.write_lock.lock().await;
        let content = self.read_memories().await;
        if content.is_empty() {
            return 0;
        }
        let entries = parse_entries(&content);
        let cutoff = now_millis() - MAX_MEMORY_AGE_DAYS * MILLIS_PER_DAY;
        let remaining: Vec<&MemoryEntry> = entries
            .iter()
            .filter(|entry| entry.timestamp_millis > cutoff || entry.timestamp_millis == 0)
            .collect();
        let purged = entries.len() - remaining.len();
        if purged > 0 {
            let updated = join_entries(remaining.into_iter());
            if let Err(error) = self.replace_file(&updated).await {
                tracing::error!(%error, "failed to purge memories");
            } else {
                tracing::info!(purged, "purged expired memories");
            }
        }
        purged
    }

    /// BM25 语义检索（旧 `searchMemories`）。
    ///
    /// 文档标题取正文首行的 markdown 标题（旧 `extractTitle`），条目名沿用
    /// 旧 `source_epochSeconds` 拼法。rerank 未移植，故候选集上限恒为 `top_k`
    /// （见模块文档）。旧仓无生产调用点。
    pub async fn search_memories(&self, query: &str, top_k: usize) -> Vec<Memory> {
        let content = self.read_memories().await;
        if content.is_empty() {
            return Vec::new();
        }
        let entries = parse_entries(&content);
        if entries.is_empty() {
            return Vec::new();
        }
        let documents: Vec<DocumentEntry> = entries
            .iter()
            .map(|entry| DocumentEntry {
                title: extract_title(&entry.content),
                body: entry.content.clone(),
            })
            .collect();
        search_bm25(&documents, query, top_k)
            .into_iter()
            .map(|scored| to_memory(&entries[scored.index]))
            .collect()
    }

    /// 按分类过滤，时间倒序取前 `max_count` 条（旧 `searchByCategory`）。
    ///
    /// 旧仓无生产调用点。
    pub async fn search_by_category(
        &self,
        category: MemoryCategory,
        max_count: usize,
    ) -> Vec<Memory> {
        let content = self.read_memories().await;
        if content.is_empty() {
            return Vec::new();
        }
        let mut matched: Vec<MemoryEntry> = parse_entries(&content)
            .into_iter()
            .filter(|entry| entry.category == category)
            .collect();
        // 旧 `Comparator.comparing(timestamp).reversed()`：稳定排序，等值保序。
        matched.sort_by_key(|entry| std::cmp::Reverse(entry.timestamp_millis));
        matched.iter().take(max_count).map(to_memory).collect()
    }

    /// 原子替换入口文件：写 `MEMORY.md.tmp` → rename（旧 `Files.move` 的
    /// `ATOMIC_MOVE + REPLACE_EXISTING`）。
    async fn replace_file(&self, content: &str) -> Result<(), std::io::Error> {
        let mut temp = self.memory_file.clone().into_os_string();
        temp.push(".tmp");
        let temp = PathBuf::from(temp);
        tokio::fs::write(&temp, content).await?;
        tokio::fs::rename(&temp, &self.memory_file).await
    }
}

impl Default for MemdirStore {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 纯函数：解析 / 截断 / 压缩 ====================

/// 条目 → [`Memory`]（旧 `source.name() + "_" + timestamp.getEpochSecond()`）。
fn to_memory(entry: &MemoryEntry) -> Memory {
    Memory {
        name: format!(
            "{}_{}",
            entry.source.name(),
            // Java `Instant.getEpochSecond()` 向下取整（EPOCH 前为负）。
            entry.timestamp_millis.div_euclid(1000)
        ),
        content: entry.content.clone(),
        category: entry.category,
    }
}

/// 零宽前视切段（旧 `content.split("(?=<!-- source:)")`）。
///
/// Java `Pattern.split` 对**位置 0 的零宽匹配不产生前导空串**，本实现同。
fn split_sections(content: &str) -> Vec<&str> {
    let mut sections = Vec::new();
    let mut start = 0usize;
    let mut search = 0usize;
    while let Some(offset) = content[search..].find(SECTION_MARKER) {
        let index = search + offset;
        if index > start {
            sections.push(&content[start..index]);
        }
        start = index;
        search = index + SECTION_MARKER.len();
    }
    sections.push(&content[start..]);
    sections
}

/// 解析条目头 `<!-- source:(\w+) time:(\S+)(?: category:(\w+))? -->`。
///
/// 返回 `(source, time, category, 头部结束偏移)`；未匹配返回 `None`。
/// 手写扫描而非引入 regex 依赖：该头部由本模块自己生成、格式固定，`\w+` /
/// `\S+` 两类字符集判定用 `char` 谓词逐字表达即可。
fn match_entry_header(section: &str) -> Option<(&str, &str, Option<&str>, usize)> {
    // `m.find()`：头部可以不在段首（旧实现用 find 而非 matches）。
    let head = section.find(SECTION_MARKER)?;
    let rest = &section[head + SECTION_MARKER.len()..];
    let source_len = rest.find(|c: char| !is_word_char(c))?;
    if source_len == 0 {
        return None;
    }
    let source = &rest[..source_len];
    let rest = &rest[source_len..];
    let rest = rest.strip_prefix(" time:")?;
    // `\S+` 贪婪匹配后需回溯以容纳 ` -->` 或 ` category:x -->`，故先切出
    // 到下一个空白为止的整段，再判定其是否落在合法收尾上。
    let time_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
    if time_len == 0 {
        return None;
    }
    let time = &rest[..time_len];
    let rest = &rest[time_len..];
    let (category, tail) = if let Some(after) = rest.strip_prefix(" category:") {
        let len = after.find(|c: char| !is_word_char(c))?;
        if len == 0 {
            return None;
        }
        (Some(&after[..len]), &after[len..])
    } else {
        (None, rest)
    };
    let tail = tail.strip_prefix(" -->")?;
    let end = section.len() - tail.len();
    Some((source, time, category, end))
}

/// Java 正则 `\w` 字符集（`[a-zA-Z0-9_]`）。
fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// 解析全部条目（旧 `parseEntries`）。
///
/// 无头部标记的段落落 `(USER, EPOCH, trim 后的整段, SEMANTIC)`——旧实现对
/// 手写进 `MEMORY.md` 的裸文本的兼容分支。
#[must_use]
pub fn parse_entries(content: &str) -> Vec<MemoryEntry> {
    if content.trim().is_empty() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    for section in split_sections(content) {
        if section.trim().is_empty() {
            continue;
        }
        if let Some((source, time, category, end)) = match_entry_header(section) {
            entries.push(MemoryEntry {
                source: MemorySource::parse(source),
                // 旧 `Instant.parse` 失败回落 `Instant.EPOCH`。
                timestamp_millis: parse_rfc3339_millis(time).unwrap_or(0),
                content: section[end..].trim().to_owned(),
                category: MemoryCategory::from_tag(category),
            });
        } else {
            entries.push(MemoryEntry {
                source: MemorySource::User,
                timestamp_millis: 0,
                content: section.trim().to_owned(),
                category: MemoryCategory::Semantic,
            });
        }
    }
    entries
}

/// 条目回写格式（旧三处 `String.format("<!-- source:%s time:%s category:%s -->\n%s")`
/// 以 `"\n\n"` 连接；注意与追加写入的格式不同——回写**无**前导换行）。
fn join_entries<'a>(entries: impl Iterator<Item = &'a MemoryEntry>) -> String {
    entries
        .map(|entry| {
            format!(
                "<!-- source:{} time:{} category:{} -->\n{}",
                entry.source.name(),
                format_rfc3339_micros(entry.timestamp_millis),
                entry.category.tag(),
                entry.content
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// 注入提示前的双重截断（旧 `readMemoriesForPrompt` 的两步）。
///
/// 字节截断的 `cutoff == 0` 分支忠实保留旧行为：找不到换行时**不截断**
/// （只记日志），因为旧实现的 `if (cutoff > 0)` 把替换整个跳过了。
#[must_use]
pub fn truncate_for_prompt(content: &str) -> String {
    if content.is_empty() {
        return String::new();
    }
    // 1. 按行截断（`split("\n", -1)` 保留尾部空串，与 Rust `split('\n')` 同）。
    let lines: Vec<&str> = content.split('\n').collect();
    let mut out = if lines.len() > MAX_ENTRYPOINT_LINES {
        tracing::info!(
            from = lines.len(),
            to = MAX_ENTRYPOINT_LINES,
            "memory truncated by lines"
        );
        format!(
            "{}\n<!-- truncated: exceeded {MAX_ENTRYPOINT_LINES} lines -->",
            lines[..MAX_ENTRYPOINT_LINES].join("\n")
        )
    } else {
        content.to_owned()
    };

    // 2. 按字节截断，回退到最近的换行处。
    let bytes = out.as_bytes();
    if bytes.len() > MAX_ENTRYPOINT_BYTES {
        let mut cutoff = MAX_ENTRYPOINT_BYTES;
        while cutoff > 0 && bytes[cutoff] != b'\n' {
            cutoff -= 1;
        }
        tracing::info!(from = bytes.len(), to = cutoff, "memory truncated by bytes");
        if cutoff > 0 {
            // 切点恒为换行字节，故必落在字符边界上。
            out = format!(
                "{}\n<!-- truncated: exceeded {MAX_ENTRYPOINT_BYTES} bytes -->",
                String::from_utf8_lossy(&bytes[..cutoff])
            );
        }
    }
    out
}

/// 压缩：按时间倒序保留最新 70%（旧 `compactMemories`）。
#[must_use]
pub fn compact_memories(existing: &str) -> String {
    let mut entries = parse_entries(existing);
    if entries.is_empty() {
        return String::new();
    }
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.timestamp_millis));
    // 旧 `Math.max(1, (int)(size * 0.7))`：向零截断。
    let keep = keep_count(entries.len());
    join_entries(entries.iter().take(keep))
}

/// 保留条目数（旧 `Math.max(1, (int)(entries.size() * COMPACT_KEEP_RATIO))`）。
///
/// 比例 < 1 且下限为 1，故结果恒落在 `1..=total`；两端先用浮点比较夹取，中间
/// 区间的整数值转换才交给 [`as_usize_trunc`]。
fn keep_count(total: usize) -> usize {
    let scaled = (as_f64(total) * COMPACT_KEEP_RATIO).trunc();
    if scaled <= 1.0 {
        return 1;
    }
    if scaled >= as_f64(total) {
        return total;
    }
    as_usize_trunc(scaled)
}

/// 移除正文包含 `pattern`（小写包含）的条目（旧 `removeMatchingEntries`）。
///
/// 无条目被移除时**原样返回入参**——调用方据此判定「没删成」。
#[must_use]
pub fn remove_matching_entries(content: &str, pattern: &str) -> String {
    let entries = parse_entries(content);
    let needle = pattern.to_lowercase();
    let remaining: Vec<&MemoryEntry> = entries
        .iter()
        .filter(|entry| !entry.content.to_lowercase().contains(&needle))
        .collect();
    if remaining.len() == entries.len() {
        return content.to_owned();
    }
    join_entries(remaining.into_iter())
}

/// 提取 markdown 标题（旧 `extractTitle`）。
///
/// 忠实保留旧仓两处细节：`indexOf('\n')` 返回 0（正文以换行开头）时取**整个**
/// 正文而非空串（`newline > 0` 的判定）；剥离前缀用 `^#+\s*` 正则（无 `#` 时
/// 不改动）。
#[must_use]
pub fn extract_title(content: &str) -> String {
    let first_line = match content.find('\n') {
        Some(0) | None => content,
        Some(index) => &content[..index],
    };
    let stripped = first_line.trim_start_matches('#');
    // `^#+` 要求至少一个 `#`：没剥掉任何字符时不吃后续空白。
    let stripped = if stripped.len() == first_line.len() {
        first_line
    } else {
        stripped.trim_start_matches(is_java_regex_space)
    };
    stripped.trim().to_owned()
}

/// Java 正则 `\s` 字符集（`[ \t\n\x0B\f\r]`）。
fn is_java_regex_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
}

/// `usize` → `f64`（BM25 与压缩比例算术用；记忆条目量级下无精度损失）。
#[allow(clippy::cast_precision_loss)]
fn as_f64(value: usize) -> f64 {
    value as f64
}

/// `f64` → `usize`（旧 Java `(int)` 强转的向零截断）。
///
/// 唯一调用点 [`keep_count`] 已把入参夹在 `(1, total)` 开区间且为整数值，故此处
/// 无实际截断与符号丢失。
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn as_usize_trunc(value: f64) -> usize {
    value as usize
}

// ==================== BM25 检索引擎（旧 MemorySearchEngine 逐字） ====================

/// 词频饱和参数（旧 `K1`）。
const K1: f64 = 1.2;
/// 文档长度归一化权重（旧 `B`）。
const B: f64 = 0.75;
/// 标题匹配加权（旧 `TITLE_BOOST`）。
const TITLE_BOOST: f64 = 2.0;

/// 待检索文档（旧 `MemorySearchEngine.DocumentEntry` record）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentEntry {
    /// 标题（取正文首行 markdown 标题，见 [`extract_title`]）。
    pub title: String,
    /// 正文。
    pub body: String,
}

/// 打分结果（旧 `MemorySearchEngine.ScoredResult` record）。
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoredResult {
    /// 在入参 `entries` 中的下标。
    pub index: usize,
    /// 总分（正文分 + 标题分 × [`TITLE_BOOST`]）。
    pub score: f64,
}

/// 停用词表（旧 `STOP_WORDS`：31 中文 + 20 英文，逐字同集）。
fn stop_words() -> &'static HashSet<&'static str> {
    static WORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();
    WORDS.get_or_init(|| {
        [
            // 中文停用词
            "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都", "一", "一个", "上",
            "也", "很", "到", "说", "要", "去", "你", "会", "着", "没有", "看", "好", "自己", "这",
            "那", "被", "从", // 英文停用词
            "the", "is", "at", "which", "on", "a", "an", "and", "or", "but", "in", "to", "for",
            "of", "with", "it", "this", "that", "are", "was",
        ]
        .into_iter()
        .collect()
    })
}

/// BM25 检索（旧 `MemorySearchEngine.search`）。
///
/// `entries` 为空、`query` 空白、或 query 分词为空一律返回空列表（旧三处提前
/// 返回 `List.of()`）。文档平均长度 `avgDL` 只统计**正文** token 数；标题分用
/// `avgDL / 5.0` 单独计算后乘 [`TITLE_BOOST`] 叠加。
#[must_use]
pub fn search_bm25(entries: &[DocumentEntry], query: &str, top_k: usize) -> Vec<ScoredResult> {
    if entries.is_empty() || query.trim().is_empty() {
        return Vec::new();
    }
    let query_tokens = tokenize(query);
    if query_tokens.is_empty() {
        return Vec::new();
    }

    let mut all_doc_tokens: Vec<Vec<String>> = Vec::with_capacity(entries.len());
    let mut all_title_tokens: Vec<Vec<String>> = Vec::with_capacity(entries.len());
    let mut total_length = 0.0_f64;
    for entry in entries {
        let body_tokens = tokenize(&entry.body);
        total_length += as_f64(body_tokens.len());
        all_title_tokens.push(tokenize(&entry.title));
        all_doc_tokens.push(body_tokens);
    }
    // 旧 `double totalLength` 累加后再除，故为浮点除法（非整除）。
    let avg_dl = total_length / as_f64(entries.len());

    let idf = compute_idf(&query_tokens, &all_doc_tokens, entries.len());

    let mut results: Vec<ScoredResult> = Vec::new();
    for (index, doc_tokens) in all_doc_tokens.iter().enumerate() {
        let body_score = compute_bm25(&query_tokens, doc_tokens, &idf, avg_dl);
        // 标题平均长度远短于正文（旧注释）。
        let title_score = compute_bm25(&query_tokens, &all_title_tokens[index], &idf, avg_dl / 5.0);
        let score = body_score + title_score * TITLE_BOOST;
        if score > 0.0 {
            results.push(ScoredResult { index, score });
        }
    }

    // 旧 `Comparator.comparingDouble(score).reversed()` + `Stream.sorted`：稳定
    // 排序，等分保 index 升序。分值恒为有限实数（IDF 与 BM25 分母恒 > 0），
    // 故 `partial_cmp` 不会返回 `None`。
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(top_k);
    results
}

/// 分词（旧 `tokenize`）：先全量抽英文/数字 token，再抽 CJK 段的 Unigram + Bigram。
///
/// 两趟扫描顺序与旧实现一致（英数 token 恒在前），因 BM25 与顺序无关，此处仅为
/// 逐字保真。
fn tokenize(text: &str) -> Vec<String> {
    if text.trim().is_empty() {
        return Vec::new();
    }
    let mut tokens: Vec<String> = Vec::new();
    let lower = text.to_lowercase();

    // 旧 `WORD_PATTERN`（`[a-zA-Z0-9_]+`）：长度 > 1 且非停用词才收。
    let mut word = String::new();
    for c in lower.chars() {
        if is_word_char(c) {
            word.push(c);
        } else if !word.is_empty() {
            push_word_token(&mut word, &mut tokens);
        }
    }
    if !word.is_empty() {
        push_word_token(&mut word, &mut tokens);
    }

    // 旧 CJK 聚段：非 CJK 字符即为段边界。
    let mut cjk = String::new();
    for c in lower.chars() {
        if is_cjk_unified_ideograph(c) {
            cjk.push(c);
        } else if !cjk.is_empty() {
            extract_cjk_tokens(&cjk, &mut tokens);
            cjk.clear();
        }
    }
    if !cjk.is_empty() {
        extract_cjk_tokens(&cjk, &mut tokens);
    }

    tokens
}

/// 收一个英数 token 并清空缓冲（旧 `word.length() > 1 && !STOP_WORDS.contains`）。
fn push_word_token(word: &mut String, tokens: &mut Vec<String>) {
    if word.chars().count() > 1 && !stop_words().contains(word.as_str()) {
        tokens.push(word.clone());
    }
    word.clear();
}

/// Java `Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS`（U+4E00..=U+9FFF）。
fn is_cjk_unified_ideograph(c: char) -> bool {
    matches!(c, '\u{4e00}'..='\u{9fff}')
}

/// CJK 分词（旧 `extractCJKTokens`）：Unigram **过**停用词，Bigram **不过**。
fn extract_cjk_tokens(cjk_text: &str, tokens: &mut Vec<String>) {
    // CJK 统一表意文字全在 BMP 内，Java `charAt` 逐 UTF-16 单元与逐 char 等价。
    let chars: Vec<char> = cjk_text.chars().collect();
    for c in &chars {
        let unigram = c.to_string();
        if !stop_words().contains(unigram.as_str()) {
            tokens.push(unigram);
        }
    }
    for window in chars.windows(2) {
        tokens.push(window.iter().collect());
    }
}

/// IDF（旧 `computeIDF`）：`log((N - df + 0.5) / (df + 0.5) + 1)`。
///
/// `df` 只数**正文** token 命中的文档数（旧实现只传 `allDocTokens`）。
fn compute_idf(
    query_tokens: &[String],
    all_doc_tokens: &[Vec<String>],
    total_docs: usize,
) -> HashMap<String, f64> {
    let mut idf = HashMap::new();
    for token in query_tokens {
        let df = all_doc_tokens
            .iter()
            .filter(|doc_tokens| doc_tokens.iter().any(|t| t == token))
            .count();
        let value = ((as_f64(total_docs) - as_f64(df) + 0.5) / (as_f64(df) + 0.5) + 1.0).ln();
        idf.insert(token.clone(), value);
    }
    idf
}

/// 单文档 BM25（旧 `computeBM25`）。
///
/// `query_tokens` **不去重**：重复 query token 会重复计分（旧实现直接遍历
/// `List`）。
fn compute_bm25(
    query_tokens: &[String],
    doc_tokens: &[String],
    idf: &HashMap<String, f64>,
    avg_dl: f64,
) -> f64 {
    let mut score = 0.0_f64;
    let doc_len = as_f64(doc_tokens.len());
    let mut term_freq: HashMap<&str, usize> = HashMap::new();
    for token in doc_tokens {
        *term_freq.entry(token.as_str()).or_insert(0) += 1;
    }

    for q_token in query_tokens {
        let freq = term_freq.get(q_token.as_str()).copied().unwrap_or(0);
        if freq == 0 {
            continue;
        }
        let tf = as_f64(freq);
        let idf_val = idf.get(q_token.as_str()).copied().unwrap_or(0.0);
        let numerator = tf * (K1 + 1.0);
        // 旧 `Math.max(1, avgDL)`：int 提升为 double 后取大者。
        let denominator = tf + K1 * (1.0 - B + B * (doc_len / f64::max(1.0, avg_dl)));
        score += idf_val * (numerator / denominator);
    }
    score
}

// ==================== zk-tools 端口实现 ====================

/// [`zk_tools::MemoryStore`] 端口实现——把 `Memory` 工具接到本存储上。
///
/// 依赖方向铁律禁止 `zk-tools → zk-engine`，故端口定义在 zk-tools、实现落此处
/// （`zk-engine → zk-tools` 合法），装配落 zk-server 组合根。旧仓由 Spring 直接
/// 把 `MemdirService` 注入 `MemoryTool` 构造器，语义等价。
///
/// [`zk_tools::MemoryStore::write_tool_memory`] 固定
/// [`MemorySource::Tool`] + [`MemoryCategory::Semantic`]——旧 `MemoryTool` 逐字调
/// `writeMemory(content, MemorySource.TOOL)`，即两参重载（分类 `SEMANTIC`）。
impl zk_tools::MemoryStore for MemdirStore {
    fn read_memories(&self) -> futures::future::BoxFuture<'_, String> {
        Box::pin(MemdirStore::read_memories(self))
    }

    fn write_tool_memory(
        &self,
        content: String,
    ) -> futures::future::BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.write_semantic(&content, MemorySource::Tool)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn delete_memory(&self, pattern: String) -> futures::future::BoxFuture<'_, bool> {
        Box::pin(async move { MemdirStore::delete_memory(self, &pattern).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 独占临时目录（本仓无 `tempfile` 依赖，沿用 zk-tools 的 `temp_dir` 惯例）。
    fn temp_store(tag: &str) -> (MemdirStore, PathBuf) {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("zk_memdir_{}_{tag}_{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        (MemdirStore::with_dir(dir.clone()), dir)
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    // ---------- 枚举 ----------

    #[test]
    fn category_tags_round_trip_and_fall_back_to_semantic() {
        assert_eq!(MemoryCategory::Episodic.tag(), "episodic");
        assert_eq!(MemoryCategory::Team.tag(), "team");
        assert_eq!(
            MemoryCategory::from_tag(Some("PROCEDURAL")),
            MemoryCategory::Procedural
        );
        assert_eq!(MemoryCategory::from_tag(None), MemoryCategory::Semantic);
        assert_eq!(
            MemoryCategory::from_tag(Some("nope")),
            MemoryCategory::Semantic
        );
    }

    #[test]
    fn source_names_are_upper_case_and_parse_is_case_sensitive() {
        assert_eq!(MemorySource::Auto.name(), "AUTO");
        assert_eq!(MemorySource::parse("USER"), MemorySource::User);
        assert_eq!(MemorySource::parse("TOOL"), MemorySource::Tool);
        // 旧 `valueOf` 区分大小写，失败回落 AUTO。
        assert_eq!(MemorySource::parse("user"), MemorySource::Auto);
        assert_eq!(MemorySource::parse("weird"), MemorySource::Auto);
    }

    // ---------- 解析 ----------

    #[test]
    fn split_sections_does_not_emit_leading_empty_chunk() {
        let content = "<!-- source:A -->x<!-- source:B -->y";
        assert_eq!(
            split_sections(content),
            vec!["<!-- source:A -->x", "<!-- source:B -->y"]
        );
        // 头部前有裸文本时，该文本自成一段。
        assert_eq!(
            split_sections("bare\n<!-- source:A -->x"),
            vec!["bare\n", "<!-- source:A -->x"]
        );
    }

    #[test]
    fn match_entry_header_accepts_optional_category() {
        let (source, time, category, end) =
            match_entry_header("<!-- source:USER time:2024-01-01T00:00:00Z -->\nbody")
                .expect("header");
        assert_eq!(
            (source, time, category),
            ("USER", "2024-01-01T00:00:00Z", None)
        );
        assert_eq!(
            &"<!-- source:USER time:2024-01-01T00:00:00Z -->\nbody"[end..],
            "\nbody"
        );

        let (_, _, category, _) = match_entry_header(
            "<!-- source:TOOL time:2024-01-01T00:00:00Z category:episodic -->\nb",
        )
        .expect("header with category");
        assert_eq!(category, Some("episodic"));

        assert!(match_entry_header("<!-- source:USER -->").is_none());
        assert!(match_entry_header("plain text").is_none());
    }

    #[test]
    fn parse_entries_reads_header_fields_and_tolerates_bare_text() {
        let content = "bare note\n\
                       <!-- source:USER time:2024-01-01T00:00:00.000000Z category:episodic -->\n\
                       first\n\
                       <!-- source:TOOL time:2024-01-02T00:00:00.000000Z -->\n\
                       second\n";
        let entries = parse_entries(content);
        assert_eq!(entries.len(), 3);

        // 无头部段：USER + EPOCH + SEMANTIC。
        assert_eq!(entries[0].source, MemorySource::User);
        assert_eq!(entries[0].timestamp_millis, 0);
        assert_eq!(entries[0].category, MemoryCategory::Semantic);
        assert_eq!(entries[0].content, "bare note");

        assert_eq!(entries[1].source, MemorySource::User);
        assert_eq!(entries[1].category, MemoryCategory::Episodic);
        assert_eq!(entries[1].content, "first");
        assert!(entries[1].timestamp_millis > 0);

        // 缺 category 回落 SEMANTIC。
        assert_eq!(entries[2].source, MemorySource::Tool);
        assert_eq!(entries[2].category, MemoryCategory::Semantic);
        assert_eq!(entries[2].content, "second");
    }

    #[test]
    fn parse_entries_returns_empty_for_blank_content() {
        assert!(parse_entries("").is_empty());
        assert!(parse_entries("   \n\t ").is_empty());
    }

    #[test]
    fn parse_entries_falls_back_to_epoch_on_unparsable_timestamp() {
        let entries = parse_entries("<!-- source:USER time:not-a-time category:team -->\nx");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].timestamp_millis, 0);
        assert_eq!(entries[0].category, MemoryCategory::Team);
    }

    // ---------- 标题 / 截断 / 压缩 ----------

    #[test]
    fn extract_title_strips_markdown_hashes_and_keeps_java_quirks() {
        assert_eq!(extract_title("## Title\nbody"), "Title");
        assert_eq!(extract_title("no newline"), "no newline");
        // `indexOf('\n') == 0` 时旧实现取整个正文（`newline > 0` 判定）。
        assert_eq!(extract_title("\nsecond line"), "second line");
        // 无 `#` 时不吃前导空白之外的字符（仅 trim）。
        assert_eq!(extract_title("  spaced  \nrest"), "spaced");
    }

    #[test]
    fn truncate_for_prompt_cuts_by_lines_then_bytes() {
        assert_eq!(truncate_for_prompt(""), "");

        let short = "a\nb\nc";
        assert_eq!(truncate_for_prompt(short), short);

        let many = (0..MAX_ENTRYPOINT_LINES + 50)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let cut = truncate_for_prompt(&many);
        assert!(cut.ends_with(&format!(
            "<!-- truncated: exceeded {MAX_ENTRYPOINT_LINES} lines -->"
        )));
        assert_eq!(cut.split('\n').count(), MAX_ENTRYPOINT_LINES + 1);
    }

    #[test]
    fn truncate_for_prompt_cuts_by_bytes_at_last_newline() {
        // 单行 300 字节 × 100 行 = 30 000 字节，行数 100 < 200 故只走字节截断。
        let line = "x".repeat(299);
        let content = (0..100)
            .map(|_| line.clone())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(content.len() > MAX_ENTRYPOINT_BYTES);
        let cut = truncate_for_prompt(&content);
        assert!(cut.ends_with(&format!(
            "<!-- truncated: exceeded {MAX_ENTRYPOINT_BYTES} bytes -->"
        )));
        assert!(cut.len() < content.len());
    }

    #[test]
    fn truncate_for_prompt_keeps_content_when_no_newline_before_cutoff() {
        // 旧 `if (cutoff > 0)`：找不到换行时整个替换被跳过 → 不截断。
        let content = "y".repeat(MAX_ENTRYPOINT_BYTES + 10);
        assert_eq!(truncate_for_prompt(&content), content);
    }

    #[test]
    fn keep_count_truncates_toward_zero_with_floor_one() {
        assert_eq!(keep_count(1), 1);
        assert_eq!(keep_count(2), 1); // (int)(2*0.7) = 1
        assert_eq!(keep_count(3), 2); // (int)(3*0.7) = 2
        assert_eq!(keep_count(10), 7);
        assert_eq!(keep_count(100), 70);
    }

    #[test]
    fn compact_memories_keeps_newest_seventy_percent() {
        let content = "<!-- source:USER time:2024-01-01T00:00:00.000000Z category:semantic -->\nold\n\
                       <!-- source:USER time:2024-06-01T00:00:00.000000Z category:semantic -->\nmid\n\
                       <!-- source:USER time:2024-12-01T00:00:00.000000Z category:semantic -->\nnew\n";
        let compacted = compact_memories(content);
        // 3 条 → 保留 2 条，倒序（最新在前），无前导换行。
        assert!(compacted.starts_with("<!-- source:USER time:2024-12-01"));
        assert!(compacted.contains("new"));
        assert!(compacted.contains("mid"));
        assert!(!compacted.contains("old"));
        assert_eq!(parse_entries(&compacted).len(), 2);
    }

    #[test]
    fn compact_memories_on_blank_input_yields_empty() {
        assert_eq!(compact_memories("  "), "");
    }

    #[test]
    fn remove_matching_entries_is_case_insensitive_and_identity_on_miss() {
        let content = "<!-- source:USER time:2024-01-01T00:00:00.000000Z category:semantic -->\nAlpha\n\
                       <!-- source:USER time:2024-01-02T00:00:00.000000Z category:semantic -->\nBeta\n";
        let removed = remove_matching_entries(content, "alpha");
        assert!(!removed.contains("Alpha"));
        assert!(removed.contains("Beta"));
        // 无命中时原样返回入参（调用方据此判 false）。
        assert_eq!(remove_matching_entries(content, "gamma"), content);
    }

    // ---------- 分词 / BM25 ----------

    #[test]
    fn tokenize_drops_single_chars_and_stop_words() {
        let tokens = tokenize("The Rust engine is a x9 tool_kit");
        assert!(tokens.contains(&"rust".to_owned()));
        assert!(tokens.contains(&"engine".to_owned()));
        assert!(tokens.contains(&"x9".to_owned()));
        assert!(tokens.contains(&"tool_kit".to_owned()));
        // `the` / `is` / `a` 命中停用词，单字符 token 被长度过滤。
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"is".to_owned()));
        assert!(!tokens.contains(&"a".to_owned()));
    }

    #[test]
    fn tokenize_emits_cjk_unigrams_and_bigrams() {
        let tokens = tokenize("配置文件");
        // Unigram 4 个 + Bigram 3 个。
        assert_eq!(tokens.len(), 7);
        assert!(tokens.contains(&"配".to_owned()));
        assert!(tokens.contains(&"配置".to_owned()));
        assert!(tokens.contains(&"置文".to_owned()));
        assert!(tokens.contains(&"文件".to_owned()));
    }

    #[test]
    fn tokenize_filters_cjk_stop_word_unigram_but_keeps_bigram() {
        let tokens = tokenize("的确");
        // "的" 是停用词 → Unigram 只留 "确"；Bigram "的确" 不过滤。
        assert_eq!(tokens, vec!["确".to_owned(), "的确".to_owned()]);
    }

    #[test]
    fn tokenize_blank_yields_empty() {
        assert!(tokenize("").is_empty());
        assert!(tokenize("  \n ").is_empty());
        // 纯标点：既非英数 token 也非 CJK。
        assert!(tokenize("--- ??? ---").is_empty());
    }

    #[test]
    fn tokenize_splits_cjk_runs_at_non_cjk_boundaries() {
        let tokens = tokenize("配置 文件");
        // 两段各自出 Unigram + Bigram，跨空格不产生 "置文"。
        assert!(tokens.contains(&"配置".to_owned()));
        assert!(tokens.contains(&"文件".to_owned()));
        assert!(!tokens.contains(&"置文".to_owned()));
    }

    #[test]
    fn search_bm25_short_circuits_on_empty_inputs() {
        let docs = vec![DocumentEntry {
            title: "t".to_owned(),
            body: "rust engine".to_owned(),
        }];
        assert!(search_bm25(&[], "rust", 5).is_empty());
        assert!(search_bm25(&docs, "   ", 5).is_empty());
        // query 全是停用词 / 单字符 → 分词为空。
        assert!(search_bm25(&docs, "the a is", 5).is_empty());
    }

    #[test]
    fn search_bm25_ranks_matching_documents_and_honours_top_k() {
        let docs = vec![
            DocumentEntry {
                title: "unrelated".to_owned(),
                body: "cooking pasta recipes".to_owned(),
            },
            DocumentEntry {
                title: "rust guide".to_owned(),
                body: "rust ownership rust borrow rust lifetimes".to_owned(),
            },
            DocumentEntry {
                title: "misc".to_owned(),
                body: "rust appears once here".to_owned(),
            },
        ];
        let results = search_bm25(&docs, "rust", 10);
        assert_eq!(results.len(), 2);
        // 词频更高 + 标题命中 → 排第一。
        assert_eq!(results[0].index, 1);
        assert_eq!(results[1].index, 2);
        assert!(results[0].score > results[1].score);

        assert_eq!(search_bm25(&docs, "rust", 1).len(), 1);
    }

    #[test]
    fn search_bm25_title_boost_outranks_body_only_match() {
        let docs = vec![
            DocumentEntry {
                title: "filler words here".to_owned(),
                body: "cascade appears once padding padding padding".to_owned(),
            },
            DocumentEntry {
                title: "cascade".to_owned(),
                body: "appears once padding padding padding".to_owned(),
            },
        ];
        let results = search_bm25(&docs, "cascade", 10);
        assert_eq!(results.len(), 2);
        // 标题命中乘 TITLE_BOOST(2.0) 后压过纯正文命中。
        assert_eq!(results[0].index, 1);
    }

    #[test]
    fn search_bm25_keeps_index_order_on_tied_scores() {
        let docs = vec![
            DocumentEntry {
                title: "same".to_owned(),
                body: "rust engine".to_owned(),
            },
            DocumentEntry {
                title: "same".to_owned(),
                body: "rust engine".to_owned(),
            },
            DocumentEntry {
                title: "same".to_owned(),
                body: "rust engine".to_owned(),
            },
        ];
        let results = search_bm25(&docs, "rust", 10);
        assert_eq!(
            results.iter().map(|r| r.index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn compute_idf_falls_to_low_weight_for_ubiquitous_tokens() {
        let docs = vec![
            vec!["rust".to_owned(), "engine".to_owned()],
            vec!["rust".to_owned(), "server".to_owned()],
        ];
        let query = vec!["rust".to_owned(), "engine".to_owned(), "absent".to_owned()];
        let idf = compute_idf(&query, &docs, docs.len());
        // df=2/N=2 → log(0.5/2.5+1) ≈ 0.182；df=1 → log(1.5/1.5+1)=log 2 ≈ 0.693。
        assert!(idf["rust"] < idf["engine"]);
        // 未出现的 token：df=0 → log(2.5/0.5+1)=log 6 最大。
        assert!(idf["absent"] > idf["engine"]);
    }

    #[test]
    fn compute_bm25_scores_zero_when_no_term_hits() {
        let idf = HashMap::from([("rust".to_owned(), 1.0_f64)]);
        let doc = vec!["python".to_owned(), "engine".to_owned()];
        let score = compute_bm25(&["rust".to_owned()], &doc, &idf, 2.0);
        assert!(score.abs() < f64::EPSILON);
    }

    // ---------- 存储读写 ----------

    #[tokio::test]
    async fn write_memory_appends_header_and_round_trips() {
        let (store, dir) = temp_store("write");
        assert_eq!(store.read_memories().await, "");
        assert_eq!(store.entry_count().await, 0);

        store
            .write_memory("first note", MemorySource::Tool, MemoryCategory::Episodic)
            .await
            .expect("write");
        let raw = store.read_memories().await;
        assert!(raw.starts_with('\n'), "追加写入带前导换行（旧格式）");
        assert!(raw.contains("<!-- source:TOOL time:"));
        assert!(raw.contains("category:episodic -->"));

        let entries = store.list_entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source, MemorySource::Tool);
        assert_eq!(entries[0].category, MemoryCategory::Episodic);
        assert_eq!(entries[0].content, "first note");
        assert!(entries[0].timestamp_millis > 0);

        store
            .write_semantic("second note", MemorySource::User)
            .await
            .expect("write semantic");
        assert_eq!(store.entry_count().await, 2);
        assert!(!store.memory_file().with_extension("md.tmp").exists());

        cleanup(&dir);
    }

    #[tokio::test]
    async fn write_memory_creates_missing_directory() {
        let (_, dir) = temp_store("mkdir");
        let nested = dir.join("deep").join("nest");
        let store = MemdirStore::with_dir(nested.clone());
        store
            .write_semantic("note", MemorySource::Auto)
            .await
            .expect("write into missing dir");
        assert!(nested.join(ENTRYPOINT_NAME).exists());
        cleanup(&dir);
    }

    #[tokio::test]
    async fn save_memory_prefixes_markdown_title_and_uses_tool_source() {
        let (store, dir) = temp_store("save");
        store
            .save_memory("Build Steps", "run cargo")
            .await
            .expect("save");
        let entries = store.list_entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].source, MemorySource::Tool);
        assert_eq!(entries[0].category, MemoryCategory::Semantic);
        assert_eq!(entries[0].content, "## Build Steps\nrun cargo");
        assert_eq!(extract_title(&entries[0].content), "Build Steps");
        cleanup(&dir);
    }

    #[tokio::test]
    async fn write_memory_compacts_when_over_size_limit() {
        let (store, dir) = temp_store("compact");
        // 已有 2 条各 20 000 字符 → 第 3 条写入前 `40000 + 20000 > 50000` 触发压缩
        // （2 条保留 1 条）。时间戳显式布开：压缩排序是**稳定**降序（旧
        // `Comparator.comparing(timestamp).reversed()`），同毫秒写入会保留先写的
        // 那条，故不能依赖挂钟先后。
        let older = now_millis() - 2 * MILLIS_PER_DAY;
        let newer = now_millis() - MILLIS_PER_DAY;
        let seeded = format!(
            "<!-- source:AUTO time:{} category:semantic -->\n{}\n\n             <!-- source:AUTO time:{} category:semantic -->\n{}",
            format_rfc3339_micros(older),
            "a".repeat(20_000),
            format_rfc3339_micros(newer),
            "b".repeat(20_000)
        );
        tokio::fs::write(store.memory_file(), &seeded)
            .await
            .expect("seed");
        assert_eq!(store.entry_count().await, 2);

        store
            .write_semantic(&"c".repeat(20_000), MemorySource::Auto)
            .await
            .expect("write triggering compaction");
        let entries = store.list_entries().await;
        // 压缩保留最新 1 条（b）+ 本次新增 1 条（c）。
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| !e.content.starts_with('a')));
        assert!(entries.iter().any(|e| e.content.starts_with('b')));
        assert!(entries.iter().any(|e| e.content.starts_with('c')));
        cleanup(&dir);
    }

    #[tokio::test]
    async fn delete_memory_removes_matching_entries_only() {
        let (store, dir) = temp_store("delete");
        // 空文件：无可删。
        assert!(!store.delete_memory("anything").await);

        store
            .write_semantic("alpha secret", MemorySource::User)
            .await
            .expect("write");
        store
            .write_semantic("beta public", MemorySource::User)
            .await
            .expect("write");

        assert!(store.delete_memory("SECRET").await, "大小写不敏感匹配");
        let entries = store.list_entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "beta public");

        // 无命中返回 false 且不改文件。
        let before = store.read_memories().await;
        assert!(!store.delete_memory("nothing here").await);
        assert_eq!(store.read_memories().await, before);

        cleanup(&dir);
    }

    #[tokio::test]
    async fn purge_expired_drops_old_entries_but_spares_epoch_ones() {
        let (store, dir) = temp_store("purge");
        assert_eq!(store.purge_expired().await, 0);

        let stale = now_millis() - (MAX_MEMORY_AGE_DAYS + 5) * MILLIS_PER_DAY;
        let fresh = now_millis();
        let content = format!(
            "bare legacy note\n\
             <!-- source:USER time:{} category:semantic -->\nstale entry\n\
             <!-- source:USER time:{} category:semantic -->\nfresh entry\n",
            format_rfc3339_micros(stale),
            format_rfc3339_micros(fresh)
        );
        tokio::fs::write(store.memory_file(), &content)
            .await
            .expect("seed");

        assert_eq!(store.purge_expired().await, 1);
        let remaining = store.list_entries().await;
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().any(|e| e.content == "fresh entry"));
        // 无头部条目时间戳为 EPOCH，旧实现显式豁免。
        assert!(remaining.iter().any(|e| e.content == "bare legacy note"));
        assert!(remaining.iter().all(|e| e.content != "stale entry"));

        cleanup(&dir);
    }

    #[tokio::test]
    async fn search_memories_returns_named_hits_by_relevance() {
        let (store, dir) = temp_store("search");
        assert!(store.search_memories("rust", 5).await.is_empty());

        store
            .write_memory(
                "## Rust build\nuse cargo build for the rust workspace",
                MemorySource::Tool,
                MemoryCategory::Procedural,
            )
            .await
            .expect("write");
        store
            .write_memory(
                "## Pasta\nboil water then add pasta",
                MemorySource::User,
                MemoryCategory::Episodic,
            )
            .await
            .expect("write");

        let hits = store.search_memories("rust cargo", 5).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].category, MemoryCategory::Procedural);
        assert!(hits[0].content.starts_with("## Rust build"));
        // 旧命名：`{SOURCE}_{epochSeconds}`。
        assert!(hits[0].name.starts_with("TOOL_"));
        assert!(
            hits[0].name["TOOL_".len()..]
                .parse::<i64>()
                .expect("epoch seconds")
                > 0
        );

        cleanup(&dir);
    }

    #[tokio::test]
    async fn search_by_category_filters_and_orders_newest_first() {
        let (store, dir) = temp_store("category");
        assert!(
            store
                .search_by_category(MemoryCategory::Team, 3)
                .await
                .is_empty()
        );

        let older = now_millis() - 10 * MILLIS_PER_DAY;
        let newer = now_millis();
        let content = format!(
            "<!-- source:USER time:{} category:team -->\nteam older\n\
             <!-- source:USER time:{} category:team -->\nteam newer\n\
             <!-- source:USER time:{} category:episodic -->\nepisodic one\n",
            format_rfc3339_micros(older),
            format_rfc3339_micros(newer),
            format_rfc3339_micros(newer)
        );
        tokio::fs::write(store.memory_file(), &content)
            .await
            .expect("seed");

        let team = store.search_by_category(MemoryCategory::Team, 5).await;
        assert_eq!(team.len(), 2);
        assert_eq!(team[0].content, "team newer");
        assert_eq!(team[1].content, "team older");
        // max_count 生效。
        assert_eq!(
            store
                .search_by_category(MemoryCategory::Team, 1)
                .await
                .len(),
            1
        );

        cleanup(&dir);
    }

    #[tokio::test]
    async fn read_memories_for_prompt_applies_line_guard() {
        let (store, dir) = temp_store("prompt");
        let content = (0..MAX_ENTRYPOINT_LINES + 20)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        tokio::fs::write(store.memory_file(), &content)
            .await
            .expect("seed");
        let prompt = store.read_memories_for_prompt().await;
        assert!(prompt.ends_with(&format!(
            "<!-- truncated: exceeded {MAX_ENTRYPOINT_LINES} lines -->"
        )));
        cleanup(&dir);
    }

    #[tokio::test]
    async fn memory_file_is_under_the_configured_dir() {
        let (store, dir) = temp_store("path");
        assert_eq!(store.memory_file(), dir.join(ENTRYPOINT_NAME));
        cleanup(&dir);
    }
}
