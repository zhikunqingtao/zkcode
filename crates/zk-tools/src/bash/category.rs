//! 命令分类枚举——逐字对照旧源 `tool/bash/CommandCategory.java`（45 行）。
//!
//! 同时用于 UI 展示与动态超时推荐；原始五个分类（`READ_ONLY` / `SEARCH` /
//! `MODIFICATION` / `SYSTEM_INFO` / `UNKNOWN`）保持向后兼容，新增细粒度分类
//! （`COMPILATION` / `TEST_EXECUTION` / `PACKAGE_INSTALL` / `GIT_OPERATION` /
//! `SERVER_START`）用于超时策略。不影响 AST→正则→路径验证三层安全架构。

/// 命令分类——对照旧源 `CommandCategory` L11-21。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    /// `grep` / `cat` / `ls` / `head` / `tail` / `wc` → 30s。
    ReadOnly,
    /// `find` / `rg` / `ag` → 60s。
    Search,
    /// `rm` / `mkdir` / `touch` → 120s。
    Modification,
    /// `uname` / `pwd` / `whoami` → 30s。
    SystemInfo,
    /// `mvn compile` / `npm run build` / `cargo build` → 300s（5min）。
    Compilation,
    /// `mvn test` / `pytest` / `npm test` → 600s（10min）。
    TestExecution,
    /// `npm install` / `pip install` / `mvn dependency` → 300s。
    PackageInstall,
    /// `git status` / `git diff` / `git log` → 60s。
    GitOperation,
    /// `npm start` / `java -jar` → 120s。
    ServerStart,
    /// 默认 → 120s。
    Unknown,
}

impl CommandCategory {
    /// 返回用于 UI/日志展示的简短标签——对照旧源 `getDisplayLabel()` L34-36。
    #[must_use]
    pub const fn display_label(self) -> &'static str {
        match self {
            Self::ReadOnly => "read",
            Self::Search => "search",
            Self::Modification => "write",
            Self::SystemInfo => "info",
            Self::Compilation => "compile",
            Self::TestExecution => "test",
            Self::PackageInstall => "install",
            Self::GitOperation => "git",
            Self::ServerStart => "server",
            Self::Unknown => "command",
        }
    }

    /// 返回该类型命令的推荐超时时间（毫秒）——对照旧源 `getRecommendedTimeoutMs()` L42-44。
    #[must_use]
    pub const fn recommended_timeout_ms(self) -> u64 {
        match self {
            Self::ReadOnly | Self::SystemInfo => 30_000,
            Self::Search | Self::GitOperation => 60_000,
            Self::Modification | Self::ServerStart | Self::Unknown => 120_000,
            Self::Compilation | Self::PackageInstall => 300_000,
            Self::TestExecution => 600_000,
        }
    }
}
