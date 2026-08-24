//! 旧用户目录（[`paths::LEGACY_CONFIG_DIR_NAME`]）→ `.zk/` 的启动期目录迁移。
//!
//! 设计约束（三条铁律）：
//!
//! 1. **幂等**：迁移成功后在新目录写 [`MARKER_FILE_NAME`] 标记，标记存在即整体
//!    跳过；标记未写成功（迁移中途失败）时下次启动可续跑——已拷贝的文件因
//!    「目标已存在则跳过」而不会重复写入。
//! 2. **不阻塞启动**：任何 [`std::io::Error`] 一律降级为 `warn` 日志，绝不
//!    panic、绝不返回错误给调用方——迁移失败只意味着旧数据没搬过来，新目录
//!    照常工作。
//! 3. **不破坏新数据**：拷贝而非移动（旧目录原样保留，故授权门禁必须继续
//!    保护它），且目标已存在的文件一律跳过——新布局里的数据永远优先。
//!
//! 符号链接不跟随、直接跳过：既避免环导致的无限递归，也避免把链接目标之外的
//! 内容（可能指向 `~/.ssh` 之类）复制进新目录。

use std::fs;
use std::io;
use std::path::Path;

use crate::paths;

/// 迁移完成标记文件名（落在新目录内）。
pub const MARKER_FILE_NAME: &str = ".migrated";

/// 启动时把旧用户目录（`$HOME/` 下 [`paths::LEGACY_CONFIG_DIR_NAME`]）迁到 `~/.zk/`。
///
/// 幂等：若 `~/.zk/.migrated` 标记已存在则跳过。任何 IO 错误降级为 `warn`
/// 日志，不阻塞启动。必须在任何读取 `~/.zk/` 的初始化之前调用。
pub fn run_if_needed() {
    let legacy = paths::legacy_user_config_dir();
    let target = paths::user_config_dir();
    match migrate_tree(&legacy, &target) {
        Ok(Outcome::Skipped(reason)) => {
            tracing::debug!(
                legacy = %legacy.display(),
                target = %target.display(),
                reason = reason.as_str(),
                "legacy config directory migration skipped"
            );
        }
        Ok(Outcome::Migrated(stats)) => {
            tracing::info!(
                legacy = %legacy.display(),
                target = %target.display(),
                directories = stats.directories,
                files_copied = stats.files_copied,
                files_kept = stats.files_kept,
                symlinks_skipped = stats.symlinks_skipped,
                "migrated legacy config directory"
            );
        }
        Err(err) => {
            tracing::warn!(
                legacy = %legacy.display(),
                target = %target.display(),
                error = %err,
                "legacy config directory migration failed; continuing with new layout"
            );
        }
    }
}

/// 迁移结果（内部诊断用，不进公开 API）。
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// 无需迁移。
    Skipped(SkipReason),
    /// 已迁移，附带计数。
    Migrated(Stats),
}

/// 跳过原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkipReason {
    /// 旧目录不存在（全新安装的常态路径）。
    NoLegacyDir,
    /// 标记文件已存在（迁移过一次）。
    AlreadyMigrated,
}

impl SkipReason {
    /// 日志字段值。
    fn as_str(self) -> &'static str {
        match self {
            Self::NoLegacyDir => "no-legacy-dir",
            Self::AlreadyMigrated => "already-migrated",
        }
    }
}

/// 拷贝计数。
#[derive(Debug, Default, PartialEq, Eq)]
struct Stats {
    /// 新建/复用的目录数（含目标根）。
    directories: usize,
    /// 实际拷贝的文件数。
    files_copied: usize,
    /// 目标已存在因而保留新数据、未覆盖的文件数。
    files_kept: usize,
    /// 跳过的符号链接数。
    symlinks_skipped: usize,
}

/// 纯路径版迁移（可测）：`legacy` → `target`，成功后写标记。
fn migrate_tree(legacy: &Path, target: &Path) -> io::Result<Outcome> {
    if !legacy.is_dir() {
        return Ok(Outcome::Skipped(SkipReason::NoLegacyDir));
    }
    let marker = target.join(MARKER_FILE_NAME);
    if marker.exists() {
        return Ok(Outcome::Skipped(SkipReason::AlreadyMigrated));
    }
    let mut stats = Stats::default();
    copy_dir(legacy, target, &mut stats)?;
    // 标记最后写：中途失败则下次启动续跑（已拷贝文件走 files_kept 分支）。
    fs::write(
        &marker,
        format!("migrated from {}\n", legacy.display()).as_bytes(),
    )?;
    Ok(Outcome::Migrated(stats))
}

/// 递归拷贝目录内容，保留目录结构。
fn copy_dir(source: &Path, target: &Path, stats: &mut Stats) -> io::Result<()> {
    fs::create_dir_all(target)?;
    stats.directories += 1;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        // `read_dir` 的 file_type 不跟随符号链接，故 is_symlink 可靠。
        let file_type = entry.file_type()?;
        let destination = target.join(entry.file_name());
        if file_type.is_symlink() {
            stats.symlinks_skipped += 1;
        } else if file_type.is_dir() {
            copy_dir(&entry.path(), &destination, stats)?;
        } else if destination.exists() {
            stats.files_kept += 1;
        } else {
            fs::copy(entry.path(), &destination)?;
            stats.files_copied += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    /// 自清理临时根（zk-core 不引入 tempfile：依赖面只允许 tracing + std）。
    struct TempRoot {
        path: PathBuf,
    }

    impl TempRoot {
        fn new(tag: &str) -> Self {
            static SEQ: AtomicU32 = AtomicU32::new(0);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos());
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "zk-core-migrate-{tag}-{}-{nanos}-{seq}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp root");
            Self { path }
        }

        fn join(&self, relative: &str) -> PathBuf {
            self.path.join(relative)
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// 建文件（含父目录）。
    fn seed(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent dir");
        }
        fs::write(path, contents).expect("seed file");
    }

    #[test]
    fn copies_tree_and_writes_marker() {
        let temp = TempRoot::new("copy");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy.join("settings.json"), "{}");
        seed(&legacy.join("skills/commit.md"), "# commit");
        seed(&legacy.join("scratchpad/session/note.md"), "note");

        let outcome = migrate_tree(&legacy, &target).expect("migration");

        assert_eq!(
            outcome,
            Outcome::Migrated(Stats {
                directories: 4, // 根 + skills + scratchpad + scratchpad/session
                files_copied: 3,
                files_kept: 0,
                symlinks_skipped: 0,
            })
        );
        assert_eq!(
            fs::read_to_string(target.join("skills/commit.md")).expect("copied skill"),
            "# commit"
        );
        assert_eq!(
            fs::read_to_string(target.join("scratchpad/session/note.md")).expect("copied note"),
            "note"
        );
        let marker = fs::read_to_string(target.join(MARKER_FILE_NAME)).expect("marker");
        assert!(marker.contains(&legacy.display().to_string()), "{marker}");
        // 拷贝而非移动：旧目录必须原样保留（授权门禁仍需保护它）。
        assert!(legacy.join("settings.json").is_file());
    }

    #[test]
    fn second_run_is_idempotent() {
        let temp = TempRoot::new("idempotent");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy.join("settings.json"), "{}");

        assert!(matches!(
            migrate_tree(&legacy, &target).expect("first run"),
            Outcome::Migrated(_)
        ));
        // 第二轮：标记已在，整体跳过（即便旧目录又长出新文件也不再搬）。
        seed(&legacy.join("late.json"), "late");
        assert_eq!(
            migrate_tree(&legacy, &target).expect("second run"),
            Outcome::Skipped(SkipReason::AlreadyMigrated)
        );
        assert!(!target.join("late.json").exists());
    }

    #[test]
    fn missing_legacy_dir_skips_without_creating_target() {
        let temp = TempRoot::new("absent");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);

        assert_eq!(
            migrate_tree(&legacy, &target).expect("skip"),
            Outcome::Skipped(SkipReason::NoLegacyDir)
        );
        assert!(!target.exists(), "跳过时不得副作用建目录");
    }

    #[test]
    fn legacy_file_instead_of_dir_is_not_migrated() {
        let temp = TempRoot::new("legacy-file");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy, "not a directory");

        assert_eq!(
            migrate_tree(&legacy, &target).expect("skip"),
            Outcome::Skipped(SkipReason::NoLegacyDir)
        );
        assert!(!target.exists());
    }

    #[test]
    fn existing_target_files_are_never_overwritten() {
        let temp = TempRoot::new("keep-new");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy.join("settings.json"), "old");
        seed(&legacy.join("skills/commit.md"), "old skill");
        seed(&target.join("settings.json"), "new");

        let outcome = migrate_tree(&legacy, &target).expect("migration");

        assert_eq!(
            outcome,
            Outcome::Migrated(Stats {
                directories: 2,
                files_copied: 1,
                files_kept: 1,
                symlinks_skipped: 0,
            })
        );
        assert_eq!(
            fs::read_to_string(target.join("settings.json")).expect("kept"),
            "new",
            "新布局数据优先，旧值不得覆盖"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_skipped_not_followed() {
        let temp = TempRoot::new("symlink");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        let outside = temp.join("outside/secret.txt");
        seed(&outside, "secret");
        seed(&legacy.join("settings.json"), "{}");
        std::os::unix::fs::symlink(&outside, legacy.join("leak.txt")).expect("file symlink");
        // 自引用目录链接：跟随即无限递归，必须被跳过。
        std::os::unix::fs::symlink(&legacy, legacy.join("loop")).expect("dir symlink");

        let outcome = migrate_tree(&legacy, &target).expect("migration");

        assert_eq!(
            outcome,
            Outcome::Migrated(Stats {
                directories: 1,
                files_copied: 1,
                files_kept: 0,
                symlinks_skipped: 2,
            })
        );
        assert!(!target.join("leak.txt").exists());
        assert!(!target.join("loop").exists());
    }

    #[test]
    fn interrupted_migration_resumes_on_next_run() {
        let temp = TempRoot::new("resume");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy.join("a.json"), "a");
        seed(&legacy.join("b.json"), "b");
        // 模拟上轮中断：a 已落地、标记未写。
        seed(&target.join("a.json"), "a");

        let outcome = migrate_tree(&legacy, &target).expect("resumed migration");

        assert_eq!(
            outcome,
            Outcome::Migrated(Stats {
                directories: 1,
                files_copied: 1,
                files_kept: 1,
                symlinks_skipped: 0,
            })
        );
        assert!(target.join(MARKER_FILE_NAME).is_file());
    }

    #[test]
    fn marker_lives_inside_new_config_dir() {
        // 标记必须落在**新**目录内：放旧目录会让「旧目录只读保护」与迁移写入相冲。
        assert_eq!(MARKER_FILE_NAME, ".migrated");
        let temp = TempRoot::new("marker");
        let legacy = temp.join(paths::LEGACY_CONFIG_DIR_NAME);
        let target = temp.join(paths::CONFIG_DIR_NAME);
        seed(&legacy.join("settings.json"), "{}");

        migrate_tree(&legacy, &target).expect("migration");

        assert!(target.join(MARKER_FILE_NAME).is_file());
        assert!(!legacy.join(MARKER_FILE_NAME).exists());
    }
}

// 注意：本模块**刻意不测** `run_if_needed()`。它解析真实 `$HOME`，而 `HOME` 在
// Rust 2024 下不可安全改写（`set_var` 是 unsafe，workspace `unsafe_code =
// "forbid"`）。若在测试里直调，`cargo test` 会真的把开发机上的旧用户目录迁进
// `~/.zk/` 并写标记——测试污染用户数据，绝不可接受。故 `run_if_needed` 只做
// 「env 取路径 + 调 `migrate_tree` + 日志」三件事，全部逻辑在 `migrate_tree`
// 上覆盖。
