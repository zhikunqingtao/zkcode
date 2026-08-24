//! 产物清单载荷——条目 / 清单 / 完整性校验结果（纯数据层，无 IO）。
//!
//! 语义来源（旧仓库只读）：`artifact/ArtifactManifestService.java`（327L）与
//! 同包 `ArtifactEntry` / `VerificationResult`。逐条保留的语义词汇表：
//!
//! - 操作归一（`normalizeOperation`）：`create`/`created` → `created`，
//!   `modify`/`modified`/`update` → `modified`，`delete`/`deleted` → `deleted`；
//! - 校验体积上限 200 MiB（旧 `MAX_VERIFICATION_BYTES`）；
//! - 失败码 `HASH_MISMATCH` / `SEALED_HASH_MISSING` /
//!   `ARTIFACT_NOT_REGULAR_FILE` / `VERIFICATION_SIZE_LIMIT` /
//!   `DELETE_TARGET_STILL_EXISTS` / `VERIFICATION_ERROR`；
//! - 汇总状态：有失败时 `partial`（另有通过）或 `failed`；无失败时
//!   `unverified`（存在未校验项）或 `verified`。
//!
//! # 有意差异
//!
//! 旧实现把清单落 `SQLite` 双表（`artifact_manifests` / `artifact_entries`）并
//! 带 `declared → sealed → *_verified` 状态机；本迁移按 Batch 8F 判据改为
//! **内存 `DashMap` + `.zk/artifacts/{run_id}.json`** 单文件存储（见
//! [`super::service`]），故不保留 `sealed` 中间态：条目登记时即算 hash
//! （相当于旧 `seal`），`hash` 缺失即 `SEALED_HASH_MISSING`。

use serde::{Deserialize, Serialize};

/// 单文件校验体积上限（旧 `MAX_VERIFICATION_BYTES = 200 * 1024 * 1024`）。
pub const MAX_VERIFICATION_BYTES: u64 = 200 * 1024 * 1024;

/// 产物操作（旧 `normalizeOperation` 的三个归一值）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactAction {
    /// 新建文件。
    Created,
    /// 修改既有文件。
    Modified,
    /// 删除文件。
    Deleted,
}

impl ArtifactAction {
    /// 线格式名。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
        }
    }

    /// 归一入参操作名（旧 `normalizeOperation`；不可识别 → `None`，调用方
    /// 映射 `ARTIFACT_OPERATION_INVALID`）。
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "create" | "created" | "add" | "added" => Some(Self::Created),
            "modify" | "modified" | "update" | "updated" | "edit" => Some(Self::Modified),
            "delete" | "deleted" | "remove" | "removed" => Some(Self::Deleted),
            _ => None,
        }
    }

    /// 是否期望文件在磁盘上存在（删除操作期望不存在）。
    #[must_use]
    pub const fn expects_present(self) -> bool {
        !matches!(self, Self::Deleted)
    }
}

/// 清单条目——一次文件变更的证据。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactEntry {
    /// 绝对路径。
    pub path: String,
    /// 操作。
    pub action: ArtifactAction,
    /// 登记时的字节数（删除操作或读取失败时 0）。
    pub size: u64,
    /// 登记时的 SHA-256（十六进制小写；删除 / 超限 / 读取失败时 `None`）。
    pub hash: Option<String>,
    /// 登记时刻（RFC 3339，微秒）。
    pub recorded_at: String,
}

/// 一个 run 的完整产物清单。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactManifest {
    /// 归属 run。
    pub run_id: String,
    /// 归属会话（未知时 `None`——工具上下文可能缺 `session_id`）。
    #[serde(default)]
    pub session_id: Option<String>,
    /// 首次登记时刻。
    pub created_at: String,
    /// 最近一次登记时刻。
    pub updated_at: String,
    /// 逐文件条目（按路径首次出现次序）。
    #[serde(default)]
    pub files: Vec<ArtifactEntry>,
}

impl ArtifactManifest {
    /// 构造空清单。
    #[must_use]
    pub fn new(run_id: impl Into<String>, session_id: Option<String>, now: String) -> Self {
        Self {
            run_id: run_id.into(),
            session_id,
            created_at: now.clone(),
            updated_at: now,
            files: Vec::new(),
        }
    }

    /// 按路径登记条目（同路径覆盖）。
    ///
    /// 覆盖规则：既有条目为 `Created` 且新操作为 `Modified` 时保留
    /// `Created`——同一 run 内先建后改，对外仍是「本 run 新增的文件」；
    /// 其余情况以新操作为准（改后删 → `Deleted`）。
    pub fn upsert(&mut self, entry: ArtifactEntry) {
        self.updated_at = entry.recorded_at.clone();
        if let Some(existing) = self.files.iter_mut().find(|item| item.path == entry.path) {
            let action = if existing.action == ArtifactAction::Created
                && entry.action == ArtifactAction::Modified
            {
                ArtifactAction::Created
            } else {
                entry.action
            };
            *existing = ArtifactEntry { action, ..entry };
            return;
        }
        self.files.push(entry);
    }

    /// 按路径查条目。
    #[must_use]
    pub fn entry(&self, path: &str) -> Option<&ArtifactEntry> {
        self.files.iter().find(|item| item.path == path)
    }
}

/// 单条校验失败（旧 `VerificationResult.failures` 元素）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationFailure {
    /// 路径。
    pub path: String,
    /// 稳定失败码。
    pub code: String,
    /// 说明。
    pub message: String,
}

impl VerificationFailure {
    /// 构造失败项。
    #[must_use]
    pub fn new(path: impl Into<String>, code: &str, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            code: code.to_owned(),
            message: message.into(),
        }
    }
}

/// 完整性校验汇总（旧 `VerificationResult` record 的等价形状）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationResult {
    /// 归属 run。
    pub run_id: String,
    /// 汇总状态（`verified` / `partial` / `failed` / `unverified`）。
    pub status: String,
    /// 通过条目数。
    pub verified: usize,
    /// 失败条目数。
    pub failed: usize,
    /// 无法校验条目数（缺 hash / 超体积上限）。
    pub unverified: usize,
    /// 条目总数。
    pub total: usize,
    /// 逐条失败明细（含无法校验项）。
    pub failures: Vec<VerificationFailure>,
}

impl VerificationResult {
    /// 按计数装配汇总（状态判定逐字对照旧实现）。
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        verified: usize,
        failed: usize,
        unverified: usize,
        failures: Vec<VerificationFailure>,
    ) -> Self {
        let status = if failed > 0 {
            if verified > 0 { "partial" } else { "failed" }
        } else if unverified > 0 {
            "unverified"
        } else {
            "verified"
        };
        Self {
            run_id: run_id.into(),
            status: status.to_owned(),
            verified,
            failed,
            unverified,
            total: verified + failed + unverified,
            failures,
        }
    }

    /// 是否整体通过（无失败且无未校验项）。
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failed == 0 && self.unverified == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, action: ArtifactAction) -> ArtifactEntry {
        ArtifactEntry {
            path: path.to_owned(),
            action,
            size: 3,
            hash: Some("abc".to_owned()),
            recorded_at: "2026-01-01T00:00:00.000000Z".to_owned(),
        }
    }

    #[test]
    fn action_parse_normalizes_legacy_aliases() {
        for raw in ["create", "Created", " add "] {
            assert_eq!(ArtifactAction::parse(raw), Some(ArtifactAction::Created));
        }
        for raw in ["modify", "MODIFIED", "update", "edit"] {
            assert_eq!(ArtifactAction::parse(raw), Some(ArtifactAction::Modified));
        }
        for raw in ["delete", "Deleted", "remove"] {
            assert_eq!(ArtifactAction::parse(raw), Some(ArtifactAction::Deleted));
        }
        assert_eq!(ArtifactAction::parse("rename"), None);
    }

    #[test]
    fn action_serializes_as_lowercase_wire_name() {
        let body = serde_json::to_value(ArtifactAction::Modified).expect("json");
        assert_eq!(body, serde_json::json!("modified"));
        assert_eq!(ArtifactAction::Deleted.as_str(), "deleted");
        assert!(ArtifactAction::Created.expects_present());
        assert!(!ArtifactAction::Deleted.expects_present());
    }

    #[test]
    fn upsert_keeps_created_when_later_modified() {
        let mut manifest =
            ArtifactManifest::new("run-1", None, "2026-01-01T00:00:00.000000Z".to_owned());
        manifest.upsert(entry("/tmp/a", ArtifactAction::Created));
        manifest.upsert(entry("/tmp/a", ArtifactAction::Modified));
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.entry("/tmp/a").expect("entry").action, ArtifactAction::Created);
    }

    #[test]
    fn upsert_overrides_with_delete_and_touches_updated_at() {
        let mut manifest =
            ArtifactManifest::new("run-1", Some("s-1".to_owned()), "t0".to_owned());
        manifest.upsert(entry("/tmp/a", ArtifactAction::Created));
        let mut deleted = entry("/tmp/a", ArtifactAction::Deleted);
        deleted.recorded_at = "t9".to_owned();
        manifest.upsert(deleted);
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].action, ArtifactAction::Deleted);
        assert_eq!(manifest.updated_at, "t9");
        assert_eq!(manifest.created_at, "t0");
    }

    #[test]
    fn upsert_appends_distinct_paths_in_order() {
        let mut manifest = ArtifactManifest::new("run-1", None, "t0".to_owned());
        manifest.upsert(entry("/tmp/a", ArtifactAction::Created));
        manifest.upsert(entry("/tmp/b", ArtifactAction::Modified));
        let paths: Vec<&str> = manifest.files.iter().map(|item| item.path.as_str()).collect();
        assert_eq!(paths, vec!["/tmp/a", "/tmp/b"]);
    }

    #[test]
    fn verification_status_follows_legacy_matrix() {
        assert_eq!(VerificationResult::new("r", 2, 0, 0, vec![]).status, "verified");
        assert_eq!(VerificationResult::new("r", 0, 0, 0, vec![]).status, "verified");
        assert_eq!(VerificationResult::new("r", 1, 0, 1, vec![]).status, "unverified");
        assert_eq!(VerificationResult::new("r", 1, 1, 0, vec![]).status, "partial");
        assert_eq!(VerificationResult::new("r", 0, 2, 0, vec![]).status, "failed");
    }

    #[test]
    fn verification_totals_and_passed_flag() {
        let result = VerificationResult::new(
            "r",
            1,
            1,
            0,
            vec![VerificationFailure::new("/tmp/a", "HASH_MISMATCH", "changed")],
        );
        assert_eq!(result.total, 2);
        assert!(!result.passed());
        assert!(VerificationResult::new("r", 3, 0, 0, vec![]).passed());
        assert!(!VerificationResult::new("r", 3, 0, 1, vec![]).passed());
    }

    #[test]
    fn manifest_round_trips_camel_case_json() {
        let mut manifest = ArtifactManifest::new("run-7", Some("s-1".to_owned()), "t0".to_owned());
        manifest.upsert(entry("/tmp/a", ArtifactAction::Created));
        let body = serde_json::to_value(&manifest).expect("json");
        assert_eq!(body["runId"], "run-7");
        assert_eq!(body["sessionId"], "s-1");
        assert_eq!(body["files"][0]["recordedAt"], "2026-01-01T00:00:00.000000Z");
        let parsed: ArtifactManifest = serde_json::from_value(body).expect("parse");
        assert_eq!(parsed, manifest);
    }
}
