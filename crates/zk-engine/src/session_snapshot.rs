//! 会话快照——服务重启后恢复会话的磁盘持久化（Batch 5 Step 7）。
//!
//! 逐字对照旧 `session/SessionSnapshotService.java`（161 行）、
//! `session/SessionSnapshot.java`、`session/SessionSnapshotSummary.java`；
//! [`SessionSnapshot::from_session_detail`] 对照旧
//! `controller/SessionSnapshotController.saveSnapshot` 的快照装配段（该段是纯
//! 领域逻辑——回合数统计与元数据组装，与 HTTP 无关，故下沉到本模块，端点层
//! 只做取会话 / 调服务 / 出摘要）。
//!
//! # 存储形态
//!
//! 一会话一文件：`{snapshot_dir}/{sessionId}.json`，缺省目录
//! `~/.zk/snapshots/`（经 [`zk_core::paths::user_config_dir`]）——旧实现取
//! `~/.zhiku/snapshots/`，路径前缀差异属 Step 0-1 的 `.zk/` 统一裁定，非本步
//! 行为差异。目录在构造时 `create_dir_all`，失败只 `warn`（旧构造器同：
//! 「Snapshot feature may be unavailable」，服务照常起）。
//!
//! # 路径安全（旧 `validateSessionId` 逐条保留）
//!
//! `sessionId` 为空白、含 `..`、含 `/` 或 `\` 一律拒绝（旧
//! `IllegalArgumentException`，本端为 [`InvalidSessionId`]）。这是唯一的路径
//! 守卫：文件名由 `sessionId + ".json"` 直接拼接，穿越序列必须在此拦死。
//!
//! # IO 失败一律降级，不上抛
//!
//! 保存 / 加载 / 列出 / 删除的 IO 与解析失败全部只记日志并返回「无结果」
//! （`Ok(())` / `Ok(None)` / 空表 / `Ok(false)`），与旧实现逐字一致——快照是
//! 尽力而为的旁路能力，磁盘异常不得让主流程失败。**参数非法**是唯一例外
//! （旧实现同样抛异常）。
//!
//! # 与旧实现的差异
//!
//! 1. **原子替换**：[`SessionSnapshotService::save_snapshot`] 写
//!    `{sessionId}.json.tmp` 后 `rename`；旧 `objectMapper.writeValue(file, ...)`
//!    直接截断原文件，写入中途崩溃会留下半截 JSON（下次 `loadSnapshot` 解析失败
//!    → 快照静默丢失）。同 [`crate::memdir`] 的 `replace_file` 取向。
//! 2. **JSON 缩进风格**：本端 `serde_json::to_vec_pretty`（`"key": value`）；
//!    旧 Jackson `DefaultPrettyPrinter`（`"key" : value`，冒号前带空格）。
//!    字节不同、JSON 语义相同，无消费方依赖该风格。
//! 3. **`null` 收敛**：文件里 `messages` / `turnCount` / `metadata` 为 `null`
//!    （或缺键）时本端分别收敛为空表 / 0 / 空对象——旧端 record 分量得 `null`
//!    （`int turnCount` 经 Jackson 的原始类型缺省同样得 0），回写时原样吐
//!    `null`。收敛面只影响手工编辑或旧版本文件，本端写出的文件三键恒非
//!    `null`。`sessionId` / `model` / `createdAt` 不收敛（见 [`SessionSnapshot`]）。
//! 4. **时间戳精度**：`createdAt` 恒 6 位微秒（[`format_rfc3339_micros`]），旧
//!    `Instant` 经 Jackson 输出的小数位随值动态（0/3/6/9 位）。解析侧两端都吃
//!    任意小数位，故互读兼容。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use zk_db::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};
use zk_db::{MessageRecord, MessageRole, SessionDetail};

/// 快照子目录名（相对配置目录，旧 `~/.zhiku/snapshots` 的叶子名）。
pub const SNAPSHOT_DIR_NAME: &str = "snapshots";
/// 快照文件后缀（旧 `sessionId + ".json"`）。
pub const SNAPSHOT_FILE_SUFFIX: &str = ".json";

/// `sessionId` 非法（旧 `IllegalArgumentException`，消息逐字保留）。
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Invalid sessionId: must not contain path separators or traversal sequences")]
pub struct InvalidSessionId;

// ==================== 载荷 ====================

/// 会话快照全文（旧 `SessionSnapshot` record）。
///
/// `sessionId` / `model` / `createdAt` 用 [`Option`] 承载：旧 record 的这三个
/// 引用型分量允许为 `null`（手工编辑或旧版本文件），摘要面原样透传该 `null`，
/// 本端因此不做「缺失即空串」的隐式收敛。`turnCount`（Java `int` 原始类型）与
/// `messages` 缺失时取 0 / 空表，与 Jackson 的原始类型缺省一致。
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    /// 会话 ID。
    #[serde(default)]
    pub session_id: Option<String>,
    /// 完整消息历史（按序）。
    #[serde(default, deserialize_with = "null_as_default")]
    pub messages: Vec<MessageRecord>,
    /// 模型标识。
    #[serde(default)]
    pub model: Option<String>,
    /// 回合数（user 消息条数）。
    #[serde(default, deserialize_with = "null_as_default")]
    pub turn_count: i64,
    /// 快照创建时间（epoch 毫秒；序列化为 RFC 3339）。
    #[serde(default, with = "serde_iso_ms_opt")]
    pub created_at: Option<i64>,
    /// 附加元数据（标题 / 工作目录 / 状态 / 用量 / 成本）。
    #[serde(default, deserialize_with = "null_as_default")]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// 快照摘要（旧 `SessionSnapshotSummary` record；列表与保存/恢复响应共用）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshotSummary {
    /// 会话 ID。
    pub session_id: Option<String>,
    /// 模型标识。
    pub model: Option<String>,
    /// 回合数。
    pub turn_count: i64,
    /// 消息条数（`messages` 缺失时为 0，旧 `messages != null ? size() : 0`）。
    pub message_count: usize,
    /// 创建时间（epoch 毫秒；序列化为 RFC 3339）。
    #[serde(with = "serde_iso_ms_opt")]
    pub created_at: Option<i64>,
}

impl SessionSnapshot {
    /// 由会话明细装配快照（旧 `SessionSnapshotController.saveSnapshot` L78-101）。
    ///
    /// `turnCount` 取 user 消息条数；`createdAt` 取当前时刻（旧 `Instant.now()`）；
    /// `metadata` 固定五键——`title` / `workingDir` / `status` / `totalCostUsd`
    /// 恒写出，`totalInputTokens` / `totalOutputTokens` 在旧端受
    /// `totalUsage != null` 门控，而本端 [`zk_db::SessionDetail::total_usage`]
    /// 非可空（缺行即 0），故恒写出。
    #[must_use]
    pub fn from_session_detail(detail: &SessionDetail) -> Self {
        let turn_count = detail
            .messages
            .iter()
            .filter(|message| message.role == MessageRole::User)
            .count();
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "title".to_owned(),
            detail
                .title
                .clone()
                .map_or(serde_json::Value::Null, serde_json::Value::String),
        );
        metadata.insert(
            "workingDir".to_owned(),
            serde_json::Value::String(detail.working_dir.clone()),
        );
        metadata.insert(
            "status".to_owned(),
            serde_json::Value::String(detail.status.clone()),
        );
        metadata.insert(
            "totalInputTokens".to_owned(),
            detail.total_usage.input_tokens.into(),
        );
        metadata.insert(
            "totalOutputTokens".to_owned(),
            detail.total_usage.output_tokens.into(),
        );
        metadata.insert(
            "totalCostUsd".to_owned(),
            serde_json::Number::from_f64(detail.total_cost_usd)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
        );
        Self {
            session_id: Some(detail.session_id.clone()),
            messages: detail.messages.clone(),
            model: Some(detail.model.clone()),
            turn_count: i64::try_from(turn_count).unwrap_or(i64::MAX),
            created_at: Some(now_millis()),
            metadata,
        }
    }

    /// 取摘要（旧控制器两处 `new SessionSnapshotSummary(...)` 与
    /// `listSnapshots` 的同一投影）。
    #[must_use]
    pub fn summary(&self) -> SessionSnapshotSummary {
        SessionSnapshotSummary {
            session_id: self.session_id.clone(),
            model: self.model.clone(),
            turn_count: self.turn_count,
            message_count: self.messages.len(),
            created_at: self.created_at,
        }
    }
}

// ==================== 服务 ====================

/// 会话快照存储（旧 `SessionSnapshotService` `@Service` 单例）。
#[derive(Debug)]
pub struct SessionSnapshotService {
    /// 快照目录（构造时 `create_dir_all`）。
    snapshot_dir: PathBuf,
}

impl SessionSnapshotService {
    /// 用户级默认目录 `~/.zk/snapshots/`（旧 `~/.zhiku/snapshots/`）。
    #[must_use]
    pub fn new() -> Self {
        Self::with_dir(zk_core::paths::user_config_dir().join(SNAPSHOT_DIR_NAME))
    }

    /// 指定快照目录（测试与自定义部署用）。
    ///
    /// 建目录失败只 `warn`（旧构造器同）：快照能力随后整体不可用，但服务照常起。
    #[must_use]
    pub fn with_dir(snapshot_dir: PathBuf) -> Self {
        match std::fs::create_dir_all(&snapshot_dir) {
            Ok(()) => tracing::info!(dir = %snapshot_dir.display(), "Snapshot directory ready"),
            Err(error) => tracing::warn!(
                dir = %snapshot_dir.display(),
                %error,
                "Failed to create snapshot directory. Snapshot feature may be unavailable."
            ),
        }
        Self { snapshot_dir }
    }

    /// 快照目录。
    #[must_use]
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    /// 快照文件路径（旧 `snapshotDir.resolve(sessionId + ".json")`）。
    fn snapshot_file(&self, session_id: &str) -> PathBuf {
        self.snapshot_dir
            .join(format!("{session_id}{SNAPSHOT_FILE_SUFFIX}"))
    }

    /// 保存快照（旧 `saveSnapshot`）。
    ///
    /// 序列化 / IO 失败只记 `error` 并返回 `Ok(())`（旧实现 void + `log.error`）。
    ///
    /// # Errors
    ///
    /// `session_id` 不过路径守卫时返回 [`InvalidSessionId`]（唯一失败面）。
    pub async fn save_snapshot(
        &self,
        session_id: &str,
        snapshot: &SessionSnapshot,
    ) -> Result<(), InvalidSessionId> {
        validate_session_id(session_id)?;
        let file = self.snapshot_file(session_id);
        match serde_json::to_vec_pretty(snapshot) {
            Ok(bytes) => match replace_file(&file, &bytes).await {
                Ok(()) => tracing::info!(
                    session_id,
                    messages = snapshot.messages.len(),
                    model = snapshot.model.as_deref().unwrap_or_default(),
                    "Session snapshot saved"
                ),
                Err(error) => tracing::error!(
                    session_id,
                    %error,
                    "Failed to save snapshot for session"
                ),
            },
            Err(error) => tracing::error!(
                session_id,
                %error,
                "Failed to serialize snapshot for session"
            ),
        }
        Ok(())
    }

    /// 加载快照（旧 `loadSnapshot`）。
    ///
    /// 文件缺失 → `Ok(None)`（旧 `Optional.empty()` + `log.debug`）；读或解析
    /// 失败 → 记 `error` 后同样 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// `session_id` 不过路径守卫时返回 [`InvalidSessionId`]（唯一失败面）。
    pub async fn load_snapshot(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionSnapshot>, InvalidSessionId> {
        validate_session_id(session_id)?;
        let file = self.snapshot_file(session_id);
        let bytes = match tokio::fs::read(&file).await {
            Ok(bytes) => bytes,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    tracing::debug!(session_id, "No snapshot found for session");
                } else {
                    tracing::error!(session_id, %error, "Failed to load snapshot for session");
                }
                return Ok(None);
            }
        };
        match serde_json::from_slice::<SessionSnapshot>(&bytes) {
            Ok(snapshot) => {
                tracing::info!(
                    session_id,
                    messages = snapshot.messages.len(),
                    model = snapshot.model.as_deref().unwrap_or_default(),
                    "Session snapshot loaded"
                );
                Ok(Some(snapshot))
            }
            Err(error) => {
                tracing::error!(session_id, %error, "Failed to load snapshot for session");
                Ok(None)
            }
        }
    }

    /// 列出全部快照摘要，按 `createdAt` 降序（旧 `listSnapshots`）。
    ///
    /// 目录不存在 → 空表。单个文件读或解析失败只 `warn` 并跳过（旧同）。
    /// 排序缺省值为 EPOCH（旧 `createdAt != null ? createdAt : Instant.EPOCH`），
    /// 且排序稳定——等值条目保持目录遍历序。
    ///
    /// **注意**：旧实现「读取顶层字段生成摘要」的文档说明与代码不符（`readValue`
    /// 读的是完整快照，含全量 `messages`）。本移植以代码为准，同样整读。
    pub async fn list_snapshots(&self) -> Vec<SessionSnapshotSummary> {
        let mut dir = match tokio::fs::read_dir(&self.snapshot_dir).await {
            Ok(dir) => dir,
            Err(error) => {
                // 旧 `!Files.isDirectory` → 静默空表；其余 IO 失败 → log.error。
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::error!(%error, "Failed to list snapshot directory");
                }
                return Vec::new();
            }
        };

        let mut summaries: Vec<SessionSnapshotSummary> = Vec::new();
        loop {
            match dir.next_entry().await {
                Ok(Some(entry)) => {
                    let path = entry.path();
                    if !path.to_string_lossy().ends_with(SNAPSHOT_FILE_SUFFIX) {
                        continue;
                    }
                    match tokio::fs::read(&path).await {
                        Ok(bytes) => match serde_json::from_slice::<SessionSnapshot>(&bytes) {
                            Ok(snapshot) => summaries.push(snapshot.summary()),
                            Err(error) => tracing::warn!(
                                file = %path.display(),
                                %error,
                                "Failed to read snapshot file"
                            ),
                        },
                        Err(error) => tracing::warn!(
                            file = %path.display(),
                            %error,
                            "Failed to read snapshot file"
                        ),
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::error!(%error, "Failed to list snapshot directory");
                    break;
                }
            }
        }

        // 旧 `Comparator.comparing(createdAt ?: EPOCH).reversed()`——稳定降序。
        summaries.sort_by(|left, right| {
            right
                .created_at
                .unwrap_or(0)
                .cmp(&left.created_at.unwrap_or(0))
        });
        summaries
    }

    /// 删除快照（旧 `deleteSnapshot`）。
    ///
    /// 返回是否确有文件被删除；不存在 → `false`（旧 `Files.deleteIfExists`），
    /// 其余 IO 失败 → 记 `error` 后同样 `false`。
    ///
    /// # Errors
    ///
    /// `session_id` 不过路径守卫时返回 [`InvalidSessionId`]（唯一失败面）。
    pub async fn delete_snapshot(&self, session_id: &str) -> Result<bool, InvalidSessionId> {
        validate_session_id(session_id)?;
        let file = self.snapshot_file(session_id);
        match tokio::fs::remove_file(&file).await {
            Ok(()) => {
                tracing::info!(session_id, "Session snapshot deleted");
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(session_id, "No snapshot to delete for session");
                Ok(false)
            }
            Err(error) => {
                tracing::error!(session_id, %error, "Failed to delete snapshot for session");
                Ok(false)
            }
        }
    }
}

impl Default for SessionSnapshotService {
    fn default() -> Self {
        Self::new()
    }
}

/// `sessionId` 守卫（旧 `validateSessionId`）：空白、`..`、`/`、`\` 一律拒绝。
fn validate_session_id(session_id: &str) -> Result<(), InvalidSessionId> {
    if session_id.trim().is_empty()
        || session_id.contains("..")
        || session_id.contains('/')
        || session_id.contains('\\')
    {
        return Err(InvalidSessionId);
    }
    Ok(())
}

/// `null` → [`Default`]（差异 3）：`#[serde(default)]` 只覆盖**缺键**，显式
/// `null` 仍会让非 [`Option`] 字段解析失败，而旧端 Jackson 对这三个分量都接受
/// `null`。
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// 原子替换：写 `{file}.tmp` → rename（差异 1，见模块文档）。
async fn replace_file(file: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut temp = file.to_path_buf().into_os_string();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, file).await
}

/// `Option<i64>`（epoch 毫秒）↔ RFC 3339 字符串 / `null` 的 serde 适配。
///
/// 与 `zk-db` 的 `serde_iso_ms` 同算法（该模块 `pub(crate)`，跨 crate 不可
/// 见），额外承载 `null`：旧 record 的 `Instant createdAt` 可为 `null`。
/// 反序列化同时接受数字毫秒（对齐 `FlexEpoch` 输入域）。
mod serde_iso_ms_opt {
    use super::{format_rfc3339_micros, parse_rfc3339_millis};
    use serde::{Deserialize, Deserializer, Serializer};

    /// `Some` → RFC 3339 字符串；`None` → `null`。
    // serde `with` 约定签名为 `&Option<T>`，无法改传 `Option<&T>`——定点豁免
    // idiom lint（同 zk-db `serde_iso_ms` 对 `trivially_copy_pass_by_ref` 的处理）。
    #[allow(clippy::ref_option)]
    pub(super) fn serialize<S: Serializer>(
        value: &Option<i64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(millis) => serializer.serialize_str(&format_rfc3339_micros(*millis)),
            None => serializer.serialize_none(),
        }
    }

    /// `null` → `None`；数字 → 毫秒；字符串 → RFC 3339 解析。
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<i64>, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::Number(number) => number
                .as_i64()
                .map(Some)
                .ok_or_else(|| serde::de::Error::custom("epoch milliseconds must be an integer")),
            serde_json::Value::String(text) => {
                parse_rfc3339_millis(&text).map(Some).ok_or_else(|| {
                    serde::de::Error::custom(format!("invalid RFC 3339 timestamp: {text}"))
                })
            }
            other => Err(serde::de::Error::custom(format!(
                "createdAt must be a timestamp: {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// 独占快照目录（同进程内并发测试互不干扰）。
    fn fixture() -> PathBuf {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        let root = std::env::temp_dir().join(format!(
            "zk-snapshot-svc-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    /// 造一条 user 文本消息。
    fn message(id: &str, role: MessageRole, seq: i64) -> MessageRecord {
        MessageRecord {
            id: id.to_owned(),
            session_id: "s-1".to_owned(),
            role,
            content: vec![zk_db::StoredBlock::Text {
                text: format!("body {id}"),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: seq,
            created_at: 1_700_000_000_000,
        }
    }

    /// 造一份最小快照。
    fn snapshot(session_id: &str, created_at: Option<i64>) -> SessionSnapshot {
        SessionSnapshot {
            session_id: Some(session_id.to_owned()),
            messages: vec![message("m-1", MessageRole::User, 1)],
            model: Some("claude-sonnet-4".to_owned()),
            turn_count: 1,
            created_at,
            metadata: serde_json::Map::new(),
        }
    }

    /// 构造即建目录（旧构造器 `Files.createDirectories`）。
    #[test]
    fn constructor_creates_snapshot_directory() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        assert!(service.snapshot_dir().is_dir());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 缺省目录挂在 `~/.zk/snapshots`（不建目录，只验路径拼接）。
    #[test]
    fn default_directory_nests_under_user_config_dir() {
        let expected = zk_core::paths::user_config_dir().join(SNAPSHOT_DIR_NAME);
        assert_eq!(
            expected.file_name().map(std::ffi::OsStr::to_string_lossy),
            Some(SNAPSHOT_DIR_NAME.into())
        );
    }

    /// url 型图片块（无 base64 载荷）经快照保存 → 加载逐字等价。
    #[tokio::test]
    async fn url_image_blocks_survive_snapshot_round_trip() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        let mut original = snapshot("s-url", Some(1_700_000_123_456));
        original.messages[0]
            .content
            .push(zk_db::StoredBlock::Image {
                source: zk_db::model::ImageSource {
                    kind: "url".into(),
                    media_type: None,
                    data: None,
                    url: Some(
                        "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png".into(),
                    ),
                },
                width: None,
                height: None,
            });
        service
            .save_snapshot("s-url", &original)
            .await
            .expect("save");
        let loaded = service
            .load_snapshot("s-url")
            .await
            .expect("load")
            .expect("snapshot present");
        assert_eq!(loaded.messages, original.messages);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 保存 → 加载往返：全部分量逐字等价，文件名为 `{sessionId}.json`。
    #[tokio::test]
    async fn save_then_load_round_trips_all_fields() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        let mut original = snapshot("s-1", Some(1_700_000_123_456));
        original.metadata.insert(
            "title".to_owned(),
            serde_json::Value::String("t".to_owned()),
        );
        original.metadata.insert(
            "totalInputTokens".to_owned(),
            serde_json::Value::from(42_i64),
        );

        service.save_snapshot("s-1", &original).await.expect("save");
        assert!(root.join("s-1.json").is_file());
        // 临时文件不得残留。
        assert!(!root.join("s-1.json.tmp").exists());

        let loaded = service
            .load_snapshot("s-1")
            .await
            .expect("load")
            .expect("snapshot present");
        assert_eq!(loaded, original);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 落盘形状：camelCase 键 + `createdAt` 为 RFC 3339 字符串。
    #[tokio::test]
    async fn saved_file_uses_camel_case_and_iso_timestamp() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        service
            .save_snapshot("s-1", &snapshot("s-1", Some(0)))
            .await
            .expect("save");

        let raw = std::fs::read_to_string(root.join("s-1.json")).expect("read");
        let parsed: serde_json::Value = serde_json::from_str(&raw).expect("valid json");
        let mut keys: Vec<&str> = parsed
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "createdAt",
                "messages",
                "metadata",
                "model",
                "sessionId",
                "turnCount"
            ]
        );
        assert_eq!(parsed["createdAt"], "1970-01-01T00:00:00.000000Z");
        assert!(raw.contains('\n'), "pretty printer must emit line breaks");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 缺文件 → `None`；损坏 JSON → `None` 且文件保留（不静默删用户数据）。
    #[tokio::test]
    async fn load_returns_none_for_missing_and_corrupt_files() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        assert_eq!(service.load_snapshot("absent").await.expect("load"), None);

        std::fs::write(root.join("broken.json"), "{ not json").expect("seed corrupt");
        assert_eq!(service.load_snapshot("broken").await.expect("load"), None);
        assert!(root.join("broken.json").is_file());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// `createdAt` 接受数字毫秒与 `null`；`messages` / `metadata` / `turnCount`
    /// 缺键时取缺省（旧 Jackson 缺省语义）。
    #[tokio::test]
    async fn load_accepts_numeric_null_and_missing_fields() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());

        std::fs::write(root.join("numeric.json"), "{\"createdAt\":1700000000000}")
            .expect("seed numeric");
        let numeric = service
            .load_snapshot("numeric")
            .await
            .expect("load")
            .expect("present");
        assert_eq!(numeric.created_at, Some(1_700_000_000_000));
        assert_eq!(numeric.turn_count, 0);
        assert!(numeric.messages.is_empty());
        assert!(numeric.metadata.is_empty());
        assert_eq!(numeric.session_id, None);
        assert_eq!(numeric.model, None);

        std::fs::write(
            root.join("nulls.json"),
            "{\"sessionId\":null,\"model\":null,\"createdAt\":null,\"metadata\":null,\
             \"messages\":null,\"turnCount\":null}",
        )
        .expect("seed nulls");
        let nulls = service
            .load_snapshot("nulls")
            .await
            .expect("load")
            .expect("present");
        assert_eq!(nulls.created_at, None);
        assert_eq!(nulls.session_id, None);
        assert!(nulls.metadata.is_empty());
        assert!(nulls.messages.is_empty());
        assert_eq!(nulls.turn_count, 0);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 列表按 `createdAt` 降序，`null` 视作 EPOCH 垫底。
    #[tokio::test]
    async fn list_sorts_by_created_at_descending_with_epoch_fallback() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        for (session_id, created_at) in [
            ("old", Some(1_000)),
            ("newest", Some(9_000)),
            ("undated", None),
            ("middle", Some(5_000)),
        ] {
            service
                .save_snapshot(session_id, &snapshot(session_id, created_at))
                .await
                .expect("save");
        }

        let listed = service.list_snapshots().await;
        let ids: Vec<&str> = listed
            .iter()
            .map(|summary| summary.session_id.as_deref().unwrap_or_default())
            .collect();
        assert_eq!(ids, vec!["newest", "middle", "old", "undated"]);
        assert_eq!(listed[0].message_count, 1);
        assert_eq!(listed[0].turn_count, 1);
        assert_eq!(listed[0].model.as_deref(), Some("claude-sonnet-4"));

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 非 `.json` 文件与不可解析文件都跳过；目录缺失 → 空表。
    #[tokio::test]
    async fn list_skips_unrelated_and_unparsable_files() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        service
            .save_snapshot("good", &snapshot("good", Some(1)))
            .await
            .expect("save");
        std::fs::write(root.join("notes.txt"), "ignored").expect("seed txt");
        std::fs::write(root.join("broken.json"), "{ not json").expect("seed corrupt");

        let listed = service.list_snapshots().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].session_id.as_deref(), Some("good"));

        std::fs::remove_dir_all(&root).expect("drop dir");
        assert!(service.list_snapshots().await.is_empty());
    }

    /// 删除：命中 → `true`，重复删除 → `false`。
    #[tokio::test]
    async fn delete_reports_whether_a_file_was_removed() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        service
            .save_snapshot("s-1", &snapshot("s-1", Some(1)))
            .await
            .expect("save");

        assert!(service.delete_snapshot("s-1").await.expect("delete"));
        assert!(!service.delete_snapshot("s-1").await.expect("delete"));
        assert!(!root.join("s-1.json").exists());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// 四个入口一律先过 `sessionId` 守卫（旧 `validateSessionId`）。
    #[tokio::test]
    async fn all_entry_points_reject_traversal_and_separators() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        for session_id in ["", "   ", "..", "../etc/passwd", "a/b", "a\\b", "x..y"] {
            assert!(
                service
                    .save_snapshot(session_id, &snapshot("s", Some(1)))
                    .await
                    .is_err(),
                "save accepted {session_id:?}"
            );
            assert!(
                service.load_snapshot(session_id).await.is_err(),
                "load accepted {session_id:?}"
            );
            assert!(
                service.delete_snapshot(session_id).await.is_err(),
                "delete accepted {session_id:?}"
            );
        }
        // 守卫必须在触碰磁盘之前生效：目录里不得留下任何文件。
        assert_eq!(
            std::fs::read_dir(&root).expect("read dir").count(),
            0,
            "guard must reject before any IO"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 合法 `sessionId`（UUID 形）放行。
    #[tokio::test]
    async fn valid_session_ids_pass_the_guard() {
        let root = fixture();
        let service = SessionSnapshotService::with_dir(root.clone());
        for session_id in ["s1", "3f2b9c48-0a1d-4c9e-8b77-1a2b3c4d5e6f", "a.b"] {
            service
                .save_snapshot(session_id, &snapshot(session_id, Some(1)))
                .await
                .expect("save");
            assert!(
                service
                    .load_snapshot(session_id)
                    .await
                    .expect("load")
                    .is_some()
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 由会话明细装配：`turnCount` 只数 user 消息，`metadata` 六键齐备。
    #[test]
    fn from_session_detail_counts_user_turns_and_fills_metadata() {
        let detail = SessionDetail {
            session_id: "s-9".to_owned(),
            model: "claude-opus-4".to_owned(),
            working_dir: "/tmp/work".to_owned(),
            title: Some("hello".to_owned()),
            status: "active".to_owned(),
            messages: vec![
                message("m-1", MessageRole::User, 1),
                message("m-2", MessageRole::Assistant, 2),
                message("m-3", MessageRole::User, 3),
                message("m-4", MessageRole::System, 4),
            ],
            config: serde_json::Map::new(),
            total_usage: zk_protocol::Usage {
                input_tokens: 11,
                output_tokens: 22,
                cache_read_input_tokens: 3,
                cache_creation_input_tokens: 4,
            },
            total_cost_usd: 0.5,
            summary: None,
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_001_000,
        };

        let snapshot = SessionSnapshot::from_session_detail(&detail);
        assert_eq!(snapshot.session_id.as_deref(), Some("s-9"));
        assert_eq!(snapshot.model.as_deref(), Some("claude-opus-4"));
        assert_eq!(snapshot.turn_count, 2, "只数 user 消息");
        assert_eq!(snapshot.messages.len(), 4, "消息历史整份带走");
        assert!(snapshot.created_at.is_some());

        let mut keys: Vec<&str> = snapshot.metadata.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "status",
                "title",
                "totalCostUsd",
                "totalInputTokens",
                "totalOutputTokens",
                "workingDir"
            ]
        );
        assert_eq!(snapshot.metadata["title"], "hello");
        assert_eq!(snapshot.metadata["workingDir"], "/tmp/work");
        assert_eq!(snapshot.metadata["status"], "active");
        assert_eq!(snapshot.metadata["totalInputTokens"], 11);
        assert_eq!(snapshot.metadata["totalOutputTokens"], 22);
        assert_eq!(snapshot.metadata["totalCostUsd"], 0.5);

        // 摘要投影与快照同源。
        let summary = snapshot.summary();
        assert_eq!(summary.session_id, snapshot.session_id);
        assert_eq!(summary.message_count, 4);
        assert_eq!(summary.turn_count, 2);
        assert_eq!(summary.created_at, snapshot.created_at);
    }

    /// 无标题会话 → `metadata.title` 为 `null`（旧 `HashMap.put(null)` 同）。
    #[test]
    fn from_session_detail_keeps_null_title() {
        let detail = SessionDetail {
            session_id: "s-10".to_owned(),
            model: "m".to_owned(),
            working_dir: "/tmp".to_owned(),
            title: None,
            status: "active".to_owned(),
            messages: Vec::new(),
            config: serde_json::Map::new(),
            total_usage: zk_protocol::Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 0,
            updated_at: 0,
        };
        let snapshot = SessionSnapshot::from_session_detail(&detail);
        assert_eq!(snapshot.metadata["title"], serde_json::Value::Null);
        assert_eq!(snapshot.turn_count, 0);
        assert_eq!(snapshot.summary().message_count, 0);
    }

    /// 摘要序列化形状：五键 camelCase，`createdAt` 为 ISO / `null`。
    #[test]
    fn summary_serializes_five_camel_case_keys() {
        let summary = snapshot("s-1", Some(0)).summary();
        let value = serde_json::to_value(&summary).expect("serialize");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "createdAt",
                "messageCount",
                "model",
                "sessionId",
                "turnCount"
            ]
        );
        assert_eq!(value["createdAt"], "1970-01-01T00:00:00.000000Z");

        let undated = snapshot("s-1", None).summary();
        let value = serde_json::to_value(&undated).expect("serialize");
        assert_eq!(value["createdAt"], serde_json::Value::Null);
    }
}
