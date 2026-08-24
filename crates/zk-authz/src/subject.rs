//! 授权主体解析（root session / root run / workspace 身份）。
//!
//! 逐字移植 `authorization/AuthorizationSubjectResolver.java`（84 行）。
//!
//! 关键语义（逐字保留）：
//! - `MAX_DEPTH = 32`：父链上溯深度上限，超出即 `AUTHORIZATION_ANCESTRY_INVALID`。
//! - 上溯 `run_envelopes.parent_run_id` 时**不** join `sessions`——子代理使用合成
//!   会话 ID 且无 `sessions` 记录，join 会把整条祖先链判为无效。
//! - `seen` 集合防环；命中环等同链路损坏。
//! - 到达根 Run 后才查 `sessions.working_dir`，再经
//!   [`WorkspaceIdentityService::resolve`] 得 `authorizationRoot` + `workspaceKey`。
//!
//! 缓存：旧源用 Caffeine `maximumSize(4096).expireAfterAccess(30min)`。zkcode 以
//! `Mutex<HashMap>` + 插入时刻戳实现同容量/同过期语义（见 docs §8 偏离表 S-01(a)）。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::{OptionalExtension, params};
use zk_db::Db;

use crate::model::{AuthorizationSubject, AuthzError, AuthzResult};
use crate::workspace::WorkspaceIdentityService;

/// 旧源 `MAX_DEPTH`（`AuthorizationSubjectResolver.java:24`）。
const MAX_DEPTH: usize = 32;
/// 旧源 Caffeine `maximumSize(4096)`。
const MAX_CACHE_ENTRIES: usize = 4096;
/// 旧源 Caffeine `expireAfterAccess(Duration.ofMinutes(30))`。
const CACHE_TTL_MILLIS: i64 = 30 * 60 * 1000;

/// 根 Run 的解析结果（旧源私有 `RootIdentity` 等价物）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct RootIdentity {
    root_session_id: String,
    root_run_id: String,
    workspace_key: String,
    authorization_root: PathBuf,
}

/// 授权主体解析器。
pub struct AuthorizationSubjectResolver {
    db: Db,
    workspaces: WorkspaceIdentityService,
    cache: Mutex<HashMap<String, (i64, RootIdentity)>>,
}

impl std::fmt::Debug for AuthorizationSubjectResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationSubjectResolver")
            .finish_non_exhaustive()
    }
}

impl AuthorizationSubjectResolver {
    /// 构造解析器。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            db,
            workspaces: WorkspaceIdentityService,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// 旧源 `resolve(String currentRunId)`（`AuthorizationSubjectResolver.java:37-48`）。
    ///
    /// # Errors
    /// `current_run_id` 缺失/为空、Run 不存在、或祖先链不可解析时返回
    /// `AUTHORIZATION_ANCESTRY_INVALID`。
    pub async fn resolve(&self, current_run_id: Option<&str>) -> AuthzResult<AuthorizationSubject> {
        let Some(current_run_id) = current_run_id.filter(|id| !id.trim().is_empty()) else {
            // 旧源 `AuthorizationSubjectResolver.java:37-39`（`isBlank()` → 纯空白亦拒绝）。
            return Err(ancestry_invalid("Tool execution requires a persisted Run"));
        };
        if let Some(root) = self.cached(current_run_id) {
            return Ok(subject_of(&root, current_run_id));
        }
        let root = self.load_root(current_run_id).await?;
        self.remember(current_run_id, &root);
        Ok(subject_of(&root, current_run_id))
    }

    /// 清空缓存（Run 生命周期结束或工作区重绑定后调用）。
    pub fn invalidate_all(&self) {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn cached(&self, run_id: &str) -> Option<RootIdentity> {
        let now = zk_db::time::now_millis();
        let mut guard = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.get_mut(run_id) {
            Some((touched, root)) if now - *touched <= CACHE_TTL_MILLIS => {
                // expireAfterAccess：命中即续期。
                *touched = now;
                Some(root.clone())
            }
            Some(_) => {
                guard.remove(run_id);
                None
            }
            None => None,
        }
    }

    fn remember(&self, run_id: &str, root: &RootIdentity) {
        let now = zk_db::time::now_millis();
        let mut guard = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.len() >= MAX_CACHE_ENTRIES {
            // Caffeine 的 W-TinyLFU 驱逐不可逐字复刻；以最旧访问时刻驱逐一条，
            // 保持「容量恒不超过 4096」的可观察不变量。
            if let Some(oldest) = guard
                .iter()
                .min_by_key(|(_, (touched, _))| *touched)
                .map(|(key, _)| key.clone())
            {
                guard.remove(&oldest);
            }
        }
        guard.insert(run_id.to_owned(), (now, root.clone()));
    }

    /// 旧源 `loadRoot`（`AuthorizationSubjectResolver.java:50-76`）。
    ///
    /// 四条失败消息逐字对齐旧源：`cycle`(L54) / `missing parent`(L60) /
    /// `Root session is missing or ambiguous`(L67) / `exceeds N levels`(L75)。
    async fn load_root(&self, current_run_id: &str) -> AuthzResult<RootIdentity> {
        let run_id = current_run_id.to_owned();
        let chain = self
            .db
            .with_reader(move |conn| {
                let mut seen: HashSet<String> = HashSet::new();
                let mut cursor = run_id;
                // 旧源 `for (int depth = 0; depth <= MAX_DEPTH; depth++)`：33 次迭代。
                for _ in 0..=MAX_DEPTH {
                    if !seen.insert(cursor.clone()) {
                        return Ok(Err(ancestry_invalid("Run ancestry contains a cycle")));
                    }
                    // 逐字保留：只查 run_envelopes，不 join sessions。
                    let row: Option<(String, String, Option<String>)> = conn
                        .query_row(
                            "SELECT id,session_id,parent_run_id FROM run_envelopes WHERE id=?1",
                            params![cursor],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .optional()?;
                    let Some((id, session_id, parent_run_id)) = row else {
                        return Ok(Err(ancestry_invalid(
                            "Run ancestry contains a missing parent",
                        )));
                    };
                    let Some(parent) = parent_run_id else {
                        // 抵达根 Run 后才解析会话工作目录（旧源 L62-71）。
                        let working_dir: Option<Option<String>> = conn
                            .query_row(
                                "SELECT working_dir FROM sessions WHERE id=?1",
                                params![session_id],
                                |row| row.get(0),
                            )
                            .optional()?;
                        // 旧源仅判「行数 != 1」；`working_dir` 为 NULL 时旧源会
                        // `Path.of(null)` NPE，Rust 侧一并失败关闭到同一消息。
                        let Some(Some(working_dir)) = working_dir else {
                            return Ok(Err(ancestry_invalid(
                                "Root session is missing or ambiguous",
                            )));
                        };
                        return Ok(Ok((id, session_id, working_dir)));
                    };
                    cursor = parent;
                }
                Ok(Err(ancestry_invalid(&format!(
                    "Run ancestry exceeds {MAX_DEPTH} levels"
                ))))
            })
            .await
            .map_err(|failure| {
                ancestry_invalid(&format!("Authorization ancestry lookup failed: {failure}"))
            })?;

        let (root_run_id, root_session_id, working_dir) = chain?;
        let identity = self
            .workspaces
            .resolve(std::path::Path::new(&working_dir))?;
        Ok(RootIdentity {
            root_session_id,
            root_run_id,
            workspace_key: identity.workspace_key,
            authorization_root: identity.authorization_root,
        })
    }
}

/// 上溯到的根身份 + 当前 Run 组成主体（旧源 `resolve` 尾部构造）。
fn subject_of(root: &RootIdentity, current_run_id: &str) -> AuthorizationSubject {
    AuthorizationSubject {
        root_session_id: root.root_session_id.clone(),
        root_run_id: root.root_run_id.clone(),
        current_run_id: current_run_id.to_owned(),
        workspace_key: root.workspace_key.clone(),
        authorization_root: root.authorization_root.clone(),
    }
}

fn ancestry_invalid(message: &str) -> AuthzError {
    AuthzError::new("AUTHORIZATION_ANCESTRY_INVALID", message)
}
