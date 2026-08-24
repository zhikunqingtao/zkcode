//! `permission_grants` 授权记录仓储。
//!
//! 逐字移植 `authorization/PermissionGrantRepository.java`（402 行）——V019 授权
//! 记录的唯一权威；读取不改变状态，所有写入均有等待上限。
//!
//! # 与旧源的结构映射
//!
//! | 旧源成员 | 本模块 |
//! |---|---|
//! | `findMatch` (L61-90) | [`find_match_in_tx`] / [`GrantStore::find_match`] |
//! | `create` (L93-96) | [`GrantStore::create`] |
//! | `createInCurrentTransaction` (L99-152) | [`create_in_tx`] |
//! | `revoke` (L154-160) | [`GrantStore::revoke`] |
//! | `listActiveForSession` (L162-174) | [`GrantStore::list_active_for_session`] |
//! | `revokeForSession` (L176-182) | [`GrantStore::revoke_for_session`] |
//! | `supportedScopes` (L185-197) | [`supported_scopes`] |
//! | `revokeRunScoped` (L211-217) | [`GrantStore::revoke_run_scoped`] |
//! | `plan` (L219-251) | [`plan`] |
//! | `capabilityConstraint` (L253-276) | [`capability_constraint`] |
//! | `identity` (L278-286) | [`identity`] |
//! | `rootRunIsTerminal` (L288-294) | [`root_run_is_terminal`] |
//! | `workspaceForSession` (L296-306) | [`workspace_for_session`] |
//! | `exactIdentity` (L308-327) | [`exact_identity`] |
//! | `writeConstraint` (L329-350) | [`encode_constraint`] |
//! | `decodeConstraint` (L352-377) | [`decode_constraint`] |
//! | `directoryOf` (L385-395) | [`directory_of`] |
//!
//! 旧源 `boundedWrite` = `sqlite.executeWriteBounded(dbPath, 5s, body)`。zkcode 的
//! 等价物是 zk-db 的单 writer `Mutex<Connection>` + `busy_timeout=5s`（D-P2-4），
//! 因此本模块不再自持等待上限——见 docs §8 偏离表 G-01（EQUIVALENT）。

use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value};
use zk_db::{Db, DbError};

use crate::constraint::{self, GrantConstraint};
use crate::hashing;
use crate::model::AuthorizationSubject;
use crate::model::{
    DelegationPolicy, EffectClass, GrantKind, OperationDescriptor, PermissionScope, RiskClass,
    TypedFileOperation,
};
use crate::workspace::WorkspaceIdentityService;

/// 旧源 `REMOTE_CAPABILITY_ANALYZERS`（`PermissionGrantRepository.java:36-37`）。
const REMOTE_CAPABILITY_ANALYZERS: [&str; 2] = ["network-v1", "mcp-v1"];

/// 旧源 `Match` record（`PermissionGrantRepository.java:39`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantMatch {
    /// 命中记录的主键。
    pub grant_id: String,
    /// 记录种类。
    pub kind: GrantKind,
    /// 记录作用域。
    pub scope: PermissionScope,
}

/// 旧源 `GrantView` record（`PermissionGrantRepository.java:40-41`）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantView {
    /// 记录主键。
    pub grant_id: String,
    /// `grant_kind` 原始字符串。
    pub kind: String,
    /// `scope` 原始字符串。
    pub scope: String,
    /// 工具名。
    pub tool_name: String,
    /// 动作。
    pub action: String,
    /// 旧源此列取 `constraints_json`（第 6 列），字段名为 `summary`。
    pub summary: String,
    /// 创建时刻（ISO）。
    pub created_at: String,
    /// 过期时刻（ISO）。
    pub expires_at: String,
}

/// 旧源私有 `Identity` record（`PermissionGrantRepository.java:397`）。
#[derive(Debug, Clone, Default)]
struct Identity {
    root_session_id: Option<String>,
    root_run_id: Option<String>,
    actor_run_id: Option<String>,
    workspace_key: Option<String>,
}

/// 旧源私有 `GrantPlan` record（`PermissionGrantRepository.java:398-399`）。
#[derive(Debug, Clone)]
pub struct GrantPlan {
    /// 记录种类。
    pub kind: GrantKind,
    /// 落库作用域（永不为 `Once`）。
    pub scope: PermissionScope,
    /// 委派策略。
    pub delegation: DelegationPolicy,
    /// 封闭约束。
    pub constraint: GrantConstraint,
}

/// 时钟端口（旧源注入 `java.time.Clock`，测试用固定时钟）。
pub trait Clock: Send + Sync {
    /// 当前 epoch 毫秒。
    fn now_millis(&self) -> i64;

    /// 当前时刻的 ISO 表示（zkcode 恒 6 位微秒）。
    fn now_iso(&self) -> String {
        zk_db::time::format_rfc3339_micros(self.now_millis())
    }
}

/// 系统时钟。
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> i64 {
        zk_db::time::now_millis()
    }
}

/// 固定时刻时钟（测试用；旧源 `Clock.fixed(...)`）。
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub i64);

impl Clock for FixedClock {
    fn now_millis(&self) -> i64 {
        self.0
    }
}

/// 授权记录仓储。
///
/// `Clone` 为浅克隆（`Db` 内部 `Arc` 连接池 + `Arc<dyn Clock>`），组合根与
/// 测试可自由分发同一份仓储。
#[derive(Clone)]
pub struct GrantStore {
    db: Db,
    clock: Arc<dyn Clock>,
    workspaces: WorkspaceIdentityService,
}

impl std::fmt::Debug for GrantStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrantStore").finish_non_exhaustive()
    }
}

impl GrantStore {
    /// 以系统时钟构造。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self::with_clock(db, Arc::new(SystemClock))
    }

    /// 以显式时钟构造（旧源测试构造器）。
    #[must_use]
    pub fn with_clock(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self {
            db,
            clock,
            workspaces: WorkspaceIdentityService,
        }
    }

    /// 当前时刻 ISO。
    #[must_use]
    pub fn now_iso(&self) -> String {
        self.clock.now_iso()
    }

    /// 旧源 `findMatch`（`PermissionGrantRepository.java:61-90`）。
    ///
    /// # Errors
    /// 读连接不可用或查询失败时返回 [`DbError`]。
    pub async fn find_match(
        &self,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> Result<Option<GrantMatch>, DbError> {
        let now = self.clock.now_iso();
        let subject = subject.clone();
        let operation = operation.clone();
        self.db
            .with_reader(move |conn| find_match_at(conn, &now, &subject, &operation))
            .await
    }

    /// 旧源 `create`（`PermissionGrantRepository.java:93-96`）：包一层有界写事务。
    ///
    /// # Errors
    /// 写连接不可用、事务提交失败或约束冲突时返回 [`DbError`]。
    pub async fn create(
        &self,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        requested_scope: Option<PermissionScope>,
        interaction_id: Option<String>,
    ) -> Result<Option<String>, DbError> {
        let now = self.clock.now_iso();
        let now_millis = self.clock.now_millis();
        let subject = subject.clone();
        let operation = operation.clone();
        self.db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                let created = create_in_tx(
                    &tx,
                    &now,
                    now_millis,
                    &subject,
                    &operation,
                    requested_scope,
                    interaction_id.as_deref(),
                )?;
                tx.commit()?;
                Ok(created)
            })
            .await
    }

    /// 旧源 `revoke`（`PermissionGrantRepository.java:154-160`）。
    ///
    /// # Errors
    /// 写连接不可用或更新失败时返回 [`DbError`]。
    pub async fn revoke(&self, grant_id: &str) -> Result<bool, DbError> {
        let now = self.clock.now_iso();
        let grant_id = grant_id.to_owned();
        self.db
            .with_writer(move |conn| {
                let changed = conn.execute(
                    "UPDATE permission_grants SET revoked_at=?1,version=version+1 \
                     WHERE grant_id=?2 AND revoked_at IS NULL",
                    params![now, grant_id],
                )?;
                Ok(changed == 1)
            })
            .await
    }

    /// 旧源 `listActiveForSession`（`PermissionGrantRepository.java:162-174`）。
    ///
    /// # Errors
    /// 读连接不可用或查询失败时返回 [`DbError`]。
    pub async fn list_active_for_session(
        &self,
        root_session_id: &str,
        requested_limit: i64,
    ) -> Result<Vec<GrantView>, DbError> {
        let limit = requested_limit.clamp(1, 500);
        let now = self.clock.now_iso();
        let root_session_id = root_session_id.to_owned();
        let workspaces = self.workspaces;
        self.db
            .with_reader(move |conn| {
                let workspace = workspace_for_session(conn, workspaces, &root_session_id);
                let mut stmt = conn.prepare(
                    "SELECT grant_id,grant_kind,scope,tool_name,action,constraints_json,\
                     created_at,expires_at FROM permission_grants \
                     WHERE revoked_at IS NULL AND expires_at>?1 \
                     AND (root_session_id=?2 OR workspace_key=?3) \
                     ORDER BY created_at DESC LIMIT ?4",
                )?;
                let rows = stmt
                    .query_map(params![now, root_session_id, workspace, limit], |row| {
                        Ok(GrantView {
                            grant_id: row.get(0)?,
                            kind: row.get(1)?,
                            scope: row.get(2)?,
                            tool_name: row.get(3)?,
                            action: row.get(4)?,
                            summary: row.get(5)?,
                            created_at: row.get(6)?,
                            expires_at: row.get(7)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
    }

    /// 旧源 `revokeForSession`（`PermissionGrantRepository.java:176-182`）。
    ///
    /// # Errors
    /// 写连接不可用或更新失败时返回 [`DbError`]。
    pub async fn revoke_for_session(
        &self,
        grant_id: &str,
        root_session_id: &str,
    ) -> Result<usize, DbError> {
        let now = self.clock.now_iso();
        let grant_id = grant_id.to_owned();
        let root_session_id = root_session_id.to_owned();
        let workspaces = self.workspaces;
        self.db
            .with_writer(move |conn| {
                let workspace = workspace_for_session(conn, workspaces, &root_session_id);
                let changed = conn.execute(
                    "UPDATE permission_grants SET revoked_at=?1,version=version+1 \
                     WHERE grant_id=?2 AND revoked_at IS NULL \
                     AND (root_session_id=?3 OR workspace_key=?4)",
                    params![now, grant_id, root_session_id, workspace],
                )?;
                Ok(changed)
            })
            .await
    }

    /// 旧源 `revokeRunScoped`（`PermissionGrantRepository.java:211-217`）。
    ///
    /// # Errors
    /// 写连接不可用或更新失败时返回 [`DbError`]。
    pub async fn revoke_run_scoped(&self, root_run_id: &str) -> Result<usize, DbError> {
        let now = self.clock.now_iso();
        let root_run_id = root_run_id.to_owned();
        self.db
            .with_writer(move |conn| {
                let changed = conn.execute(
                    "UPDATE permission_grants SET revoked_at=?1,version=version+1 \
                     WHERE scope='RUN' AND root_run_id=?2 AND revoked_at IS NULL",
                    params![now, root_run_id],
                )?;
                Ok(changed)
            })
            .await
    }
}

/// 旧源 `findMatch` 的查询主体，供外层事务直接复用。
///
/// # Errors
/// 查询失败时返回 [`DbError`]。
pub fn find_match_in_tx(
    conn: &Connection,
    now_iso: &str,
    subject: &AuthorizationSubject,
    operation: &OperationDescriptor,
) -> Result<Option<GrantMatch>, DbError> {
    find_match_at(conn, now_iso, subject, operation)
}

fn find_match_at(
    conn: &Connection,
    now: &str,
    subject: &AuthorizationSubject,
    operation: &OperationDescriptor,
) -> Result<Option<GrantMatch>, DbError> {
    // 旧源 SELECT *，但只读取 scope / grant_kind / grant_id / constraints_json 四列。
    let mut stmt = conn.prepare(
        "SELECT grant_id,grant_kind,scope,constraints_json FROM permission_grants \
         WHERE tool_name=?1 AND action=?2 AND analyzer_id=?3 \
           AND authorization_schema_version=?4 \
           AND revoked_at IS NULL AND expires_at>?5 \
           AND ((scope='RUN' AND root_run_id=?6 \
                 AND (delegation_policy='ROOT_AND_DESCENDANTS' OR actor_run_id=?7)) \
             OR (scope='SESSION' AND root_session_id=?8 \
                 AND delegation_policy='ROOT_AND_DESCENDANTS') \
             OR (scope='WORKSPACE' AND workspace_key=?9 \
                 AND delegation_policy='ROOT_AND_DESCENDANTS')) \
         ORDER BY CASE scope WHEN 'RUN' THEN 1 WHEN 'SESSION' THEN 2 ELSE 3 END, \
                  created_at DESC",
    )?;
    let candidates = stmt
        .query_map(
            params![
                operation.tool_name,
                operation.action,
                operation.analyzer_id,
                operation.authorization_schema_version,
                now,
                subject.root_run_id,
                subject.current_run_id,
                subject.root_session_id,
                subject.workspace_key,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;

    for (grant_id, grant_kind, scope, constraints_json) in candidates {
        if scope == "RUN" && root_run_is_terminal(conn, &subject.root_run_id)? {
            continue;
        }
        let Some(kind) = GrantKind::parse(&grant_kind) else {
            continue;
        };
        let Some(constraint) = decode_constraint(&grant_id, kind, &constraints_json) else {
            continue;
        };
        if constraint::matches(&constraint, operation) {
            let Some(scope) = PermissionScope::parse(&scope) else {
                continue;
            };
            tracing::debug!(
                grant_id = %grant_id,
                kind = %kind,
                scope = %scope,
                tool = %operation.tool_name,
                analyzer = %operation.analyzer_id,
                "Permission grant matched"
            );
            return Ok(Some(GrantMatch {
                grant_id,
                kind,
                scope,
            }));
        }
    }
    Ok(None)
}

/// 旧源 `createInCurrentTransaction`（`PermissionGrantRepository.java:99-152`）。
///
/// 旧源以 `TransactionSynchronizationManager.isActualTransactionActive()` 强制外层
/// 事务存在；Rust 侧由签名要求 `&Connection` 处于调用方开启的事务中来静态表达。
///
/// # Errors
/// 身份查询、约束编码或插入失败时返回 [`DbError`]。
///
/// 逐字移植的单函数决策链（作用域推导 → 能力约束 → 幂等键 → 插入），刻意不拆分
/// 以保持与旧源逐行可比，故豁免行数上限。
#[allow(clippy::too_many_lines)]
pub fn create_in_tx(
    conn: &Connection,
    now: &str,
    now_millis: i64,
    subject: &AuthorizationSubject,
    operation: &OperationDescriptor,
    requested_scope: Option<PermissionScope>,
    interaction_id: Option<&str>,
) -> Result<Option<String>, DbError> {
    let Some(plan) = plan(operation, requested_scope) else {
        return Ok(None);
    };

    // SQLite 部分索引不能包含动态时间条件，因此插入前先注销已过期记录。
    conn.execute(
        "UPDATE permission_grants SET revoked_at=?1,version=version+1 \
         WHERE revoked_at IS NULL AND expires_at<=?2",
        params![now, now],
    )?;

    let constraints_json = encode_constraint(&plan.constraint);
    let exact_kind = matches!(plan.kind, GrantKind::ExactGuarded | GrantKind::ToolGuarded);
    let capability_hash = if exact_kind {
        None
    } else {
        Some(hashing::capability_hash(
            plan.kind.as_str(),
            &constraints_json,
            operation.authorization_schema_version,
        ))
    };
    let identity = identity(subject, plan.scope, plan.delegation);
    let id = uuid::Uuid::new_v4().to_string();
    let expires = match plan.scope {
        // RUN / SESSION → +12 小时；WORKSPACE → +30 天（旧源 L119-123）。
        PermissionScope::Run | PermissionScope::Session => {
            zk_db::time::format_rfc3339_micros(now_millis + 12 * 3_600_000)
        }
        PermissionScope::Workspace => {
            zk_db::time::format_rfc3339_micros(now_millis + 30 * 86_400_000)
        }
        PermissionScope::Once => {
            return Err(DbError::Invalid("ONCE is not persisted".into()));
        }
    };
    let operation_hash = if exact_kind {
        Some(operation.operation_hash.clone())
    } else {
        None
    };
    let effects_json = serde_json::to_string(
        &operation
            .effects
            .iter()
            .map(|e| e.as_str())
            .collect::<Vec<_>>(),
    )
    .unwrap_or_else(|_| "[]".into());

    let inserted = conn.execute(
        "INSERT INTO permission_grants(grant_id,grant_kind,scope,delegation_policy,\
           root_session_id,root_run_id,actor_run_id,workspace_key,\
           authorization_schema_version,analyzer_id,tool_name,action,effects_json,\
           operation_hash,capability_hash,constraints_json,risk_class,\
           created_by_interaction_id,created_at,expires_at) \
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20) \
         ON CONFLICT DO NOTHING",
        params![
            id,
            plan.kind.as_str(),
            plan.scope.as_str(),
            plan.delegation.as_str(),
            identity.root_session_id,
            identity.root_run_id,
            identity.actor_run_id,
            identity.workspace_key,
            operation.authorization_schema_version,
            operation.analyzer_id,
            operation.tool_name,
            operation.action,
            effects_json,
            operation_hash,
            capability_hash,
            constraints_json,
            operation.risk.as_str(),
            interaction_id,
            now,
            expires,
        ],
    )?;
    if inserted == 1 {
        tracing::info!(
            grant_id = %id,
            kind = %plan.kind,
            scope = %plan.scope,
            tool = %operation.tool_name,
            analyzer = %operation.analyzer_id,
            "Permission grant created"
        );
        return Ok(Some(id));
    }

    let existing = exact_identity(
        conn,
        &plan,
        &identity,
        operation,
        operation_hash.as_deref(),
        capability_hash.as_deref(),
    )?;
    if existing.len() != 1 {
        tracing::error!(
            kind = %plan.kind,
            scope = %plan.scope,
            tool = %operation.tool_name,
            matches = existing.len(),
            "Permission grant identity conflict"
        );
        return Err(DbError::Invalid("GRANT_CREATE_IDENTITY_CONFLICT".into()));
    }
    let reused = existing.into_iter().next().unwrap_or_default();
    tracing::debug!(
        grant_id = %reused,
        kind = %plan.kind,
        scope = %plan.scope,
        tool = %operation.tool_name,
        "Reused concurrent permission grant"
    );
    Ok(Some(reused))
}

/// 旧源 `supportedScopes`（`PermissionGrantRepository.java:185-197`）：
/// 服务端依据当前操作分析结果允许展示的「记住授权」范围。
#[must_use]
pub fn supported_scopes(operation: &OperationDescriptor) -> Vec<PermissionScope> {
    if operation.risk == RiskClass::High {
        return Vec::new();
    }
    if operation.analyzer_id == "bash-v2"
        || REMOTE_CAPABILITY_ANALYZERS.contains(&operation.analyzer_id.as_str())
    {
        return vec![PermissionScope::Run, PermissionScope::Session];
    }
    if operation.analyzer_id != "file-v1" {
        return Vec::new();
    }
    if capability_constraint(operation).is_none() {
        vec![PermissionScope::Run, PermissionScope::Session]
    } else {
        vec![
            PermissionScope::Run,
            PermissionScope::Session,
            PermissionScope::Workspace,
        ]
    }
}

/// 旧源 `plan`（`PermissionGrantRepository.java:219-251`）。
#[must_use]
pub fn plan(
    operation: &OperationDescriptor,
    requested: Option<PermissionScope>,
) -> Option<GrantPlan> {
    let requested = requested?;
    if requested == PermissionScope::Once || operation.risk == RiskClass::High {
        return None;
    }
    let tool_wide_remote = operation.analyzer_id == "bash-v2"
        || REMOTE_CAPABILITY_ANALYZERS.contains(&operation.analyzer_id.as_str());
    if tool_wide_remote {
        if requested == PermissionScope::Workspace {
            return None;
        }
        if operation.risk != RiskClass::Guarded && operation.risk != RiskClass::Safe {
            return None;
        }
        return Some(GrantPlan {
            kind: GrantKind::ToolGuarded,
            scope: requested,
            delegation: if requested == PermissionScope::Run {
                DelegationPolicy::DirectOnly
            } else {
                DelegationPolicy::RootAndDescendants
            },
            constraint: GrantConstraint::ToolWide,
        });
    }
    if operation.analyzer_id == "file-v1" {
        if requested == PermissionScope::Run {
            return Some(GrantPlan {
                kind: GrantKind::ExactGuarded,
                scope: requested,
                delegation: DelegationPolicy::DirectOnly,
                constraint: GrantConstraint::Exact {
                    operation_hash: operation.operation_hash.clone(),
                },
            });
        }
        let Some(constraint) = capability_constraint(operation) else {
            if requested != PermissionScope::Session {
                return None;
            }
            return Some(GrantPlan {
                kind: GrantKind::ExactGuarded,
                scope: requested,
                delegation: DelegationPolicy::RootAndDescendants,
                constraint: GrantConstraint::Exact {
                    operation_hash: operation.operation_hash.clone(),
                },
            });
        };
        let write = matches!(constraint, GrantConstraint::WorkspaceEdit { .. });
        return Some(GrantPlan {
            kind: if write {
                GrantKind::EditCapability
            } else {
                GrantKind::ReadCapability
            },
            scope: requested,
            delegation: DelegationPolicy::RootAndDescendants,
            constraint,
        });
    }
    None
}

/// 旧源 `capabilityConstraint`（`PermissionGrantRepository.java:253-276`）。
#[must_use]
pub fn capability_constraint(operation: &OperationDescriptor) -> Option<GrantConstraint> {
    if operation.resources.is_empty()
        || operation
            .resources
            .iter()
            .any(|resource| resource.outside_workspace)
    {
        return None;
    }
    let file_operation = TypedFileOperation::parse(&operation.action)?;
    let mut directories = Vec::new();
    for resource in &operation.resources {
        directories.push(directory_of(&resource.value, file_operation)?);
    }
    directories.sort_unstable();
    directories.dedup();
    let write = operation.effects.contains(&EffectClass::WriteResource);
    if write {
        GrantConstraint::workspace_edit(&directories, vec![file_operation]).ok()
    } else {
        GrantConstraint::workspace_read(&directories, vec![file_operation]).ok()
    }
}

/// 旧源 `identity`（`PermissionGrantRepository.java:278-286`）。
fn identity(
    subject: &AuthorizationSubject,
    scope: PermissionScope,
    delegation: DelegationPolicy,
) -> Identity {
    match scope {
        PermissionScope::Run => Identity {
            root_run_id: Some(subject.root_run_id.clone()),
            actor_run_id: if delegation == DelegationPolicy::DirectOnly {
                Some(subject.current_run_id.clone())
            } else {
                None
            },
            ..Identity::default()
        },
        PermissionScope::Session => Identity {
            root_session_id: Some(subject.root_session_id.clone()),
            ..Identity::default()
        },
        PermissionScope::Workspace => Identity {
            workspace_key: Some(subject.workspace_key.clone()),
            ..Identity::default()
        },
        // 旧源抛 IllegalArgumentException；本函数只在 plan 排除 ONCE 后调用。
        PermissionScope::Once => Identity::default(),
    }
}

/// 旧源 `rootRunIsTerminal`（`PermissionGrantRepository.java:288-294`）。
///
/// 注意：查不到该 Run 时旧源返回 `true`（`status.isEmpty()`）——失败关闭。
fn root_run_is_terminal(conn: &Connection, run_id: &str) -> Result<bool, DbError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM run_envelopes WHERE id=?1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(match status.as_deref() {
        None | Some("completed" | "failed" | "cancelled" | "aborted") => true,
        Some(_) => false,
    })
}

/// 旧源 `workspaceForSession`（`PermissionGrantRepository.java:296-306`）。
fn workspace_for_session(
    conn: &Connection,
    workspaces: WorkspaceIdentityService,
    session_id: &str,
) -> String {
    let root: Option<String> = conn
        .query_row(
            "SELECT working_dir FROM sessions WHERE id=?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten();
    let Some(root) = root else {
        return String::new();
    };
    match workspaces.resolve(std::path::Path::new(&root)) {
        Ok(identity) => identity.workspace_key,
        Err(invalid) => {
            tracing::warn!(
                session_id = %session_id,
                code = %invalid.code,
                "Unable to resolve workspace for permission grant lookup"
            );
            String::new()
        }
    }
}

/// 旧源 `exactIdentity`（`PermissionGrantRepository.java:308-327`）。
fn exact_identity(
    conn: &Connection,
    plan: &GrantPlan,
    identity: &Identity,
    operation: &OperationDescriptor,
    operation_hash: Option<&str>,
    capability_hash: Option<&str>,
) -> Result<Vec<String>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT grant_id FROM permission_grants \
         WHERE grant_kind=?1 AND scope=?2 AND delegation_policy=?3 \
           AND COALESCE(root_session_id,'')=COALESCE(?4,'') \
           AND COALESCE(root_run_id,'')=COALESCE(?5,'') \
           AND COALESCE(actor_run_id,'')=COALESCE(?6,'') \
           AND COALESCE(workspace_key,'')=COALESCE(?7,'') \
           AND authorization_schema_version=?8 AND analyzer_id=?9 \
           AND tool_name=?10 AND action=?11 \
           AND COALESCE(operation_hash,'')=COALESCE(?12,'') \
           AND COALESCE(capability_hash,'')=COALESCE(?13,'') AND revoked_at IS NULL",
    )?;
    let rows = stmt
        .query_map(
            params![
                plan.kind.as_str(),
                plan.scope.as_str(),
                plan.delegation.as_str(),
                identity.root_session_id,
                identity.root_run_id,
                identity.actor_run_id,
                identity.workspace_key,
                operation.authorization_schema_version,
                operation.analyzer_id,
                operation.tool_name,
                operation.action,
                operation_hash,
                capability_hash,
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// 旧源 `writeConstraint`（`PermissionGrantRepository.java:329-350`）：
/// `LinkedHashMap` 插入序 `type` → 载荷字段。
#[must_use]
pub fn encode_constraint(constraint: &GrantConstraint) -> String {
    let mut encoded = Map::new();
    match constraint {
        GrantConstraint::Exact { operation_hash } => {
            encoded.insert("type".into(), "EXACT".into());
            encoded.insert("operationHash".into(), operation_hash.clone().into());
        }
        GrantConstraint::ToolWide => {
            encoded.insert("type".into(), "TOOL_WIDE".into());
        }
        GrantConstraint::WorkspaceRead {
            relative_directory_prefixes,
            allowed_operations,
        }
        | GrantConstraint::WorkspaceEdit {
            relative_directory_prefixes,
            allowed_operations,
        } => {
            encoded.insert("type".into(), constraint.kind_hint().into());
            encoded.insert(
                "relativeDirectoryPrefixes".into(),
                Value::Array(
                    relative_directory_prefixes
                        .iter()
                        .map(|prefix| Value::String(prefix.clone()))
                        .collect(),
                ),
            );
            encoded.insert(
                "allowedOperations".into(),
                Value::Array(
                    allowed_operations
                        .iter()
                        .map(|op| Value::String(op.as_str().to_owned()))
                        .collect(),
                ),
            );
        }
    }
    serde_json::to_string(&Value::Object(encoded)).unwrap_or_else(|_| "{}".into())
}

/// 旧源 `decodeConstraint`（`PermissionGrantRepository.java:352-377`）。
///
/// 持久化约束损坏时必须忽略该授权，绝不能隐式放行 → 返回 `None`。
#[must_use]
pub fn decode_constraint(
    grant_id: &str,
    kind: GrantKind,
    encoded: &str,
) -> Option<GrantConstraint> {
    let ignore = |reason: &str| {
        tracing::warn!(
            grant_id = %grant_id,
            kind = %kind,
            reason = %reason,
            "Ignoring invalid permission grant constraint"
        );
        None::<GrantConstraint>
    };
    let Ok(Value::Object(value)) = serde_json::from_str::<Value>(encoded) else {
        return ignore("not an object");
    };
    if kind == GrantKind::ExactGuarded {
        // 旧源 String.valueOf(null) → "null"，此处等价保留（永不匹配任何真实 hash）。
        let hash = value
            .get("operationHash")
            .map_or_else(|| "null".to_owned(), value_to_java_string);
        return Some(GrantConstraint::Exact {
            operation_hash: hash,
        });
    }
    if kind == GrantKind::ToolGuarded {
        return Some(GrantConstraint::ToolWide);
    }
    let prefixes: Vec<String> = match value.get("relativeDirectoryPrefixes") {
        None => Vec::new(),
        Some(Value::Array(items)) => items.iter().map(value_to_java_string).collect(),
        Some(_) => return ignore("relativeDirectoryPrefixes is not an array"),
    };
    let mut operations = Vec::new();
    match value.get("allowedOperations") {
        None => {}
        Some(Value::Array(items)) => {
            for item in items {
                match TypedFileOperation::parse(&value_to_java_string(item)) {
                    Some(op) => operations.push(op),
                    None => return ignore("unknown TypedFileOperation"),
                }
            }
        }
        Some(_) => return ignore("allowedOperations is not an array"),
    }
    let built = if kind == GrantKind::ReadCapability {
        GrantConstraint::workspace_read(&prefixes, operations)
    } else {
        GrantConstraint::workspace_edit(&prefixes, operations)
    };
    match built {
        Ok(constraint) => Some(constraint),
        Err(_) => ignore("constraint rejected by its own invariants"),
    }
}

/// Jackson `String.valueOf(Object)` 的等价：字符串取原值，其余走 JSON 文本。
fn value_to_java_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "null".to_owned(),
        other => other.to_string(),
    }
}

/// 旧源 `directoryOf`（`PermissionGrantRepository.java:385-395`）。
#[must_use]
pub fn directory_of(path: &str, operation: TypedFileOperation) -> Option<String> {
    let normalized = constraint::normalize_relative_path(path).ok()?;
    if normalized == "." {
        return if operation == TypedFileOperation::ListDirectory {
            Some(".".to_owned())
        } else {
            None
        };
    }
    // 目录浏览授权以用户实际看到的目录为边界，不能取父目录，
    // 否则会把 src/main 静默扩大到 src。
    if operation == TypedFileOperation::ListDirectory {
        return Some(normalized);
    }
    // 根目录文件属于工作区根目录能力；这是单文件授权扩展到同目录的预期语义。
    match normalized.rfind('/') {
        None => Some(".".to_owned()),
        Some(slash) => Some(normalized[..slash].to_owned()),
    }
}
