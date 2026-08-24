//! 授权链集成测试脚手架——旧 Spring 测试里手搭协作者（mock/stub）的等价物。
//!
//! 旧测试用 `Mockito` + 真 `JdbcTemplate`；Rust 侧用内存 `Db` + 本文件的
//! 手写 fake 端口。端口默认实现（`DefaultModeProvider` / `NoopRunEventSink` /
//! `UntrustedWorkspaceProbe` / `StatelessShellState` / `PassthroughFilter`）
//! 由 zk-authz 自带，本文件只补「可编程」的三个：模式、交互网关、工具事实。

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use zk_authz::analyzer::OperationAnalyzerRegistry;
use zk_authz::frozen::{FrozenToolInput, FrozenToolInputFactory};
use zk_authz::gateway::ToolExecutionGateway;
use zk_authz::grants::GrantStore;
use zk_authz::interaction::{
    AuthorizationInteractionSpec, BoxFuture, InteractionGateway, InteractionRecord,
    InteractionStatus, InteractionType,
};
use zk_authz::model::{
    AuthorizationSubject, AuthzError, AuthzResult, OperationDescriptor, PermissionMode,
    PermissionScope,
};
use zk_authz::path_security::{PathSecurityService, SystemScratchpadPathPolicy};
use zk_authz::service::AuthorizationService;
use zk_authz::subject::AuthorizationSubjectResolver;
use zk_authz::tool_facts::{
    ArtifactPublicationPort, BashParseOutcome, BashSecurityPort, ModeProvider, PassthroughFilter,
    PublicationSnapshot, RunEventSink, StatelessShellState, ToolFacts, ToolUseContext,
    WorkspaceTrustProbe,
};
use zk_db::Db;

/// 可编程会话模式（旧 `PermissionModeManager` 的测试替身）。
#[derive(Debug)]
pub struct FakeModes {
    mode: Mutex<PermissionMode>,
}

impl Default for FakeModes {
    /// 缺省 `DEFAULT` 模式（旧 `PermissionModeManager` 未设置时的回落）。
    fn default() -> Self {
        Self::new(PermissionMode::Default)
    }
}

impl FakeModes {
    pub fn new(mode: PermissionMode) -> Self {
        Self {
            mode: Mutex::new(mode),
        }
    }

    pub fn set(&self, mode: PermissionMode) {
        *self.mode.lock().expect("modes lock") = mode;
    }
}

impl ModeProvider for FakeModes {
    fn mode(&self, _root_session_id: &str) -> PermissionMode {
        *self.mode.lock().expect("modes lock")
    }
}

/// 可编程工作区信任探针（旧 `ProjectWorkspaceService` 的测试替身）。
#[derive(Debug, Default)]
pub struct FakeWorkspaceTrust {
    trusted: Mutex<Vec<PathBuf>>,
    /// 旧 `when(projects.isTrustedFileScope(root)).thenReturn(a, b, ...)` 的连续答案。
    ///
    /// 只对脚本指定的 root 生效，用尽后重复末位答案（Mockito 语义）。
    script: Mutex<Option<(PathBuf, std::collections::VecDeque<bool>)>>,
    /// 全部探测调用（旧 `verify(projects, never()).isTrustedFileScope(any())` 的观测点）。
    calls: Mutex<Vec<PathBuf>>,
}

impl FakeWorkspaceTrust {
    pub fn trust(&self, root: PathBuf) {
        self.trusted.lock().expect("trust lock").push(root);
    }

    /// 脚本化某 root 的连续答案（旧 `thenReturn(true, false)`）。
    pub fn script(&self, root: PathBuf, answers: &[bool]) {
        *self.script.lock().expect("trust script lock") =
            Some((root, answers.iter().copied().collect()));
    }

    /// 信任探测被调用的次数。
    #[must_use]
    pub fn probe_count(&self) -> usize {
        self.calls.lock().expect("trust calls lock").len()
    }
}

impl WorkspaceTrustProbe for FakeWorkspaceTrust {
    fn is_trusted_file_scope(&self, authorization_root: &std::path::Path) -> bool {
        self.calls
            .lock()
            .expect("trust calls lock")
            .push(authorization_root.to_path_buf());
        if let Some((root, answers)) = self.script.lock().expect("trust script lock").as_mut()
            && root == authorization_root
        {
            return if answers.len() > 1 {
                answers.pop_front().unwrap_or(false)
            } else {
                answers.front().copied().unwrap_or(false)
            };
        }
        self.trusted
            .lock()
            .expect("trust lock")
            .iter()
            .any(|root| root == authorization_root)
    }

    fn is_trusted_file_scope_in_current_write(
        &self,
        _conn: &rusqlite::Connection,
        authorization_root: &std::path::Path,
    ) -> bool {
        self.is_trusted_file_scope(authorization_root)
    }
}

/// 可编程 Bash 安全解析端口（旧 `BashSecurityAnalyzer` 的测试替身）。
///
/// 真实实现在 zk-tools（依赖方向铁律：zk-authz 不得依赖 zk-tools），故 zk-authz
/// 自己的测试只能用替身注入判定结局——`BlacklistDeny` 即 `ABSOLUTE_DENY` 入口。
#[derive(Debug)]
pub struct FakeBashSecurity {
    outcome: Mutex<BashParseOutcome>,
    inherited: Mutex<Vec<String>>,
}

impl Default for FakeBashSecurity {
    fn default() -> Self {
        Self {
            outcome: Mutex::new(BashParseOutcome::Parsed),
            inherited: Mutex::new(Vec::new()),
        }
    }
}

impl FakeBashSecurity {
    /// 脚本化 `parseForSecurity` 结局。
    pub fn set(&self, outcome: BashParseOutcome) {
        *self.outcome.lock().expect("bash outcome lock") = outcome;
    }

    /// 脚本化 `analyzeEnvironmentReferences(...).inheritedReferences()`。
    pub fn inherit(&self, names: &[&str]) {
        *self.inherited.lock().expect("bash inherit lock") =
            names.iter().map(|name| (*name).to_owned()).collect();
    }
}

impl BashSecurityPort for FakeBashSecurity {
    fn parse_for_security(
        &self,
        _command: &str,
        _cwd: &std::path::Path,
        _authorization_root: &std::path::Path,
    ) -> BashParseOutcome {
        self.outcome.lock().expect("bash outcome lock").clone()
    }

    fn inherited_environment_references(&self, _command: &str) -> Vec<String> {
        self.inherited.lock().expect("bash inherit lock").clone()
    }
}

/// 可编程产物发布策略（旧 `mock(ArtifactPublicationPolicy.class)`）。
///
/// 按脚本顺序返回快照；用尽后重复最后一个（旧 Mockito
/// `thenReturn(a, b)` 的语义同此）。
#[derive(Debug, Default)]
pub struct FakePublicationPolicy {
    snapshots: Mutex<std::collections::VecDeque<PublicationSnapshot>>,
    last: Mutex<Option<PublicationSnapshot>>,
}

impl FakePublicationPolicy {
    /// 依次排入快照。
    pub fn script(&self, snapshots: Vec<PublicationSnapshot>) {
        *self.snapshots.lock().expect("snapshots lock") = snapshots.into();
    }
}

impl ArtifactPublicationPort for FakePublicationPolicy {
    fn inspect(
        &self,
        _file_path: &str,
        _current_run_id: Option<&str>,
    ) -> Result<PublicationSnapshot, (String, String)> {
        let next = self.snapshots.lock().expect("snapshots lock").pop_front();
        let mut last = self.last.lock().expect("last lock");
        if let Some(snapshot) = next {
            *last = Some(snapshot.clone());
            return Ok(snapshot);
        }
        last.clone().ok_or_else(|| {
            (
                "ARTIFACT_PUBLISH_POLICY_DENIED".to_owned(),
                "no scripted snapshot".to_owned(),
            )
        })
    }
}

/// 记录全部 Run 事件的 sink（旧测试 `ArgumentCaptor<Map<String,Object>>` 的等价物）。
#[derive(Debug, Default)]
pub struct RecordingRunEvents {
    events: Mutex<Vec<RunEventRecord>>,
}

/// 一条被记录的 Run 事件。
#[derive(Debug, Clone)]
pub struct RunEventRecord {
    pub run_id: String,
    pub event_type: String,
    pub tool_use_id: Option<String>,
    pub payload: Value,
}

impl RecordingRunEvents {
    /// 取指定类型的全部事件（旧 `verify(runs).appendEventInCurrentWrite(...)`）。
    pub fn of_type(&self, event_type: &str) -> Vec<RunEventRecord> {
        self.events
            .lock()
            .expect("events lock")
            .iter()
            .filter(|event| event.event_type == event_type)
            .cloned()
            .collect()
    }

    fn record(&self, run_id: &str, event_type: &str, tool_use_id: Option<&str>, payload: &Value) {
        self.events
            .lock()
            .expect("events lock")
            .push(RunEventRecord {
                run_id: run_id.to_owned(),
                event_type: event_type.to_owned(),
                tool_use_id: tool_use_id.map(str::to_owned),
                payload: payload.clone(),
            });
    }
}

impl RunEventSink for RecordingRunEvents {
    fn append_event(
        &self,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &Value,
    ) {
        self.record(run_id, event_type, tool_use_id, payload);
    }

    fn append_event_in_current_write(
        &self,
        _conn: &rusqlite::Connection,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &Value,
    ) {
        self.record(run_id, event_type, tool_use_id, payload);
    }
}

/// 可编程工具事实（旧测试里的 `Tool` stub）。
#[derive(Debug, Clone)]
pub struct FakeTool {
    pub name: String,
    pub destructive: bool,
    pub read_only: bool,
    pub path_field: Option<String>,
    pub mcp_server: Option<String>,
}

impl FakeTool {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            destructive: false,
            read_only: false,
            path_field: None,
            mcp_server: None,
        }
    }

    pub fn destructive(mut self) -> Self {
        self.destructive = true;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// 声明 `getPath(input)` 从哪个入参键取路径（旧 `FileTool` 家族语义）。
    pub fn path_from(mut self, field: &str) -> Self {
        self.path_field = Some(field.to_owned());
        self
    }

    pub fn mcp(mut self, server: &str) -> Self {
        self.mcp_server = Some(server.to_owned());
        self
    }
}

impl ToolFacts for FakeTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_mcp(&self) -> bool {
        self.mcp_server.is_some()
    }

    fn mcp_server(&self) -> Option<&str> {
        self.mcp_server.as_deref()
    }

    fn is_destructive(&self, _input: &Value) -> bool {
        self.destructive
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        self.read_only
    }

    fn path_of(&self, input: &Value) -> Option<String> {
        let field = self.path_field.as_deref()?;
        input.get(field).and_then(Value::as_str).map(str::to_owned)
    }
}

/// 交互网关测试替身：记录 spec、按脚本返回终态。
///
/// 语义与 `zk-server::interaction::DurableInteractionService` 对齐的三点：
/// 1. `create_authorization` 落 `PENDING` 行并返回；
/// 2. `find_by_correlation_key` 按 `(run_id, correlation_key)` 查已存在行；
/// 3. `await_terminal` 返回脚本终态，`find_by_id` 返回改写为该终态的行。
pub struct FakeGateway {
    db: Db,
    records: Mutex<Vec<InteractionRecord>>,
    script: Mutex<Script>,
    once_result: Mutex<Option<AuthzError>>,
    pub created: Mutex<Vec<AuthorizationInteractionSpec>>,
}

/// 脚本化的「用户回答」。
///
/// `terminal` 缺省 `Pending` 即「没人回答」；`response` 是写入 `response_json`
/// 的决策载荷；`remember` 存在时，`await_terminal` 会在返回终态前把授权记录
/// 落库——复刻真实 `DurableInteractionService` 在写决策的**同一事务**内创建
/// grant 的时序（`AuthorizationService::interact` 随后 `find_match` 必须命中，
/// 否则回 `PERMISSION_GRANT_NOT_COMMITTED`）。
#[derive(Debug)]
struct Script {
    terminal: InteractionStatus,
    response: Option<Value>,
    remember: Option<RememberPlan>,
}

/// 记住决策时要落的授权记录。
#[derive(Debug, Clone)]
struct RememberPlan {
    grants: GrantStore,
    subject: AuthorizationSubject,
    operation: OperationDescriptor,
    scope: PermissionScope,
}

impl std::fmt::Debug for FakeGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeGateway")
            .field("prompts", &self.prompt_count())
            .finish_non_exhaustive()
    }
}

impl FakeGateway {
    /// 绑定库句柄：`create_authorization` 真落 `interaction_requests` 行，
    /// 使 `permission_grants.interaction_request_id` 外键有真实目标（旧
    /// `DurableInteractionService.createAuthorization` 同样先落库）。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            db,
            records: Mutex::new(Vec::new()),
            script: Mutex::new(Script {
                terminal: InteractionStatus::Pending,
                response: None,
                remember: None,
            }),
            once_result: Mutex::new(None),
            created: Mutex::new(Vec::new()),
        }
    }

    /// 脚本化用户决策终态（默认 `PENDING`——即「没人回答」）。
    pub fn answer(&self, status: InteractionStatus) {
        self.script.lock().expect("script lock").terminal = status;
    }

    /// 「允许，仅本次」：`remember=false` / `scope=once`，不写授权记录。
    pub fn allow_once(&self, operation_hash: &str) {
        let mut script = self.script.lock().expect("script lock");
        script.terminal = InteractionStatus::Answered;
        script.response = Some(json!({
            "operationHash": operation_hash,
            "decision": "allow",
            "remember": false,
            "scope": "once",
        }));
        script.remember = None;
    }

    /// 「允许并记住」：`remember=true` + 指定 scope，终态前把 grant 落库。
    pub fn allow_remember(
        &self,
        grants: &GrantStore,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        scope: PermissionScope,
    ) {
        let mut script = self.script.lock().expect("script lock");
        script.terminal = InteractionStatus::Answered;
        script.response = Some(json!({
            "operationHash": operation.operation_hash,
            "decision": "allow",
            "remember": true,
            "scope": scope.lowercase(),
        }));
        script.remember = Some(RememberPlan {
            grants: grants.clone(),
            subject: subject.clone(),
            operation: operation.clone(),
            scope,
        });
    }

    /// 「拒绝」：终态 `DENIED`（`AuthorizationService` 回 `PERMISSION_USER_DENIED`）。
    pub fn deny(&self) {
        let mut script = self.script.lock().expect("script lock");
        script.terminal = InteractionStatus::Denied;
        script.response = None;
        script.remember = None;
    }

    /// 脚本化 `require_answered_once_in_tx` 的拒绝。
    pub fn reject_once(&self, error: AuthzError) {
        *self.once_result.lock().expect("once lock") = Some(error);
    }

    /// 已创建交互次数（弹窗次数——判定矩阵「是否弹窗」的观测点）。
    pub fn prompt_count(&self) -> usize {
        self.created.lock().expect("created lock").len()
    }

    fn stored(&self, interaction_id: &str) -> Option<InteractionRecord> {
        self.records
            .lock()
            .expect("records lock")
            .iter()
            .find(|record| record.interaction_id == interaction_id)
            .cloned()
    }
}

impl InteractionGateway for FakeGateway {
    fn find_by_correlation_key<'a>(
        &'a self,
        run_id: Option<&'a str>,
        correlation_key: &'a str,
    ) -> BoxFuture<'a, AuthzResult<Option<InteractionRecord>>> {
        Box::pin(async move {
            let found = self
                .records
                .lock()
                .expect("records lock")
                .iter()
                .find(|record| {
                    record.correlation_key == correlation_key
                        && run_id.is_some_and(|run| run == record.run_id)
                })
                .cloned();
            Ok(found)
        })
    }

    fn create_authorization(
        &self,
        spec: AuthorizationInteractionSpec,
    ) -> BoxFuture<'_, AuthzResult<InteractionRecord>> {
        Box::pin(async move {
            let interaction_id = format!("interaction-{}", self.prompt_count() + 1);
            let record = pending_record(&interaction_id, &spec);
            insert_interaction(&self.db, &record).await?;
            self.records
                .lock()
                .expect("records lock")
                .push(record.clone());
            self.created.lock().expect("created lock").push(spec);
            Ok(record)
        })
    }

    fn await_terminal<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> BoxFuture<'a, AuthzResult<InteractionStatus>> {
        Box::pin(async move {
            // 先取出脚本再释放锁：`std::sync::MutexGuard` 不能跨 `.await`
            //（`BoxFuture` 要求 `Send`）。
            let (terminal, response, remember) = {
                let script = self.script.lock().expect("script lock");
                (
                    script.terminal,
                    script.response.clone(),
                    script.remember.clone(),
                )
            };
            // 记住决策时先落 grant，再回终态——与真实实现「同事务内创建」等时序。
            if let Some(plan) = remember {
                plan.grants
                    .create(
                        &plan.subject,
                        &plan.operation,
                        Some(plan.scope),
                        Some(interaction_id.to_owned()),
                    )
                    .await?;
            }
            let response_json =
                response.map(|body| serde_json::to_string(&body).expect("response json"));
            {
                let mut records = self.records.lock().expect("records lock");
                if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.interaction_id == interaction_id)
                {
                    record.status = terminal;
                    record.response_json.clone_from(&response_json);
                    record.version += 1;
                }
            }
            update_interaction(&self.db, interaction_id, terminal, response_json).await?;
            Ok(terminal)
        })
    }

    fn find_by_id<'a>(
        &'a self,
        interaction_id: &'a str,
    ) -> BoxFuture<'a, AuthzResult<Option<InteractionRecord>>> {
        Box::pin(async move { Ok(self.stored(interaction_id)) })
    }

    fn require_answered_once_in_tx(
        &self,
        _conn: &rusqlite::Connection,
        _interaction_id: Option<&str>,
        _subject: &AuthorizationSubject,
        _descriptor: &OperationDescriptor,
        _tool_use_id: &str,
    ) -> AuthzResult<()> {
        match self.once_result.lock().expect("once lock").clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// 把交互行写进 `interaction_requests`（26 列全量，与 DDL 同序）。
async fn insert_interaction(db: &Db, record: &InteractionRecord) -> AuthzResult<()> {
    let record = record.clone();
    db.with_writer(move |conn| {
        conn.execute(
            "INSERT INTO interaction_requests(\
               interaction_id,correlation_key,session_id,run_id,type,status,prompt_json,\
               allowed_decisions_json,scope_options_json,response_json,created_at,\
               delivery_window_ends_at,source,delivery_generation,dispatch_attempts,\
               authorization_context_json,updated_at,version) \
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            rusqlite::params![
                record.interaction_id,
                record.correlation_key,
                record.session_id,
                record.run_id,
                record.kind.db(),
                record.status.db(),
                record.prompt_json,
                record.allowed_decisions_json,
                record.scope_options_json,
                record.response_json,
                record.created_at,
                record.delivery_window_ends_at,
                record.source,
                record.delivery_generation,
                record.dispatch_attempts,
                record.authorization_context_json,
                record.updated_at,
                record.version,
            ],
        )?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// 写入终态与决策载荷（旧 `DurableInteractionService.decideRequest` 的 CAS 写）。
async fn update_interaction(
    db: &Db,
    interaction_id: &str,
    status: InteractionStatus,
    response_json: Option<String>,
) -> AuthzResult<()> {
    let interaction_id = interaction_id.to_owned();
    db.with_writer(move |conn| {
        conn.execute(
            "UPDATE interaction_requests \
             SET status=?2, response_json=?3, version=version+1 \
             WHERE interaction_id=?1",
            rusqlite::params![interaction_id, status.db(), response_json],
        )?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// 构造 `PENDING` 交互行（字段语义与 `interaction_requests` 列一致）。
fn pending_record(interaction_id: &str, spec: &AuthorizationInteractionSpec) -> InteractionRecord {
    let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
    InteractionRecord {
        interaction_id: interaction_id.to_owned(),
        correlation_key: spec.correlation_key.clone(),
        session_id: spec.root_session_id.clone(),
        run_id: spec.run_id.clone().unwrap_or_default(),
        kind: InteractionType::Permission,
        status: InteractionStatus::Pending,
        prompt_json: serde_json::to_string(&spec.prompt).expect("prompt json"),
        allowed_decisions_json: serde_json::to_string(&spec.allowed_decisions).expect("decisions"),
        scope_options_json: serde_json::to_string(&spec.scope_options).expect("scopes"),
        response_json: None,
        created_at: now.clone(),
        delivery_window_ends_at: now.clone(),
        first_dispatched_at: None,
        delivery_ack_deadline_at: None,
        received_at: None,
        decision_deadline_at: None,
        decided_at: None,
        terminal_reason: None,
        source: spec.source.clone(),
        child_session_id: None,
        delivery_generation: 0,
        dispatch_attempts: 0,
        last_transport_id: None,
        authorization_context_json: Some(
            serde_json::to_string(&spec.authorization_context).expect("context json"),
        ),
        updated_at: now,
        version: 0,
    }
}

/// 组装好的授权链 + 其可编程协作者。
pub struct Harness {
    pub db: Db,
    pub service: Arc<AuthorizationService>,
    pub events: Arc<RecordingRunEvents>,
    pub gateway: Arc<FakeGateway>,
    pub modes: Arc<FakeModes>,
    pub trust: Arc<FakeWorkspaceTrust>,
    pub bash: Arc<FakeBashSecurity>,
    pub grants: GrantStore,
    pub frozen: FrozenToolInputFactory,
    pub workspace: PathBuf,
    pub scratchpad_root: PathBuf,
    _temp: TempRoot,
}

/// 自清理临时根目录。
///
/// 不引入 `tempfile` 依赖——仓库既有测试一律用
/// `std::env::temp_dir().join(uuid)`（见 `zk-db/src/project.rs:416`、
/// `zk-server/src/workspace.rs:696`），本类型只补一个 `Drop` 清理。
#[derive(Debug)]
pub struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    /// 以标签建一个唯一临时根（`zk-authz-{tag}-{uuid}`）。
    #[must_use]
    pub fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("zk-authz-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temp root");
        Self {
            path: path.canonicalize().expect("canonical temp root"),
        }
    }

    /// 规范化后的根路径。
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Harness {
    /// 建库 + 建 workspace 目录 + 建 scratchpad 根 + 装配服务。
    pub fn new() -> Self {
        Self::with_scratchpad("scratchpads")
    }

    /// 同 [`Self::new`]，但把系统 scratchpad 根放到临时根下的指定相对路径。
    ///
    /// 旧 `AuthorizationServiceSystemScratchpadScopeTest` 的
    /// `SystemScratchpadPathPolicy(scratchpad)` 每个 fixture 都可换根
    ///（`state/.zk/scratchpad` 与 `project/.zk/scratchpad` 两种布局）。
    pub fn with_scratchpad(scratchpad_relative: &str) -> Self {
        let temp = TempRoot::new("harness");
        let workspace = temp.path().join("workspace");
        let scratchpad_root = temp.path().join(scratchpad_relative);
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        std::fs::create_dir_all(&scratchpad_root).expect("scratchpad dir");
        let db = Db::open_in_memory().expect("memory db");
        let scratchpads = SystemScratchpadPathPolicy::new(&scratchpad_root);
        let bash = Arc::new(FakeBashSecurity::default());
        let analyzers = Arc::new(OperationAnalyzerRegistry::new(
            Some(bash.clone()),
            Arc::new(PassthroughFilter),
            Arc::new(PathSecurityService::new(scratchpads.clone())),
            Arc::new(StatelessShellState),
        ));
        let gateway = Arc::new(FakeGateway::new(db.clone()));
        let modes = Arc::new(FakeModes::new(PermissionMode::Default));
        let trust = Arc::new(FakeWorkspaceTrust::default());
        let events = Arc::new(RecordingRunEvents::default());
        let service = Arc::new(AuthorizationService::new(
            AuthorizationSubjectResolver::new(db.clone()),
            analyzers,
            GrantStore::new(db.clone()),
            gateway.clone(),
            modes.clone(),
            events.clone(),
            trust.clone(),
            scratchpads,
        ));
        Self {
            db: db.clone(),
            service,
            events,
            gateway,
            modes,
            trust,
            bash,
            grants: GrantStore::new(db),
            frozen: FrozenToolInputFactory::default(),
            workspace,
            scratchpad_root,
            _temp: temp,
        }
    }

    /// 建一条会话 + 一条 running run（授权主体解析的最小前置）。
    pub async fn seed_run(&self, session_id: &str, run_id: &str) {
        let workspace = self.workspace.to_string_lossy().to_string();
        let (session_id, run_id) = (session_id.to_owned(), run_id.to_owned());
        self.db
            .with_writer(move |conn| {
                let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
                conn.execute(
                    "INSERT INTO sessions(id,model,working_dir,created_at,updated_at) \
                     VALUES(?1,'test-model',?2,?3,?3)",
                    rusqlite::params![session_id, workspace, now],
                )?;
                conn.execute(
                    "INSERT INTO run_envelopes(id,session_id,parent_run_id,status,model,\
                       started_at,created_at,updated_at) \
                     VALUES(?1,?2,NULL,'running','test-model',?3,?3,?3)",
                    rusqlite::params![run_id, session_id, now],
                )?;
                Ok(())
            })
            .await
            .expect("seed run");
    }

    /// 组装唯一执行准入网关（旧 `new ToolExecutionGateway(authorization, runs)`）。
    #[must_use]
    pub fn execution_gateway(&self) -> ToolExecutionGateway {
        ToolExecutionGateway::new(self.service.clone(), self.events.clone(), self.db.clone())
    }

    /// 冻结入参（旧测试的 `factory.freeze(tool, ToolInput.from(map))`）。
    pub fn freeze(&self, tool: &str, input: &Value) -> FrozenToolInput {
        self.frozen.freeze(tool, input).expect("freeze input")
    }

    /// 组装 `ToolUseContext`（run / toolUse / rootSession / session / cwd）。
    pub fn context(&self, run_id: &str, tool_use_id: &str, session_id: &str) -> ToolUseContext {
        ToolUseContext::new(
            Some(run_id.to_owned()),
            Some(tool_use_id.to_owned()),
            Some(session_id.to_owned()),
        )
        .with_shell(
            Some(session_id.to_owned()),
            Some(self.workspace.to_string_lossy().to_string()),
        )
    }
}

/// 便捷断言：`Map<String, Value>` 取字符串。
pub fn text(map: &Map<String, Value>, key: &str) -> String {
    map.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// 便捷构造：单键入参。
pub fn input(key: &str, value: &str) -> Value {
    json!({ key: value })
}
