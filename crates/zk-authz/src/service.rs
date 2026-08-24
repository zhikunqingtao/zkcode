//! `ToolExecutionPipeline` 使用的唯一策略、授权记录和交互裁决权威。
//!
//! 逐字移植 `authorization/AuthorizationService.java`（500 行）。
//!
//! # 决策链（旧源 `authorizePrepared` L86-213）
//!
//! 顺序即语义，任何插入/重排都会改变可达性，故 14 步逐格保留：
//!
//! | # | 条件 | 结果 |
//! |---|---|---|
//! | 0 | `bindExecutionInput` 非空 | 覆盖执行输入（不是裁决） |
//! | 1 | `effects == [SAFE_INTERNAL]` | ALLOW `POLICY/BUILTIN_SAFE` |
//! | 2 | `mode == AUTO_APPROVE` | ALLOW `MODE/AUTO_APPROVE` |
//! | 3 | `risk == HIGH` | `PLAN`→DENY / `DONT_ASK`→DENY / 否则 `interact()`，**跳过全部 grant 匹配** |
//! | 4 | `grants.findMatch` 命中 | ALLOW `GRANT/GRANT_MATCH` |
//! | 5 | `[READ_RESOURCE]` 且系统 scratchpad | ALLOW `POLICY/SYSTEM_SCRATCHPAD_SCOPE` |
//! | 6 | `[READ_RESOURCE]` 且 `/private/tmp` | ALLOW `POLICY/PRIVATE_TMP_FILE_SCOPE` |
//! | 7 | `file-v1` + `SAFE` + `[READ_RESOURCE]` | ALLOW `MODE/SAFE_READ_AUTO` |
//! | 8 | `mode == PLAN` | DENY `PLAN_MODE_EFFECT_DENIED` |
//! | 9 | 非 scratchpadRead 但落在 scratchpad | ALLOW `POLICY/SYSTEM_SCRATCHPAD_SCOPE` |
//! | 10 | 非 privateTmpRead 但落在 `/private/tmp` | ALLOW `POLICY/PRIVATE_TMP_FILE_SCOPE` |
//! | 11 | `isTrustedProjectFileWrite` | ALLOW `POLICY/PROJECT_FILE_SCOPE` |
//! | 12 | `mode == DONT_ASK` | DENY `PERMISSION_INTERACTION_REQUIRED` |
//! | 13 | `ACCEPT_EDITS` + `file-v1` + 非 HIGH + 无 outsideWorkspace + `[WRITE_RESOURCE]` | ALLOW `MODE/ACCEPT_EDITS` |
//! | 14 | 兜底 | `interact()` |
//!
//! # 硬安全不变量
//!
//! - **`ABSOLUTE_DENY` 不可绕过**：`COMMAND_ABSOLUTELY_DENIED` 由
//!   [`crate::analyzer`] 在 `prepare` 阶段抛出，早于本决策链的第 1 步，也早于
//!   `AUTO_APPROVE`；连 `AUTO_APPROVE` 都无法放行（见 `prepare` 的
//!   `analyzer.analyze` → `AuthorizationException` 直接向上抛）。
//! - **HIGH 风险不可记住、不可命中授权记录**：第 3 步先于第 4 步。
//! - **`PROJECT` / `SCRATCHPAD` / `PRIVATE_TMP` 策略放行在最终复检时重验**（见
//!   [`AuthorizationService::final_grant_recheck_in_current_transaction`]）。

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use serde_json::{Map, Value};

use crate::analyzer::{AnalyzerKind, OperationAnalyzerRegistry};
use crate::diagnostic;
use crate::frozen::FrozenToolInput;
use crate::grants::{self, GrantStore};
use crate::interaction::{
    AuthorizationInteractionContext, AuthorizationInteractionSpec, AuthorizationSubjectData,
    DecisionOption, InteractionGateway, InteractionRecord, InteractionStatus, PROTOCOL_VERSION,
};
use crate::model::{
    AuthorizationSubject, AuthorizedOperation, AuthzError, AuthzResult, DiagnosticOutcome,
    DiagnosticSource, EffectClass, EvaluationStage, OperationDescriptor, PermissionMode,
    PermissionScope, PreparedOperation, RiskClass, TypedFileOperation,
};
use crate::path_security::SystemScratchpadPathPolicy;
use crate::subject::AuthorizationSubjectResolver;
use crate::tool_facts::{
    ModeProvider, RunEventSink, ToolFacts, ToolUseContext, WorkspaceTrustProbe,
};

/// 旧源 `PRIVATE_TMP_ROOT`（`AuthorizationService.java:30-31`）。
static PRIVATE_TMP_ROOT: LazyLock<PathBuf> =
    LazyLock::new(|| crate::workspace::absolute_normalized(Path::new("/private/tmp")));

/// 唯一策略、授权记录与交互裁决权威。
pub struct AuthorizationService {
    subjects: AuthorizationSubjectResolver,
    analyzers: Arc<OperationAnalyzerRegistry>,
    grants: GrantStore,
    interactions: Arc<dyn InteractionGateway>,
    modes: Arc<dyn ModeProvider>,
    runs: Arc<dyn RunEventSink>,
    project_workspaces: Arc<dyn WorkspaceTrustProbe>,
    system_scratchpad_paths: SystemScratchpadPathPolicy,
}

impl std::fmt::Debug for AuthorizationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationService")
            .finish_non_exhaustive()
    }
}

impl AuthorizationService {
    /// 完整构造（旧源 `@Autowired` 构造器 L41-52）。
    ///
    /// 8 个协作者与旧源 `@Autowired` 构造器逐一对应；聚合成 builder 会掩盖「缺哪个
    /// 端口就装不起来」的编译期约束，故保留全参构造。
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subjects: AuthorizationSubjectResolver,
        analyzers: Arc<OperationAnalyzerRegistry>,
        grants: GrantStore,
        interactions: Arc<dyn InteractionGateway>,
        modes: Arc<dyn ModeProvider>,
        runs: Arc<dyn RunEventSink>,
        project_workspaces: Arc<dyn WorkspaceTrustProbe>,
        system_scratchpad_paths: SystemScratchpadPathPolicy,
    ) -> Self {
        Self {
            subjects,
            analyzers,
            grants,
            interactions,
            modes,
            runs,
            project_workspaces,
            system_scratchpad_paths,
        }
    }

    /// 旧源 `authorize(...)`（L63-67）：`prepare` + `authorizePrepared`。
    ///
    /// # Errors
    /// 分析期拒绝、不变量拒绝、模式拒绝、交互拒绝/超时均返回 [`AuthzError`]。
    pub async fn authorize(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: Value,
        context: &ToolUseContext,
    ) -> AuthzResult<AuthorizedOperation> {
        let prepared = self
            .prepare(tool, frozen, &execution_input, context)
            .await?;
        self.authorize_prepared_with_mode(tool, frozen, execution_input, context, prepared, None)
            .await
    }

    /// Authorize with a request-local permission mode override. Reverse MCP uses `DONT_ASK`
    /// because it has no interaction transport; the session-wide mode registry is untouched.
    ///
    /// # Errors
    /// Returns the same analysis, policy, grant, and admission errors as `authorize`.
    pub async fn authorize_with_mode(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: Value,
        context: &ToolUseContext,
        mode: PermissionMode,
    ) -> AuthzResult<AuthorizedOperation> {
        let prepared = self
            .prepare(tool, frozen, &execution_input, context)
            .await?;
        self.authorize_prepared_with_mode(
            tool,
            frozen,
            execution_input,
            context,
            prepared,
            Some(mode),
        )
        .await
    }

    /// 旧源 `prepare(...)`（L69-83）：解析主体 → 分析 → 不变量；任一失败落诊断后上抛。
    ///
    /// # Errors
    /// 主体解析失败（`AUTHORIZATION_ANCESTRY_INVALID`）、分析器拒绝（含
    /// `COMMAND_ABSOLUTELY_DENIED`）、不变量拒绝时返回 [`AuthzError`]。
    pub async fn prepare(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: &Value,
        context: &ToolUseContext,
    ) -> AuthzResult<PreparedOperation> {
        let execution_attempt_id = uuid::Uuid::new_v4().to_string();
        let subject = self
            .subjects
            .resolve(context.current_run_id.as_deref())
            .await?;
        let kind = self.analyzers.analyzer_for(tool);
        let analyzed = self
            .analyzers
            .analyze(kind, tool, frozen, execution_input, context, &subject)
            .and_then(|operation| Self::invariant(&operation).map(|()| operation));
        match analyzed {
            Ok(operation) => Ok(PreparedOperation {
                subject,
                descriptor: operation,
                execution_attempt_id,
            }),
            Err(denied) => {
                self.record_analysis_failure(
                    context,
                    &subject,
                    tool.name(),
                    frozen.input_hash(),
                    &execution_attempt_id,
                    &denied.code,
                );
                Err(denied)
            }
        }
    }

    /// 旧源 `authorizePrepared(...)`（L85-213）：14 步决策链。
    ///
    /// # Errors
    /// 决策链任一 DENY 步（`PLAN_MODE_EFFECT_DENIED` /
    /// `PERMISSION_INTERACTION_REQUIRED` / 用户拒绝 / 交互过期等）返回
    /// [`AuthzError`]。
    #[allow(clippy::too_many_lines)]
    pub async fn authorize_prepared(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: Value,
        context: &ToolUseContext,
        prepared: PreparedOperation,
    ) -> AuthzResult<AuthorizedOperation> {
        self.authorize_prepared_with_mode(tool, frozen, execution_input, context, prepared, None)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn authorize_prepared_with_mode(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: Value,
        context: &ToolUseContext,
        prepared: PreparedOperation,
        mode_override: Option<PermissionMode>,
    ) -> AuthzResult<AuthorizedOperation> {
        let PreparedOperation {
            subject,
            descriptor: operation,
            execution_attempt_id,
        } = prepared;
        let mut execution_input = execution_input;
        if let Some(bound) = OperationAnalyzerRegistry::bind_execution_input(
            tool,
            &operation,
            &execution_input,
            &subject,
        ) {
            execution_input = bound;
        }

        let allow = |source: DiagnosticSource,
                     reason_code: &str,
                     grant_id: Option<String>,
                     grant_scope: Option<PermissionScope>,
                     interaction_id: Option<String>| AuthorizedOperation {
            subject: subject.clone(),
            descriptor: operation.clone(),
            execution_input: execution_input.clone(),
            source,
            reason_code: reason_code.to_owned(),
            grant_id,
            grant_scope,
            interaction_id,
            execution_attempt_id: execution_attempt_id.clone(),
        };

        // 步 1：纯内部安全操作（`SAFE_INTERNAL` 单一副作用）无条件放行。
        if operation.effects == [EffectClass::SafeInternal] {
            return Ok(allow(
                DiagnosticSource::Policy,
                "BUILTIN_SAFE",
                None,
                None,
                None,
            ));
        }
        let mode = mode_override.unwrap_or_else(|| self.modes.mode(&subject.root_session_id));
        // 步 2：AUTO_APPROVE 只替代人工授权。分析、不变量和 Security Hook 已在调用方
        // 完成，工具仍必须经过产物声明、最终动态复检和唯一执行网关。
        if mode == PermissionMode::AutoApprove {
            return Ok(allow(
                DiagnosticSource::Mode,
                "AUTO_APPROVE",
                None,
                None,
                None,
            ));
        }
        // 步 3：HIGH 风险操作每次必须弹窗确认，不可被记住，跳过所有 Grant 匹配。
        if operation.risk == RiskClass::High {
            if mode == PermissionMode::Plan {
                self.record_denial(
                    context,
                    &subject,
                    &operation,
                    &execution_attempt_id,
                    DiagnosticSource::Policy,
                    EvaluationStage::Initial,
                    "PLAN_MODE_EFFECT_DENIED",
                );
                return Err(AuthzError::new(
                    "PLAN_MODE_EFFECT_DENIED",
                    "Plan mode does not permit this operation",
                ));
            }
            if mode == PermissionMode::DontAsk {
                self.record_denial(
                    context,
                    &subject,
                    &operation,
                    &execution_attempt_id,
                    DiagnosticSource::Policy,
                    EvaluationStage::Initial,
                    "INTERACTION_DISABLED",
                );
                return Err(AuthzError::new(
                    "PERMISSION_INTERACTION_REQUIRED",
                    "The operation needs explicit permission, but this session is non-interactive",
                ));
            }
            return self
                .interact(
                    tool,
                    frozen,
                    execution_input,
                    context,
                    &subject,
                    &operation,
                    &execution_attempt_id,
                )
                .await;
        }
        // 步 4：授权记录命中（同 operationHash / 同工具 / 同能力约束的免弹路径）。
        if let Some(matched) = self.grants.find_match(&subject, &operation).await? {
            return Ok(allow(
                DiagnosticSource::Grant,
                "GRANT_MATCH",
                Some(matched.grant_id),
                Some(matched.scope),
                None,
            ));
        }

        let scratchpad_read = operation.effects == [EffectClass::ReadResource];
        // 步 5。
        if scratchpad_read && self.is_trusted_system_scratchpad_file_operation(&subject, &operation)
        {
            return Ok(allow(
                DiagnosticSource::Policy,
                "SYSTEM_SCRATCHPAD_SCOPE",
                None,
                None,
                None,
            ));
        }
        let private_tmp_read = operation.effects == [EffectClass::ReadResource];
        // 步 6。
        if private_tmp_read
            && Self::is_trusted_private_tmp_file_operation_with(
                &|root| self.project_workspaces.is_trusted_file_scope(root),
                &subject,
                &operation,
            )
        {
            return Ok(allow(
                DiagnosticSource::Policy,
                "PRIVATE_TMP_FILE_SCOPE",
                None,
                None,
                None,
            ));
        }
        // 步 7：SAFE 读操作在所有模式下自动放行，不弹窗（读操作已统一为 SAFE 级别）。
        let safe_read = operation.analyzer_id == "file-v1"
            && operation.risk == RiskClass::Safe
            && operation.effects == [EffectClass::ReadResource];
        if safe_read {
            return Ok(allow(
                DiagnosticSource::Mode,
                "SAFE_READ_AUTO",
                None,
                None,
                None,
            ));
        }
        // 步 8。
        if mode == PermissionMode::Plan {
            self.record_denial(
                context,
                &subject,
                &operation,
                &execution_attempt_id,
                DiagnosticSource::Policy,
                EvaluationStage::Initial,
                "PLAN_MODE_EFFECT_DENIED",
            );
            return Err(AuthzError::new(
                "PLAN_MODE_EFFECT_DENIED",
                "Plan mode does not permit this operation",
            ));
        }
        // 步 9。
        if !scratchpad_read
            && self.is_trusted_system_scratchpad_file_operation(&subject, &operation)
        {
            return Ok(allow(
                DiagnosticSource::Policy,
                "SYSTEM_SCRATCHPAD_SCOPE",
                None,
                None,
                None,
            ));
        }
        // 步 10。
        if !private_tmp_read
            && Self::is_trusted_private_tmp_file_operation_with(
                &|root| self.project_workspaces.is_trusted_file_scope(root),
                &subject,
                &operation,
            )
        {
            return Ok(allow(
                DiagnosticSource::Policy,
                "PRIVATE_TMP_FILE_SCOPE",
                None,
                None,
                None,
            ));
        }
        // 步 11：已持久化的 Project 选择本身就是一份有界的文件域授权。DONT_ASK 只
        // 禁止新交互，并不撤销已选中的 Project（正如它不会绕过上面已命中的 grant）。
        if self.is_trusted_project_file_write(&subject, &operation) {
            return Ok(allow(
                DiagnosticSource::Policy,
                "PROJECT_FILE_SCOPE",
                None,
                None,
                None,
            ));
        }
        // 步 12。
        if mode == PermissionMode::DontAsk {
            self.record_denial(
                context,
                &subject,
                &operation,
                &execution_attempt_id,
                DiagnosticSource::Policy,
                EvaluationStage::Initial,
                "INTERACTION_DISABLED",
            );
            return Err(AuthzError::new(
                "PERMISSION_INTERACTION_REQUIRED",
                "The operation needs explicit permission, but this session is non-interactive",
            ));
        }
        // 步 13。
        if mode == PermissionMode::AcceptEdits
            && operation.analyzer_id == "file-v1"
            && operation.risk != RiskClass::High
            && !operation
                .resources
                .iter()
                .any(|resource| resource.outside_workspace)
            && operation.effects == [EffectClass::WriteResource]
        {
            return Ok(allow(
                DiagnosticSource::Mode,
                "ACCEPT_EDITS",
                None,
                None,
                None,
            ));
        }
        // 步 14。
        self.interact(
            tool,
            frozen,
            execution_input,
            context,
            &subject,
            &operation,
            &execution_attempt_id,
        )
        .await
    }

    /// 旧源 `isTrustedProjectFileWrite`（L215-227）。
    fn is_trusted_project_file_write(
        &self,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_project_file_write_with(subject, operation, &|root| {
            self.project_workspaces.is_trusted_file_scope(root)
        })
    }

    /// [`Self::is_trusted_project_file_write`] 的判定主体：工作区信任探测以闭包
    /// 注入，供自锁出口与当前写事务出口共用（描述符检查逐字不变）。
    fn is_trusted_project_file_write_with(
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        workspace_trust: &dyn Fn(&Path) -> bool,
    ) -> bool {
        operation.analyzer_id == "file-v1"
            && operation.risk == RiskClass::Guarded
            && operation.effects == [EffectClass::WriteResource]
            && !operation.resources.is_empty()
            && !operation
                .resources
                .iter()
                .any(|resource| resource.outside_workspace)
            && workspace_trust(&subject.authorization_root)
    }

    /// 旧源 `isTrustedSystemScratchpadFileOperation`（L229-234）。
    fn is_trusted_system_scratchpad_file_operation(
        &self,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_ordinary_file_operation(
            &|root| self.project_workspaces.is_trusted_file_scope(root),
            subject,
            operation,
            &|target| self.system_scratchpad_paths.contains(target),
        )
    }

    /// 旧源 `isTrustedPrivateTmpFileOperation`（L236-242）。
    fn is_trusted_private_tmp_file_operation_with(
        workspace_trust: &dyn Fn(&Path) -> bool,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_ordinary_file_operation(workspace_trust, subject, operation, &|target| {
            target.starts_with(&*PRIVATE_TMP_ROOT)
        })
    }

    /// [`Self::is_trusted_project_file_write`] 的当前写事务变体：终局复检在
    /// `Db::with_writer` 闭包内执行（writer `Mutex` 不可重入），信任探测必须
    /// 复用已持有的 `conn`——走自锁出口即重入死锁。
    fn is_trusted_project_file_write_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_project_file_write_with(subject, operation, &|root| {
            self.project_workspaces
                .is_trusted_file_scope_in_current_write(conn, root)
        })
    }

    /// [`Self::is_trusted_system_scratchpad_file_operation`] 的当前写事务变体
    ///（同上：信任探测复用已持有的 `conn`）。
    fn is_trusted_system_scratchpad_file_operation_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_ordinary_file_operation(
            &|root| {
                self.project_workspaces
                    .is_trusted_file_scope_in_current_write(conn, root)
            },
            subject,
            operation,
            &|target| self.system_scratchpad_paths.contains(target),
        )
    }

    /// [`Self::is_trusted_private_tmp_file_operation_with`] 的当前写事务变体
    ///（同上：信任探测复用已持有的 `conn`）。
    fn is_trusted_private_tmp_file_operation_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
    ) -> bool {
        Self::is_trusted_private_tmp_file_operation_with(
            &|root| {
                self.project_workspaces
                    .is_trusted_file_scope_in_current_write(conn, root)
            },
            subject,
            operation,
        )
    }

    /// 旧源 `isTrustedOrdinaryFileOperation`（L244-276）。
    ///
    /// 逐字保留三点：非 `file-v1` / HIGH / 非「普通文件操作」/ 资源为空 一律 false；
    /// 每个资源必须 `kind == "path"` 且非空白且解析后落在 `containsTarget` 内；
    /// 路径解析异常按 false 处理；最后才查工作区信任。
    fn is_trusted_ordinary_file_operation(
        workspace_trust: &dyn Fn(&Path) -> bool,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        contains_target: &dyn Fn(&Path) -> bool,
    ) -> bool {
        if operation.analyzer_id != "file-v1"
            || operation.risk == RiskClass::High
            || !Self::is_ordinary_file_operation(operation)
            || operation.resources.is_empty()
        {
            return false;
        }
        for resource in &operation.resources {
            if resource.kind != "path" || resource.value.trim().is_empty() {
                return false;
            }
            if resource.value.contains('\0') {
                // 旧源 `Path.of` 对含 NUL 抛 `InvalidPathException` → catch → false。
                return false;
            }
            let value = Path::new(&resource.value);
            let target = if value.is_absolute() {
                value.to_path_buf()
            } else {
                subject.authorization_root.join(value)
            };
            if !contains_target(&crate::workspace::absolute_normalized(&target)) {
                return false;
            }
        }
        workspace_trust(&subject.authorization_root)
    }

    /// 旧源 `isOrdinaryFileOperation`（L278-299）：封闭的三工具白名单，
    /// 且 action 与 effects 必须与该工具的规范组合完全一致。
    fn is_ordinary_file_operation(operation: &OperationDescriptor) -> bool {
        match operation.tool_name.as_str() {
            "Read" => {
                operation.action == TypedFileOperation::ReadFile.as_str()
                    && operation.effects == [EffectClass::ReadResource]
            }
            "Write" => {
                operation.action == TypedFileOperation::ReplaceFile.as_str()
                    && operation.effects == [EffectClass::WriteResource]
            }
            "Edit" | "NotebookEdit" => {
                operation.action == TypedFileOperation::PatchFile.as_str()
                    && operation.effects == [EffectClass::WriteResource]
            }
            _ => false,
        }
    }

    /// 旧源 `finalDynamicRecheck`（L302-306）。
    ///
    /// # Errors
    /// 不变量拒绝或分析器复检发现漂移时返回 [`AuthzError`]。
    pub fn final_dynamic_recheck(
        &self,
        tool: &dyn ToolFacts,
        authorized: &AuthorizedOperation,
        context: &ToolUseContext,
    ) -> AuthzResult<()> {
        Self::invariant(&authorized.descriptor)?;
        let kind = self.analyzers.analyzer_for(tool);
        self.analyzers.recheck(
            kind,
            tool,
            &authorized.descriptor,
            &authorized.execution_input,
            context,
            &authorized.subject,
        )
    }

    /// 旧源 `finalGrantRecheckInCurrentTransaction`（L308-357）。
    ///
    /// **必须在调用方持有的写事务内执行**（旧源同名注释）。故签名接受 `&Connection`
    /// 而非 `&self.db`：`zk_db::Db` 单 writer 串行化只在同一 `with_writer` 闭包内
    /// 才等价于旧 `runs.executeBoundedWrite`。
    ///
    /// # Errors
    /// 授权记录已撤销/过期、一次性批准已被消费、或策略放行前提（Project 信任 /
    /// scratchpad / `/private/tmp` 归属）在此刻不再成立时返回
    /// `AUTHORIZATION_FINAL_RECHECK_DENIED`。
    pub fn final_grant_recheck_in_current_transaction(
        &self,
        conn: &rusqlite::Connection,
        authorized: &AuthorizedOperation,
        context: &ToolUseContext,
    ) -> AuthzResult<()> {
        let denied = |message: &str| {
            AuthzError::new("AUTHORIZATION_FINAL_RECHECK_DENIED", message.to_owned())
        };
        if let Some(grant_id) = authorized.grant_id.as_deref() {
            let current = grants::find_match_in_tx(
                conn,
                &self.grants.now_iso(),
                &authorized.subject,
                &authorized.descriptor,
            )
            .map_err(|error| denied(&format!("Permission grant recheck failed: {error}")))?;
            if current.is_none_or(|current| current.grant_id != grant_id) {
                return Err(denied("The matching permission grant is no longer valid"));
            }
        } else if authorized.source == DiagnosticSource::UserOnce {
            let tool_use_id = context.tool_use_id_or(&authorized.descriptor.tool_name);
            self.interactions
                .require_answered_once_in_tx(
                    conn,
                    authorized.interaction_id.as_deref(),
                    &authorized.subject,
                    &authorized.descriptor,
                    &tool_use_id,
                )
                .map_err(|_| denied("The one-time permission decision is no longer valid"))?;
        } else if authorized.source == DiagnosticSource::Policy
            && authorized.reason_code == "PROJECT_FILE_SCOPE"
            && !self.is_trusted_project_file_write_in_current_write(
                conn,
                &authorized.subject,
                &authorized.descriptor,
            )
        {
            return Err(denied("The Project file authorization is no longer valid"));
        } else if authorized.source == DiagnosticSource::Policy
            && authorized.reason_code == "SYSTEM_SCRATCHPAD_SCOPE"
            && !self.is_trusted_system_scratchpad_file_operation_in_current_write(
                conn,
                &authorized.subject,
                &authorized.descriptor,
            )
        {
            return Err(denied(
                "The system scratchpad authorization is no longer valid",
            ));
        } else if authorized.source == DiagnosticSource::Policy
            && authorized.reason_code == "PRIVATE_TMP_FILE_SCOPE"
            && !self.is_trusted_private_tmp_file_operation_in_current_write(
                conn,
                &authorized.subject,
                &authorized.descriptor,
            )
        {
            return Err(denied(
                "The private temp file authorization is no longer valid",
            ));
        }
        Ok(())
    }

    /// 旧源 `recordFinalDenial`（L359-368）：诊断写入失败只记 error，不改变裁决。
    pub fn record_final_denial(
        &self,
        authorized: &AuthorizedOperation,
        context: &ToolUseContext,
        reason_code: &str,
    ) {
        self.record_denial(
            context,
            &authorized.subject,
            &authorized.descriptor,
            &authorized.execution_attempt_id,
            DiagnosticSource::Invariant,
            EvaluationStage::FinalRecheck,
            reason_code,
        );
    }

    /// 旧源 `interact(...)`（L370-437）。
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn interact(
        &self,
        tool: &dyn ToolFacts,
        frozen: &FrozenToolInput,
        execution_input: Value,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        execution_attempt_id: &str,
    ) -> AuthzResult<AuthorizedOperation> {
        let tool_use_id = context.tool_use_id_or(tool.name());
        let correlation = format!("permission-v3:{tool_use_id}:{}", operation.operation_hash);
        let existing = self
            .interactions
            .find_by_correlation_key(context.current_run_id.as_deref(), &correlation)
            .await?;
        let request: InteractionRecord = if let Some(request) = existing {
            request
        } else {
            let scopes = Self::scopes(operation);
            let decision_options = Self::options(&scopes);
            let mut prompt = Map::new();
            prompt.insert("toolName".into(), tool.name().into());
            prompt.insert("toolUseId".into(), tool_use_id.clone().into());
            prompt.insert("reason".into(), Self::reason(operation).into());
            prompt.insert(
                "riskLevel".into(),
                operation.risk.as_str().to_ascii_lowercase().into(),
            );
            prompt.insert(
                "inputSummary".into(),
                operation.redacted_summary.clone().into(),
            );
            prompt.insert(
                "operationHash".into(),
                operation.operation_hash.clone().into(),
            );
            prompt.insert(
                "options".into(),
                serde_json::to_value(&decision_options).unwrap_or(Value::Null),
            );
            if !scopes.is_empty()
                && let Some(description) = Self::remember_scope_description(operation)
            {
                prompt.insert("rememberScopeDescription".into(), description.into());
            }
            let authorization_context = AuthorizationInteractionContext {
                protocol_version: PROTOCOL_VERSION,
                tool_use_id: tool_use_id.clone(),
                execution_attempt_id: execution_attempt_id.to_owned(),
                input_hash: frozen.input_hash().to_owned(),
                operation_hash: operation.operation_hash.clone(),
                subject: AuthorizationSubjectData::from_subject(subject),
                operation: operation.clone(),
                options: decision_options,
            };
            self.interactions
                .create_authorization(AuthorizationInteractionSpec {
                    correlation_key: correlation,
                    root_session_id: subject.root_session_id.clone(),
                    run_id: context.current_run_id.clone(),
                    prompt,
                    allowed_decisions: vec!["allow".to_owned(), "deny".to_owned()],
                    scope_options: scopes.iter().map(|scope| scope.lowercase()).collect(),
                    source: if subject.root_run_id == subject.current_run_id {
                        "direct".to_owned()
                    } else {
                        "descendant".to_owned()
                    },
                    authorization_context,
                })
                .await?
        };
        let status = if request.status == InteractionStatus::Pending {
            self.interactions
                .await_terminal(&request.interaction_id)
                .await?
        } else {
            request.status
        };
        let request = self
            .interactions
            .find_by_id(&request.interaction_id)
            .await?
            .ok_or_else(|| {
                AuthzError::new(
                    "INTERACTION_PROTOCOL_INVALID",
                    "The permission interaction row disappeared",
                )
            })?;
        if status != InteractionStatus::Answered {
            let code = status.denial_code();
            self.record_denial(
                context,
                subject,
                operation,
                execution_attempt_id,
                DiagnosticSource::Policy,
                EvaluationStage::Interaction,
                code,
            );
            return Err(AuthzError::new(
                code,
                format!("Permission interaction ended with {status}"),
            ));
        }
        let response = Self::response(&request)?;
        if response.get("operationHash").and_then(Value::as_str)
            != Some(operation.operation_hash.as_str())
        {
            return Err(AuthzError::new(
                "PERMISSION_PROTOCOL_MISMATCH",
                "Permission response does not match this operation",
            ));
        }
        let remembered_requested = response.get("remember") == Some(&Value::Bool(true));
        if operation.risk == RiskClass::High && remembered_requested {
            return Err(AuthzError::new(
                "PERMISSION_HIGH_RISK_NOT_REMEMBERABLE",
                "HIGH risk operations cannot be remembered",
            ));
        }
        if remembered_requested {
            // 授权记录由 `DurableInteractionService` 在写入决策的同一事务内创建；
            // 此处只确认它确实已提交，未提交即拒绝（不退化成一次性放行）。
            let Some(remembered) = self.grants.find_match(subject, operation).await? else {
                return Err(AuthzError::new(
                    "PERMISSION_GRANT_NOT_COMMITTED",
                    "The remembered permission decision has no matching grant",
                ));
            };
            return Ok(AuthorizedOperation {
                subject: subject.clone(),
                descriptor: operation.clone(),
                execution_input,
                source: DiagnosticSource::Grant,
                reason_code: "USER_REMEMBERED_GRANT".to_owned(),
                grant_id: Some(remembered.grant_id),
                grant_scope: Some(remembered.scope),
                interaction_id: Some(request.interaction_id),
                execution_attempt_id: execution_attempt_id.to_owned(),
            });
        }
        Ok(AuthorizedOperation {
            subject: subject.clone(),
            descriptor: operation.clone(),
            execution_input,
            source: DiagnosticSource::UserOnce,
            reason_code: "USER_APPROVED_ONCE".to_owned(),
            grant_id: None,
            grant_scope: None,
            interaction_id: Some(request.interaction_id),
            execution_attempt_id: execution_attempt_id.to_owned(),
        })
    }

    /// 旧源 `invariant(OperationDescriptor)`（L439-444）。
    ///
    /// 两条不可绕过的不变量，在 `prepare` 与 `finalDynamicRecheck` 各执行一次。
    ///
    /// # Errors
    /// 资源含 NUL 字节、或继承环境变量名命中敏感名模式时返回
    /// `AUTHORIZATION_INVARIANT_DENIED`。
    pub fn invariant(operation: &OperationDescriptor) -> AuthzResult<()> {
        if operation
            .resources
            .iter()
            .any(|resource| resource.value.contains('\0'))
        {
            return Err(AuthzError::new(
                "AUTHORIZATION_INVARIANT_DENIED",
                "Resource contains invalid characters",
            ));
        }
        if operation
            .inherited_environment_names
            .iter()
            .any(|name| Self::sensitive_name(name))
        {
            return Err(AuthzError::new(
                "SENSITIVE_ENVIRONMENT_ACCESS",
                "Sensitive environment access is not reusable",
            ));
        }
        Ok(())
    }

    /// 旧源 `scopes(OperationDescriptor)`（L446-448）。
    fn scopes(operation: &OperationDescriptor) -> Vec<PermissionScope> {
        grants::supported_scopes(operation)
    }

    /// 旧源 `rememberScopeDescription`（L449-459）：仅远程分析器有可记住范围文案。
    fn remember_scope_description(operation: &OperationDescriptor) -> Option<String> {
        match operation.analyzer_id.as_str() {
            "network-v1" => Some(format!(
                "Saved permission applies to {} only; URL and input values may change. \
                 Other network tools remain separate. Run/session limits follow the selected \
                 option, and saved grants expire within 12 hours.",
                operation.tool_name
            )),
            "mcp-v1" => Some(format!(
                "Saved permission applies to MCP tool {} only; input values may change. \
                 Other MCP tools remain separate. Run/session limits follow the selected \
                 option, and saved grants expire within 12 hours.",
                operation.tool_name
            )),
            _ => None,
        }
    }

    /// 旧源 `options(List<PermissionScope>)`（L460-467）：
    /// `allow_once` 恒首项、`deny` 恒末项，中间按支持范围顺序展开。
    fn options(scopes: &[PermissionScope]) -> Vec<DecisionOption> {
        let mut options = Vec::with_capacity(scopes.len() + 2);
        options.push(DecisionOption {
            option_id: "allow_once".to_owned(),
            decision: "allow".to_owned(),
            scope: "once".to_owned(),
        });
        for scope in scopes {
            options.push(DecisionOption {
                option_id: format!("allow_{}", scope.lowercase()),
                decision: "allow".to_owned(),
                scope: scope.lowercase(),
            });
        }
        options.push(DecisionOption {
            option_id: "deny".to_owned(),
            decision: "deny".to_owned(),
            scope: "once".to_owned(),
        });
        options
    }

    /// 旧源 `response(InteractionRequest)`（L468-471）。
    fn response(request: &InteractionRecord) -> AuthzResult<Map<String, Value>> {
        let invalid = || {
            AuthzError::new(
                "INTERACTION_PROTOCOL_INVALID",
                "Invalid permission response",
            )
        };
        let raw = request.response_json.as_deref().ok_or_else(invalid)?;
        match serde_json::from_str::<Value>(raw) {
            Ok(Value::Object(map)) => Ok(map),
            _ => Err(invalid()),
        }
    }

    /// 旧源 `reason(OperationDescriptor)`（L472-478）。
    fn reason(operation: &OperationDescriptor) -> &'static str {
        match operation.risk {
            RiskClass::Safe => "Read access requires confirmation",
            RiskClass::Guarded => "This operation can modify or inspect workspace resources",
            RiskClass::High => "This operation has external, control-plane, or unbounded effects",
        }
    }

    /// 旧源 `sensitiveName(String)`（L479-483）：大写包含匹配，6 个片段。
    fn sensitive_name(value: &str) -> bool {
        let upper = value.to_uppercase();
        upper.contains("TOKEN")
            || upper.contains("SECRET")
            || upper.contains("PASSWORD")
            || upper.contains("API_KEY")
            || upper.contains("PRIVATE_KEY")
            || upper.contains("CREDENTIAL")
    }

    /// 旧源 `recordDenial(...)`（L484-494）：无 run 上下文时静默跳过。
    #[allow(clippy::too_many_arguments)]
    fn record_denial(
        &self,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
        operation: &OperationDescriptor,
        execution_attempt_id: &str,
        source: DiagnosticSource,
        stage: EvaluationStage,
        reason_code: &str,
    ) {
        let Some(run_id) = context.current_run_id.as_deref() else {
            return;
        };
        let payload = diagnostic::payload(
            subject,
            operation,
            context,
            execution_attempt_id,
            DiagnosticOutcome::Deny,
            stage,
            source,
            reason_code,
            None,
            None,
            None,
        );
        self.runs.append_event(
            run_id,
            "authorization_denied",
            context.tool_use_id.as_deref(),
            &payload,
        );
    }

    /// 旧源 `recordAnalysisFailure(...)`（L496-499）。
    fn record_analysis_failure(
        &self,
        context: &ToolUseContext,
        subject: &AuthorizationSubject,
        tool_name: &str,
        input_hash: &str,
        execution_attempt_id: &str,
        reason_code: &str,
    ) {
        let Some(run_id) = context.current_run_id.as_deref() else {
            return;
        };
        let payload = diagnostic::analysis_failure(
            subject,
            tool_name,
            input_hash,
            context,
            execution_attempt_id,
            reason_code,
        );
        self.runs.append_event(
            run_id,
            "authorization_denied",
            context.tool_use_id.as_deref(),
            &payload,
        );
    }

    /// 暴露注册表给唯一执行网关（`gateway.rs`）与集成测试。
    #[must_use]
    pub fn analyzers(&self) -> &OperationAnalyzerRegistry {
        &self.analyzers
    }

    /// 暴露分析器判别式（诊断/测试用）。
    #[must_use]
    pub fn analyzer_kind(&self, tool: &dyn ToolFacts) -> AnalyzerKind {
        self.analyzers.analyzer_for(tool)
    }
}
