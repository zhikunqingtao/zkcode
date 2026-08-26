//! zk-protocol 契约冻结测试：全 variant roundtrip、序列化形状断言（含 U1
//! 扁平平铺验证）、真实样例对照、未知 type 拒绝、白名单核对。
//!
//! # 真实样例来源说明
//!
//! `docs/case-studies/` 经实地核查**不含下行 WS 帧 JSON**（仅服务端日志行与
//! 审计证据文件），故「真实样例」取自两类与线上逐字段核验过的来源：
//! 1. 前端消费方测试 `frontend/src/store/__tests__/dispatch.test.ts` 与
//!    `__tests__/api/dispatchRecovery.test.ts`（dispatch 按顶层字段消费，其
//!    构造即真实形状）；
//! 2. 后端直推调用点字段（`WebSocketController` push 系列 / `pushBindError` /
//!    `handleRunInput` / ACK 路径）。
//!
//! 样例对照的值保真比较采用 **null 归一化**：递归剔除值为 `null` 的键后再比较
//! ——zkcode 对 `Option::None` 省略键输出，与旧系统显式 `"field": null` 对消费方
//! 等价（JS 中 null/undefined 同为 falsy），属接受的序列化差异。

use std::collections::BTreeSet;

use serde_json::{Value, json};
use zk_protocol::{
    Attachment, ClientEnvelope, ClientMessage, ContentBlock, ElicitationOption, FlexEpoch,
    InteractionView, McpToolInfo, Message, Reference, ServerEnvelope, ServerMessage,
    SessionMetadata, ToolResultContent, Usage, VALID_SERVER_MESSAGE_TYPES, WorkerSnapshot,
};

const TS: i64 = 1_755_000_000_000;

/// 构造最简下行信封（ts 固定、无 seq / 路由字段）。
fn env(msg: ServerMessage) -> ServerEnvelope {
    ServerEnvelope::new(msg, TS, None)
}

/// 全部 57 个 `ServerMessage` variant 的代表性样本（roundtrip + kind 域核查共用）。
// 样本数由契约规模（57 variant）决定，拆分反而破坏「一 variant 一样本」可核对性。
#[allow(clippy::too_many_lines)]
fn server_samples() -> Vec<ServerEnvelope> {
    vec![
        env(ServerMessage::StreamDelta {
            delta: "你好".into(),
        }),
        env(ServerMessage::ThinkingDelta {
            delta: "思考".into(),
        }),
        env(ServerMessage::ToolUseStart {
            tool_use_id: "tu1".into(),
            tool_name: "Bash".into(),
            input: json!({}),
        }),
        env(ServerMessage::ToolUseProgress {
            tool_use_id: "tu1".into(),
            progress: "stdout...".into(),
        }),
        env(ServerMessage::ToolResult {
            tool_use_id: "tu1".into(),
            result: ToolResultContent {
                content: "done".into(),
                is_error: false,
                metadata: Some(json!({"structuredResult": {"exitCode": 0}})),
            },
        }),
        env(ServerMessage::ToolUseInput {
            tool_use_id: "tu1".into(),
            tool_name: "Write".into(),
            input: json!({"path": "/tmp/a.txt"}),
        }),
        env(ServerMessage::ToolPermissionDenied {
            tool_use_id: "tu1".into(),
            tool_name: "Bash".into(),
        }),
        env(ServerMessage::PermissionRequest {
            interaction_id: Some("i1".into()),
            tool_use_id: "tu1".into(),
            tool_name: "Bash".into(),
            input: json!({"command": "ls"}),
            risk_level: "medium".into(),
            reason: "test".into(),
            source: Some("subagent".into()),
            child_session_id: Some("cs1".into()),
            decision_deadline_at: Some(FlexEpoch::from_millis(TS + 60_000)),
            scope_options: Some(vec!["session".into()]),
        }),
        env(ServerMessage::MessageComplete {
            usage: Usage {
                input_tokens: 100,
                output_tokens: 50,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            stop_reason: Some("end_turn".into()),
            session_id: None,
            run_id: None,
            replace_after_message_id: None,
            committed_messages: None,
        }),
        env(ServerMessage::Error {
            code: "query_busy".into(),
            message: "当前会话正在处理中".into(),
            retryable: false,
        }),
        env(ServerMessage::CompactStart),
        env(ServerMessage::CompactComplete {
            summary: "已压缩".into(),
            tokens_saved: 12_345,
        }),
        env(ServerMessage::Elicitation {
            request_id: "req1".into(),
            question: "选择？".into(),
            options: vec![
                ElicitationOption {
                    value: "a".into(),
                    label: "选项 A".into(),
                },
                ElicitationOption {
                    value: "b".into(),
                    label: "选项 B".into(),
                },
            ],
        }),
        env(ServerMessage::AgentSpawn {
            task_id: "t1".into(),
            agent_name: "Jimmy".into(),
            agent_type: "coder".into(),
        }),
        env(ServerMessage::AgentUpdate {
            task_id: "t1".into(),
            progress: "50%".into(),
        }),
        env(ServerMessage::AgentComplete {
            task_id: "t1".into(),
            result: "ok".into(),
        }),
        env(ServerMessage::AgentStarted {
            agent_id: "a1".into(),
            prompt: "explore code".into(),
        }),
        env(ServerMessage::AgentCompleted {
            agent_id: "a1".into(),
            result: "found 3 issues".into(),
        }),
        env(ServerMessage::AgentFailed {
            agent_id: "a1".into(),
            error: "model error".into(),
        }),
        env(ServerMessage::CostUpdate {
            session_cost: 0.125,
            total_cost: 1.5,
            usage: Usage {
                input_tokens: 1,
                output_tokens: 2,
                cache_read_input_tokens: 3,
                cache_creation_input_tokens: 4,
            },
        }),
        env(ServerMessage::RateLimit {
            retry_after_ms: 30_000,
            limit_type: "rpm".into(),
        }),
        env(ServerMessage::Notification {
            key: "k1".into(),
            level: "info".into(),
            message: "通知".into(),
            timeout: 8_000,
        }),
        env(ServerMessage::TaskUpdate {
            task_id: "t1".into(),
            status: "running".into(),
            progress: Some("step 2".into()),
            output: None,
        }),
        env(ServerMessage::PromptSuggestion {
            suggestions: vec!["继续".into(), "重试".into()],
        }),
        env(ServerMessage::BridgeStatus {
            status: "connected".into(),
            url: "http://127.0.0.1:7474".into(),
        }),
        env(ServerMessage::TeammateMessage {
            from_id: "w1".into(),
            content: "收到".into(),
        }),
        env(ServerMessage::SpeculationResult {
            id: "spec1".into(),
            accepted: true,
        }),
        env(ServerMessage::McpToolUpdate {
            server_id: "srv1".into(),
            tools: vec![McpToolInfo {
                name: "search".into(),
                description: "搜索".into(),
                input_schema: json!({"type": "object"}),
            }],
        }),
        env(ServerMessage::SessionRestored {
            messages: vec![Message::User {
                uuid: "u1".into(),
                timestamp: 1,
                content: vec![ContentBlock::Text { text: "hi".into() }],
            }],
            metadata: SessionMetadata {
                session_id: "s1".into(),
                model: "qwen3-coder".into(),
                permission_mode: "AUTO_APPROVE".into(),
                status: "idle".into(),
            },
            activities: Some(vec![json!({"id": "a1"})]),
            total_activity_count: Some(1),
            has_more: Some(false),
            protocol_version: 3,
            bind_request_id: Some("br1".into()),
            binding_epoch: Some(2),
            server_now: Some(TS),
            run_snapshot: None,
            snapshot_event_seq: None,
            active_tool_calls: None,
            cost_summary: None,
        }),
        env(ServerMessage::Pong {
            bind_required: None,
            server_now: None,
        }),
        env(ServerMessage::CompactEvent {
            phase: "warning".into(),
            usage_percent: 85,
            current_tokens: 0,
        }),
        env(ServerMessage::TokenWarning {
            current_tokens: 180_000,
            max_tokens: 200_000,
            usage_percent: 90.0,
            warning_level: "red".into(),
        }),
        env(ServerMessage::InterruptAck {
            reason: "USER_INTERRUPT".into(),
        }),
        env(ServerMessage::ModelChanged {
            model: "qwen3.6-plus".into(),
        }),
        env(ServerMessage::PermissionModeChanged {
            mode: "AUTO_APPROVE".into(),
            previous: Some("DEFAULT".into()),
        }),
        env(ServerMessage::CommandResult {
            command: "plan".into(),
            result_type: "text".into(),
            output: Some("Plan Mode enabled".into()),
            data: None,
        }),
        env(ServerMessage::RewindComplete {
            message_id: "m1".into(),
            success: true,
            restored_files: vec!["a.rs".into()],
            skipped_files: vec![],
            errors: vec![],
            files: vec!["a.rs".into()],
        }),
        env(ServerMessage::McpHealthStatus {
            server_name: "srv1".into(),
            status: "reconnecting".into(),
            consecutive_failures: 2,
            last_successful_ping: Some(TS),
        }),
        env(ServerMessage::TokenBudgetNudge {
            pct: 80,
            current_tokens: 8_000,
            budget_tokens: 10_000,
        }),
        env(ServerMessage::SwarmStateUpdate {
            swarm_id: "sw1".into(),
            phase: "RUNNING".into(),
            active_workers: 1,
            total_workers: 2,
            completed_tasks: 0,
            total_tasks: 4,
            workers: [(
                "w1".to_owned(),
                WorkerSnapshot {
                    worker_id: "w1".into(),
                    status: "WORKING".into(),
                    current_task: Some("task-a".into()),
                    tool_call_count: 3,
                    token_consumed: 1_024,
                },
            )]
            .into_iter()
            .collect(),
        }),
        env(ServerMessage::WorkerProgress {
            swarm_id: "sw1".into(),
            worker_id: "w1".into(),
            status: "WORKING".into(),
            current_task: Some("task-a".into()),
            tool_call_count: 3,
            token_consumed: 1_024,
            recent_tool_calls: Some(vec!["Read".into(), "Write".into()]),
            progress_percent: Some(50),
            total_steps: Some(10),
            completed_steps: Some(5),
            error_message: None,
            current_step_description: Some("第 5 步".into()),
            termination_reason: None,
        }),
        env(ServerMessage::WorkflowPhaseUpdate {
            workflow_id: "wf1".into(),
            phase_name: "Research".into(),
            status: "RUNNING".into(),
            phase_index: 0,
            total_phases: 4,
            phase_prompt: "调研".into(),
            objective: "重构协议层".into(),
        }),
        env(ServerMessage::ModelRouted {
            original_model: "qwen3-coder".into(),
            routed_model: "qwen3-vl".into(),
            routed_model_name: "Qwen3 VL".into(),
            reason: "当前模型不支持图片".into(),
        }),
        env(ServerMessage::PlanUpdate {
            is_plan_mode: true,
            plan_name: Some("New Plan".into()),
            plan_overview: Some(String::new()),
        }),
        env(ServerMessage::SessionListUpdated),
        env(ServerMessage::Visualization {
            uuid: "v1".into(),
            view_type: "mermaid".into(),
            props: json!({"chart": "graph TB"}),
        }),
        env(ServerMessage::VerificationResult {
            signal: "auto_approve".into(),
            signal_reason: "全部通过".into(),
            overall_status: "pass".into(),
            duration: 1_200,
            file_count: 3,
            timestamp: "2026-08-15T10:00:00Z".into(),
        }),
        env(ServerMessage::VerifyProgress {
            file_path: "src/a.rs".into(),
            completed: 1,
            total: 3,
            result: json!({"filePath": "src/a.rs"}),
        }),
        env(ServerMessage::VerifyAttention {
            session_id: "s1".into(),
            bundle_id: "b1".into(),
            verdict: "failed".into(),
            claim: Some("声称完成".into()),
            summary: Some("证据不足".into()),
            requires_approval: true,
            timestamp: "2026-08-15T10:00:00Z".into(),
        }),
        env(ServerMessage::ProtocolError {
            code: "SESSION_NOT_FOUND".into(),
            supported_version: 3,
            bind_request_id: Some("br1".into()),
            binding_epoch: Some(1),
        }),
        env(ServerMessage::InteractionCreated {
            view: sample_interaction_view(),
        }),
        env(ServerMessage::InteractionTerminal {
            view: sample_interaction_view(),
        }),
        env(ServerMessage::InteractionUpdated {
            view: InteractionView {
                interaction_id: "i1".into(),
                decision_deadline_at: Some(FlexEpoch::from_millis(TS + 60_000)),
                server_now: Some(TS),
                version: Some(7),
                ..sample_interaction_view()
            },
        }),
        env(ServerMessage::RunInputQueued {
            request_id: "r1".into(),
            submitted_at: TS,
        }),
        env(ServerMessage::RunInputApplied {
            request_id: "r1".into(),
            text: "change direction".into(),
            applied_at: TS + 1_000,
        }),
        env(ServerMessage::RunInputRejected {
            request_id: "r1".into(),
            code: "NO_ACTIVE_RUN".into(),
            message: "无运行中的任务".into(),
            rejected_at: TS,
        }),
        env(ServerMessage::McpToolProgress {
            progress_token: "pt1".into(),
            server_name: "srv1".into(),
            tool_name: "long_query".into(),
            progress: 0.5,
            total: 1.0,
            message: "查询中".into(),
        }),
    ]
}

fn sample_interaction_view() -> InteractionView {
    InteractionView {
        interaction_id: "permission-ack".into(),
        protocol_version: Some(3),
        correlation_key: Some("tool-ack".into()),
        session_id: Some("session-ack".into()),
        run_id: Some("run-ack".into()),
        interaction_type: Some("permission".into()),
        status: Some("pending".into()),
        prompt: Some(json!({
            "toolUseId": "tool-ack",
            "toolName": "Write",
            "inputSummary": "ack.txt",
            "riskLevel": "medium",
            "reason": "test"
        })),
        allowed_decisions: Some(vec!["allow".into(), "deny".into()]),
        scope_options: Some(vec!["run".into(), "session".into()]),
        response: None,
        source: None,
        child_session_id: None,
        actor_run_id: None,
        actor_type: None,
        delivery_generation: Some(1),
        dispatch_attempts: None,
        created_at: Some(FlexEpoch::from_millis(TS)),
        received_at: None,
        decision_deadline_at: Some(FlexEpoch::from_millis(TS + 120_000)),
        delivery_window_ends_at: Some(FlexEpoch::from_millis(TS + 60_000)),
        decided_at: None,
        terminal_reason: None,
        version: Some(1),
        server_now: Some(TS),
        operation_hash: None,
        options: None,
    }
}

/// 全部 16 个 `ClientMessage` variant 的代表性样本。
fn client_samples() -> Vec<ClientEnvelope> {
    vec![
        ClientEnvelope::new(ClientMessage::UserMessage {
            text: "你好".into(),
            attachments: Some(vec![Attachment {
                kind: "image".into(),
                path: None,
                media_type: Some("image/png".into()),
                base64_data: Some("aGk=".into()),
                url: None,
            }]),
            references: Some(vec![Reference {
                kind: "file".into(),
                path: "src/main.rs".into(),
                start_line: Some(1),
                end_line: Some(10),
            }]),
        }),
        ClientEnvelope::new(ClientMessage::RunInput {
            request_id: "r1".into(),
            text: "追加指令".into(),
        }),
        ClientEnvelope::new(ClientMessage::PermissionResponse {
            tool_use_id: "tu1".into(),
            decision: "allow".into(),
            remember: true,
            scope: "session".into(),
        }),
        ClientEnvelope::new(ClientMessage::Interrupt {
            is_submit_interrupt: Some(true),
        }),
        ClientEnvelope::new(ClientMessage::SetModel {
            model: "qwen3-coder".into(),
        }),
        ClientEnvelope::new(ClientMessage::SetPermissionMode {
            mode: "AUTO_APPROVE".into(),
        }),
        ClientEnvelope::new(ClientMessage::SlashCommand {
            command: "plan".into(),
            args: "on 重写".into(),
        }),
        ClientEnvelope::new(ClientMessage::McpOperation {
            operation: "connect".into(),
            server_id: "srv1".into(),
            config: Some(json!({"url": "http://localhost:9000"})),
        }),
        ClientEnvelope::new(ClientMessage::RewindFiles {
            message_id: "m1".into(),
            file_paths: vec!["a.rs".into()],
        }),
        ClientEnvelope::new(ClientMessage::ElicitationResponse {
            request_id: "req1".into(),
            answer: json!("a"),
        }),
        ClientEnvelope::new(ClientMessage::Ping),
        ClientEnvelope::new(ClientMessage::BindSession {
            session_id: "s1".into(),
            bind_request_id: "br1".into(),
            binding_epoch: 1,
            protocol_version: 3,
        }),
        ClientEnvelope::new(ClientMessage::InteractionAck {
            interaction_id: "i1".into(),
            delivery_generation: 1,
        }),
        ClientEnvelope::new(ClientMessage::ActivitySave {
            id: "act1".into(),
            operation_type: "edit".into(),
            summary: Some("编辑文件".into()),
            status: Some("done".into()),
            timestamp: Some(TS),
            duration: Some(1_500),
            file_count: Some(2),
            decision: None,
            tool_result: Some(json!({"exitCode": 0})),
            changed_files: Some(json!(["a.rs"])),
            insight: None,
        }),
        ClientEnvelope::new(ClientMessage::ActivityUpdate {
            id: "act1".into(),
            decision: Some("approved".into()),
            insight: Some(json!({"note": "ok"})),
        }),
    ]
}

/// 1）全 57 个下行 variant：构造样本 → `to_string` → `from_str` → 相等。
#[test]
fn server_roundtrip_all_variants() {
    let samples = server_samples();
    assert_eq!(samples.len(), 57, "ServerMessage variant 计数必须为 57");
    for e in &samples {
        let s = serde_json::to_string(e).unwrap();
        let back: ServerEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(&back, e, "roundtrip 失败: kind={} json={s}", e.kind());
    }
}

/// 2）全 15 个上行 variant roundtrip。
#[test]
fn client_roundtrip_all_variants() {
    let samples = client_samples();
    assert_eq!(samples.len(), 15, "ClientMessage variant 计数必须为 15");
    for e in &samples {
        let s = serde_json::to_string(e).unwrap();
        let back: ClientEnvelope = serde_json::from_str(&s).unwrap();
        assert_eq!(&back, e, "roundtrip 失败: kind={} json={s}", e.kind());
    }
}

/// 3）`kind()` 输出域与旧白名单逐字一致（无缺失、无多余、计数 57）。
#[test]
fn kind_domain_exactly_matches_whitelist() {
    let got: BTreeSet<&str> = server_samples().iter().map(ServerEnvelope::kind).collect();
    let want: BTreeSet<&str> = VALID_SERVER_MESSAGE_TYPES.iter().copied().collect();
    assert_eq!(got, want, "kind 输出域与白名单不一致");
    // kind() 是序列化 tag 的事实镜像：再与实际 JSON type 字段互验一次。
    for e in &server_samples() {
        let v = serde_json::to_value(e).unwrap();
        assert_eq!(v["type"].as_str(), Some(e.kind()));
    }
}

/// 顶层键集合断言辅助。
fn top_keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

fn keys_of(s: &str) -> BTreeSet<String> {
    top_keys(&serde_json::from_str::<Value>(s).unwrap())
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// 4）激活子集序列化形状断言（14 个 ≥ 10）：验证 U1 扁平平铺——variant 字段
/// 与 type/ts 全部位于顶层，seq/路由字段按需出现。
#[test]
// 形状断言条目数下限为任务书要求的 10+，每条即一个独立断言单元，不拆分。
#[allow(clippy::too_many_lines)]
fn shape_assertions_on_activation_set() {
    let cases: Vec<(ServerEnvelope, BTreeSet<String>)> = vec![
        (
            ServerEnvelope {
                msg: ServerMessage::StreamDelta { delta: "x".into() },
                ts: TS,
                seq: Some(42),
                session_id: Some("s1".into()),
                binding_epoch: Some(3),
            },
            set(&["type", "delta", "ts", "seq", "_sessionId", "_bindingEpoch"]),
        ),
        (
            env(ServerMessage::ThinkingDelta { delta: "y".into() }),
            set(&["type", "delta", "ts"]),
        ),
        (
            env(ServerMessage::MessageComplete {
                usage: Usage::default(),
                stop_reason: Some("end_turn".into()),
                session_id: None,
                run_id: None,
                replace_after_message_id: None,
                committed_messages: None,
            }),
            set(&["type", "usage", "stopReason", "ts"]),
        ),
        (
            env(ServerMessage::MessageComplete {
                usage: Usage::default(),
                stop_reason: None,
                session_id: Some("s1".into()),
                run_id: Some("run-1".into()),
                replace_after_message_id: Some("anchor".into()),
                committed_messages: Some(vec![Message::Assistant {
                    uuid: "a1".into(),
                    timestamp: 4,
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                    stop_reason: None,
                    usage: None,
                }]),
            }),
            set(&[
                "type",
                "usage",
                "sessionId",
                "runId",
                "replaceAfterMessageId",
                "committedMessages",
                "ts",
            ]),
        ),
        (
            env(ServerMessage::Error {
                code: "query_busy".into(),
                message: "busy".into(),
                retryable: false,
            }),
            set(&["type", "code", "message", "retryable", "ts"]),
        ),
        (
            env(ServerMessage::Pong {
                bind_required: None,
                server_now: None,
            }),
            set(&["type", "ts"]),
        ),
        (
            env(ServerMessage::Pong {
                bind_required: Some(true),
                server_now: Some(TS),
            }),
            set(&["type", "bindRequired", "serverNow", "ts"]),
        ),
        (
            env(ServerMessage::SessionRestored {
                messages: vec![],
                metadata: SessionMetadata {
                    session_id: "s1".into(),
                    model: "m".into(),
                    permission_mode: "DEFAULT".into(),
                    status: "idle".into(),
                },
                activities: None,
                total_activity_count: None,
                has_more: None,
                protocol_version: 3,
                bind_request_id: Some("br".into()),
                binding_epoch: Some(1),
                server_now: None,
                run_snapshot: None,
                snapshot_event_seq: None,
                active_tool_calls: None,
                cost_summary: None,
            }),
            set(&[
                "type",
                "messages",
                "metadata",
                "protocolVersion",
                "bindRequestId",
                "bindingEpoch",
                "ts",
            ]),
        ),
        (env(ServerMessage::SessionListUpdated), set(&["type", "ts"])),
        (
            env(ServerMessage::ModelChanged { model: "m".into() }),
            set(&["type", "model", "ts"]),
        ),
        (
            env(ServerMessage::PermissionModeChanged {
                mode: "AUTO_APPROVE".into(),
                previous: Some("DEFAULT".into()),
            }),
            set(&["type", "mode", "previous", "ts"]),
        ),
        (
            env(ServerMessage::ProtocolError {
                code: "BIND_FAILED".into(),
                supported_version: 3,
                bind_request_id: Some("br".into()),
                binding_epoch: Some(1),
            }),
            set(&[
                "type",
                "code",
                "supportedVersion",
                "bindRequestId",
                "bindingEpoch",
                "ts",
            ]),
        ),
        (
            env(ServerMessage::InteractionCreated {
                view: sample_interaction_view(),
            }),
            {
                let mut s = set(&["type", "ts"]);
                for k in [
                    "interactionId",
                    "protocolVersion",
                    "correlationKey",
                    "sessionId",
                    "runId",
                    "interactionType",
                    "status",
                    "prompt",
                    "allowedDecisions",
                    "scopeOptions",
                    "deliveryGeneration",
                    "created_at_placeholder",
                    "decisionDeadlineAt",
                    "deliveryWindowEndsAt",
                    "version",
                    "serverNow",
                ] {
                    if k != "created_at_placeholder" {
                        s.insert(k.to_owned());
                    }
                }
                s.insert("createdAt".to_owned());
                s
            },
        ),
        (
            env(ServerMessage::InteractionUpdated {
                view: InteractionView {
                    interaction_id: "i1".into(),
                    protocol_version: None,
                    correlation_key: None,
                    session_id: None,
                    run_id: None,
                    interaction_type: None,
                    status: None,
                    prompt: None,
                    allowed_decisions: None,
                    scope_options: None,
                    response: None,
                    source: None,
                    child_session_id: None,
                    actor_run_id: None,
                    actor_type: None,
                    delivery_generation: None,
                    dispatch_attempts: None,
                    created_at: None,
                    received_at: None,
                    decision_deadline_at: Some(FlexEpoch::from_millis(TS)),
                    delivery_window_ends_at: None,
                    decided_at: None,
                    terminal_reason: None,
                    version: Some(7),
                    server_now: Some(TS),
                    operation_hash: None,
                    options: None,
                },
            }),
            set(&[
                "type",
                "ts",
                "interactionId",
                "decisionDeadlineAt",
                "serverNow",
                "version",
            ]),
        ),
    ];
    for (e, want) in cases {
        let s = serde_json::to_string(&e).unwrap();
        assert_eq!(
            keys_of(&s),
            want,
            "形状断言失败: kind={} json={s}",
            e.kind()
        );
    }
}

/// 5）真实样例对照（15 条）：`from_str` 成功 + `to_string` 后 JSON 值相等（null 归一化）。
#[test]
fn real_world_samples_parse_and_reemit_equal() {
    let samples: Vec<&str> = vec![
        // dispatch.test.ts L37（messageId 为前端自加字段，不属下行协议，此处按
        // 后端 sendStreamDelta 真实形状 + 路由字段）
        r#"{"type":"stream_delta","ts":1,"delta":"hello","_sessionId":"s1","_bindingEpoch":3}"#,
        r#"{"type":"thinking_delta","ts":1,"delta":"思考中"}"#,
        // dispatch.test.ts L160-164
        r#"{"type":"message_complete","ts":1,"usage":{"inputTokens":100,"outputTokens":50,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"stopReason":"end_turn"}"#,
        // dispatch.test.ts L184-192（replaceAfterMessageId 显式 null → 归一化等价）
        r#"{"type":"message_complete","ts":2,"sessionId":"s1","runId":"run-1","replaceAfterMessageId":"anchor","committedMessages":[{"type":"user","uuid":"saved-user","timestamp":3,"content":[{"type":"text","text":"saved"}]},{"type":"assistant","uuid":"saved-final","timestamp":4,"content":[{"type":"text","text":"done"}],"stopReason":null,"usage":null}],"usage":{"inputTokens":10,"outputTokens":5,"cacheReadInputTokens":0,"cacheCreationInputTokens":0},"stopReason":"end_turn"}"#,
        // dispatch.test.ts L74-77
        r#"{"type":"error","ts":1,"message":"Rate limited","code":"RATE_LIMIT","retryable":true}"#,
        // dispatch.test.ts L48-53（session_restored 恢复门形状）
        r#"{"type":"session_restored","ts":1,"bindRequestId":"br-1","protocolVersion":3,"bindingEpoch":1,"messages":[{"type":"user","uuid":"1","timestamp":1,"content":[{"type":"text","text":"hi"}]}],"metadata":{"sessionId":"s1","model":"gpt-4o","permissionMode":"AUTO_APPROVE","status":"idle"}}"#,
        // dispatch.test.ts L135-140
        r#"{"type":"permission_mode_changed","mode":"AUTO_APPROVE","previous":"DEFAULT","ts":1}"#,
        // dispatch.test.ts L121
        r#"{"type":"model_changed","ts":1,"model":"qwen3.6-plus"}"#,
        // dispatch.test.ts L103-105（usagePercent 按 Jackson double 输出 90.0）
        r#"{"type":"token_warning","ts":1,"currentTokens":180000,"maxTokens":200000,"usagePercent":90.0,"warningLevel":"red"}"#,
        // dispatch.test.ts L91-92 消费形状 + record 必填补全（后端无直推点，
        // Jackson 序列化 record 必含三字段，前端仅消费其关心的子集）。
        r#"{"type":"compact_event","ts":1,"phase":"warning","usagePercent":85,"currentTokens":0}"#,
        // dispatchRecovery.test.ts L37-58（interaction_created 消费形状，时间为数字）
        r#"{"type":"interaction_created","ts":1755000000000,"protocolVersion":3,"interactionId":"permission-ack","correlationKey":"tool-ack","sessionId":"session-ack","runId":"run-ack","interactionType":"permission","status":"pending","prompt":{"toolUseId":"tool-ack","toolName":"Write","inputSummary":"ack.txt","riskLevel":"medium","reason":"test"},"allowedDecisions":["allow","deny"],"scopeOptions":["run","session"],"deliveryGeneration":1,"deliveryWindowEndsAt":1755000060000,"version":1,"serverNow":1755000000000}"#,
        // WebSocketController L1207-1211（interaction_updated ACK 四字段形状）
        r#"{"type":"interaction_updated","ts":1,"interactionId":"i-1","decisionDeadlineAt":1755000120000,"serverNow":1755000000000,"version":7}"#,
        // WebSocketController L1560-1561（pong 未绑定路径）
        r#"{"type":"pong","ts":1,"bindRequired":true,"serverNow":1755000000000}"#,
        // dispatch.test.ts L234-236（run_input_applied；ts 为 push() 注入的顶层字段）
        r#"{"type":"run_input_applied","ts":123,"requestId":"request-1","text":"change direction","appliedAt":123}"#,
        // WebSocketController L361-370（tool_result 嵌套 result 形状）
        r#"{"type":"tool_result","ts":1,"toolUseId":"tu-1","result":{"content":"ok","isError":false,"metadata":{"structuredResult":{"exitCode":0}}}}"#,
    ];
    for raw in samples {
        let parsed: ServerEnvelope = serde_json::from_str(raw).unwrap_or_else(|e| {
            panic!("真实样例解析失败: {e}\nraw={raw}");
        });
        let reemit = serde_json::to_string(&parsed).unwrap();
        assert_eq!(
            normalize(&serde_json::from_str::<Value>(&reemit).unwrap()),
            normalize(&serde_json::from_str::<Value>(raw).unwrap()),
            "值保真失败: kind={} raw={raw} reemit={reemit}",
            parsed.kind()
        );
    }
}

/// 递归剔除值为 null 的键（zkcode 序列化省略 None 键，与显式 null 语义等价）。
fn normalize(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let out: serde_json::Map<String, Value> = map
                .iter()
                .filter(|(_, val)| !val.is_null())
                .map(|(k, val)| (k.clone(), normalize(val)))
                .collect();
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(normalize).collect()),
        other => other.clone(),
    }
}

/// 6）未知 type：反序列化为 Err，且错误信息包含原始字符串。
#[test]
fn unknown_type_is_error_containing_raw_string() {
    let raw = r#"{"type":"definitely_unknown","ts":1}"#;
    let err = serde_json::from_str::<ServerEnvelope>(raw).expect_err("未知 type 必须反序列化失败");
    let msg = err.to_string();
    assert!(
        msg.contains("definitely_unknown"),
        "错误信息须含原始 type 字符串: {msg}"
    );
    // 裸 ServerMessage（无信封）同样拒绝。
    let err2 = serde_json::from_str::<ServerMessage>(r#"{"type":"definitely_unknown"}"#)
        .expect_err("裸 ServerMessage 未知 type 必须失败");
    assert!(err2.to_string().contains("definitely_unknown"));
    // 上行同样拒绝未知 type。
    let err3 = serde_json::from_str::<ClientEnvelope>(r#"{"type":"definitely_unknown"}"#)
        .expect_err("上行未知 type 必须失败");
    assert!(err3.to_string().contains("definitely_unknown"));
    // 错误可装入 ProtocolError（ws 层 WARN+丢弃 的载体契约）。
    let boxed: zk_protocol::ProtocolError = err.into();
    assert!(boxed.to_string().contains("definitely_unknown"));
}

/// 7）激活子集标注一致性：`is_phase1_active` 与 variant 文档标注的激活集一致
/// （13 个：任务书 10 项 + interaction 三兄弟 + `protocol_error`，理由见 variant 文档）。
#[test]
fn phase1_active_kinds() {
    let active: BTreeSet<String> = server_samples()
        .into_iter()
        .filter(|e| e.msg.is_phase1_active())
        .map(|e| e.kind().to_owned())
        .collect();
    let want: BTreeSet<String> = [
        "stream_delta",
        "thinking_delta",
        "message_complete",
        "error",
        "pong",
        "session_restored",
        "session_list_updated",
        "model_changed",
        "permission_mode_changed",
        "protocol_error",
        "interaction_created",
        "interaction_terminal",
        "interaction_updated",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    assert_eq!(active, want);
}

/// A1：图片块新旧两种线上形状的双向 roundtrip——旧 base64 形状
/// （`{mediaType, base64Data}`）与新 url 形状（`{mediaType, url}`）都必须可
/// 反序列化，且序列化时缺省侧的键被省略（对齐旧 Jackson `NON_NULL`）。
#[test]
fn image_block_roundtrips_legacy_base64_and_url_shapes() {
    let legacy = json!({"type": "image", "mediaType": "image/png", "base64Data": "aGk="});
    let block: ContentBlock = serde_json::from_value(legacy.clone()).expect("legacy image");
    assert_eq!(
        block,
        ContentBlock::Image {
            media_type: "image/png".into(),
            base64_data: Some("aGk=".into()),
            url: None,
        }
    );
    assert_eq!(serde_json::to_value(&block).expect("serialize"), legacy);

    let remote = json!({
        "type": "image",
        "mediaType": "image/png",
        "url": "https://bucket.oss.example.com/zhikuncode-artifacts/clipboard/a.png"
    });
    let block: ContentBlock = serde_json::from_value(remote.clone()).expect("url image");
    assert_eq!(
        block,
        ContentBlock::Image {
            media_type: "image/png".into(),
            base64_data: None,
            url: Some("https://bucket.oss.example.com/zhikuncode-artifacts/clipboard/a.png".into()),
        }
    );
    assert_eq!(serde_json::to_value(&block).expect("serialize"), remote);
}
