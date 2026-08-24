-- zkcode 绿地单 schema 基线（单份完整 schema，无过程迁移）。
-- 依据用户裁定（2026-08-15）：zkcode 为全新绿地项目，无历史数据兼容需求，
-- 不保留旧链过程迁移代码；本文件 = 旧链全部迁移（V001→V021，含 git 历史
-- 已删除的 V011/V015_Add/V016×2）累积后的最终态，一步建成。
-- 版本号沿用 2 以保留与旧迁移链的溯源对应（绿地库从 V2 起步）。
--
-- S5b 核查结论（sessions/messages 三方 diff 零差异）：
--   1) 源码层：旧链 19 个迁移类中唯一 CREATE 双表 = V002_InitProjectSchema；
--      其后迁移（V003–V021）对双表仅有外键 REFERENCES 引用，
--      无任何 ALTER/加列/改约束/加索引；
--   2) git 全历史：已删迁移（权限类 V011/V016×2、V015_Add）同样仅外键引用；
--      V008/V009 从未存在（编号跳空）；
--   3) 实证：旧系统 data.db（只读 PRAGMA/sqlite_master）双表逐列比对零差异
--      （sessions 14 列 / messages 9 列 / FK ON DELETE CASCADE /
--       UNIQUE(session_id,seq_num) / 3 个二级索引）。
--
-- Phase 2 子阶段 2.0（D-P2-1，2026-08-16）：基线一次性扩至全量 25 张业务表
-- （§12.8 清单；.tables 另含 refinery_schema_history 与 sqlite_sequence，
-- 共 27 张物理表）。多路核查（方法沿袭 S5b，全文详见
-- docs/phase2-schema-checklist.md）：
--   §12.8 全列清单 ⊕ 旧迁移源码累积态（V001→V021）⊕ 旧系统实际运行库
--   （backend/.ai-code-assistant/data.db + ~/.config/ai-code-assistant/
--   global.db，只读 immutable 打开）三方比对，以旧系统实际最终运行态为准，
--   新增各表 DDL 自运行库 .schema 逐字照抄（仅统一 IF NOT EXISTS 风格与
--   ALTER 增列内联化——evidence_bundles.run_id / memories.source）。
-- 双库并入单库（D6）：旧 global.db 的 memories/projects/auth_tokens 三表
-- 原样并入；global_config 由既有 config KV 表承载（S7b，勿建重复表）。
CREATE TABLE IF NOT EXISTS sessions (
    id                    TEXT PRIMARY KEY,
    title                 TEXT,
    model                 TEXT NOT NULL,
    working_dir           TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'active',
    total_input_tokens    INTEGER DEFAULT 0,
    total_output_tokens   INTEGER DEFAULT 0,
    total_cache_read      INTEGER DEFAULT 0,
    total_cache_create    INTEGER DEFAULT 0,
    total_cost_usd        REAL DEFAULT 0.0,
    summary               TEXT,
    metadata_json         TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_working_dir ON sessions(working_dir);
CREATE TABLE IF NOT EXISTS messages (
    id           TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role         TEXT NOT NULL,
    content_json TEXT NOT NULL,
    stop_reason  TEXT,
    input_tokens  INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    created_at   TEXT NOT NULL,
    seq_num      INTEGER NOT NULL,
    UNIQUE(session_id, seq_num)
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, seq_num);
-- S7b：单行 KV 配置表（旧系统 global.db 的 global_config 表形状照抄，
-- 落入 D6 单库；key='user_config'，value=UserConfig JSON）。依照绿地单
-- schema 原则直接在本基线文件追加最终态表（不加迁移链）；既有开发库
-- 因 refinery checksum 变更需删库重建（裁定见 architecture.md §12）。
CREATE TABLE IF NOT EXISTS config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- ============================================================
-- 以下为 Phase 2 子阶段 2.0 扩充的 22 张表（来源标注 data=旧 data.db /
-- global=旧 global.db；DDL 照抄旧运行库最终态）。
-- ============================================================

-- data：项目级 KV 配置（V002）。与 config（旧 global_config）并存：
-- config=跨项目用户配置，project_config=项目内配置。
CREATE TABLE IF NOT EXISTS project_config (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- data：文件快照（V002 建表；V003/V004 对旧库补 message_id/operation 列，
-- 新库建表语句已含，累积态即此形状）。message_id 为逻辑外键（无 DDL）。
CREATE TABLE IF NOT EXISTS file_snapshots (
    id           TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    message_id   TEXT,
    file_path    TEXT NOT NULL,
    content      BLOB,
    operation    TEXT NOT NULL DEFAULT 'edit',
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_file_snapshots_session ON file_snapshots(session_id, file_path);

-- data：任务（V002）。
CREATE TABLE IF NOT EXISTS tasks (
    id           TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    description  TEXT NOT NULL,
    task_type    TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    output       TEXT,
    error        TEXT,
    progress     REAL DEFAULT 0.0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id, status);

-- data：项目上下文缓存（V003，git 信息/文件树快照）。
CREATE TABLE IF NOT EXISTS project_context (
    id                TEXT PRIMARY KEY,
    working_dir_hash  TEXT NOT NULL UNIQUE,
    snapshot_json     TEXT NOT NULL,
    git_head_sha      TEXT,
    updated_at        TEXT NOT NULL
);

-- data：操作活动（V005，JSON 载荷 10KB 上界+脱敏由应用层保证）。
CREATE TABLE IF NOT EXISTS activities (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    operation_type  TEXT NOT NULL,
    summary         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'completed',
    timestamp       INTEGER NOT NULL,
    duration        INTEGER,
    file_count      INTEGER DEFAULT 0,
    decision        TEXT,
    tool_result_json TEXT,
    changed_files_json TEXT,
    insight_json    TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_activities_session ON activities(session_id, timestamp);

-- data：Swarm/Worker 异常事件（V006；swarm_id/worker_id 为纯运行时 ID，
-- 无库内目标表）。
CREATE TABLE IF NOT EXISTS anomaly_events (
    id                TEXT PRIMARY KEY,
    swarm_id          TEXT NOT NULL,
    worker_id         TEXT NOT NULL,
    rule_id           TEXT NOT NULL,
    severity          TEXT NOT NULL,
    message           TEXT NOT NULL,
    detected_at       INTEGER NOT NULL,
    resolved_at       INTEGER,
    resolution        TEXT,
    context_snapshot  TEXT
);
CREATE INDEX IF NOT EXISTS idx_anomaly_swarm ON anomaly_events(swarm_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_worker ON anomaly_events(worker_id, detected_at);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_timestamp ON anomaly_events(detected_at);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_resolved ON anomaly_events(resolved_at);

-- data：Run 权威模型（V014 V2 重建版，替代 V010；含终态↔terminal_at
-- 一致性表级 CHECK 与父子自引用）。
CREATE TABLE IF NOT EXISTS run_envelopes (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    parent_run_id TEXT REFERENCES run_envelopes(id),
    status TEXT NOT NULL CHECK(status IN ('queued','running','waiting_interaction','cancelling','completed','failed','cancelled','interrupted')),
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    agent_type TEXT,
    model TEXT NOT NULL,
    prompt_hash TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    terminal_at TEXT,
    exit_reason TEXT,
    requested_exit_reason TEXT,
    verification_status TEXT NOT NULL DEFAULT 'not_requested'
        CHECK(verification_status IN ('not_requested','pending','verified','unverified','failed')),
    waiting_reason TEXT,
    abort_reason TEXT,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd REAL NOT NULL DEFAULT 0.0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    turn_count INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK((status IN ('completed','failed','cancelled','interrupted') AND terminal_at IS NOT NULL)
       OR (status NOT IN ('completed','failed','cancelled','interrupted') AND terminal_at IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_run_envelopes_session ON run_envelopes(session_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_run_envelopes_parent ON run_envelopes(parent_run_id);
CREATE INDEX IF NOT EXISTS idx_run_envelopes_status ON run_envelopes(status);

-- data：Run 事件日志（V014 重建；AUTOINCREMENT 触发 sqlite_sequence
-- 内部表创建，计入 27 张物理表）。
CREATE TABLE IF NOT EXISTS run_event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,
    ts INTEGER NOT NULL,
    UNIQUE(run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_run_events_run_seq ON run_event_log(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_run_events_type ON run_event_log(event_type);

-- data：RV-1 证据包（V007 建表；V021 ALTER 增 run_id 列，此处内联为
-- 建表列，累积态一致）。session_id 为逻辑外键（无 DDL）。
CREATE TABLE IF NOT EXISTS evidence_bundles (
    bundle_id    TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL,
    agent_id     TEXT,
    kind         TEXT NOT NULL,
    claim        TEXT,
    verdict      TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    run_id       TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_session ON evidence_bundles(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_run ON evidence_bundles(run_id, created_at DESC);

-- data：证据条目（V007；bundle_id 为逻辑外键）。
CREATE TABLE IF NOT EXISTS evidence_items (
    id           TEXT PRIMARY KEY,
    bundle_id    TEXT NOT NULL,
    type         TEXT NOT NULL,
    summary      TEXT,
    blob_sha256  TEXT,
    meta_json    TEXT,
    sort_order   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_evidence_items_bundle ON evidence_items(bundle_id, sort_order);

-- data：回归脚本（V007；旧系统无运行时读写，仅建表预留——用户裁定
-- 全量基线仍照建）。
CREATE TABLE IF NOT EXISTS regression_scripts (
    script_id      TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL,
    name           TEXT NOT NULL,
    steps_json     TEXT NOT NULL,
    base_url       TEXT,
    start_command  TEXT,
    created_at     TEXT NOT NULL,
    last_verdict   TEXT
);

-- data：子代理检查点（V012；run_id/session_id 为逻辑外键）。
CREATE TABLE IF NOT EXISTS agent_checkpoints (
    id              TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    messages_json   TEXT NOT NULL,
    file_state_json TEXT,
    tool_call_count INTEGER DEFAULT 0,
    turn_count      INTEGER DEFAULT 0,
    tokens_consumed INTEGER DEFAULT 0,
    working_dir     TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_run ON agent_checkpoints(run_id, seq DESC);
CREATE INDEX IF NOT EXISTS idx_checkpoints_agent ON agent_checkpoints(agent_id);

-- data：Run 产物清单（V017 V2 重建版，替代 V013；run_id UNIQUE 一对一）。
CREATE TABLE IF NOT EXISTS artifact_manifests (
    manifest_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE REFERENCES run_envelopes(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    workspace_root TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('open','sealed','verified','partial','failed','unverified')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_manifest_run ON artifact_manifests(run_id);

-- data：产物条目（V017 V2；declared 态 sealed_hash 必空表级 CHECK）。
CREATE TABLE IF NOT EXISTS artifact_entries (
    artifact_id TEXT PRIMARY KEY,
    manifest_id TEXT NOT NULL REFERENCES artifact_manifests(manifest_id) ON DELETE CASCADE,
    tool_use_id TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    operation TEXT NOT NULL CHECK(operation IN ('created','modified','deleted')),
    state TEXT NOT NULL CHECK(state IN ('declared','sealed','integrity_verified','content_verified','unverified','unverified_size_limit','failed')),
    sealed_hash TEXT,
    actual_hash TEXT,
    file_size INTEGER,
    required_validator_id TEXT,
    validator_result_json TEXT,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(manifest_id, canonical_path),
    CHECK((state='declared' AND sealed_hash IS NULL) OR state!='declared')
);
CREATE INDEX IF NOT EXISTS idx_artifact_entries_manifest ON artifact_entries(manifest_id);

-- data：持久化交互请求（V015；投递/决策双窗口，UNIQUE(run_id,
-- correlation_key) 幂等键）。
CREATE TABLE IF NOT EXISTS interaction_requests (
    interaction_id TEXT PRIMARY KEY,
    correlation_key TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK(type IN ('permission','elicitation','plan_approval')),
    status TEXT NOT NULL CHECK(status IN ('pending','answered','denied','expired','cancelled','undeliverable')),
    prompt_json TEXT NOT NULL,
    allowed_decisions_json TEXT NOT NULL,
    scope_options_json TEXT NOT NULL,
    response_json TEXT,
    created_at TEXT NOT NULL,
    delivery_window_ends_at TEXT NOT NULL,
    first_dispatched_at TEXT,
    delivery_ack_deadline_at TEXT,
    received_at TEXT,
    decision_deadline_at TEXT,
    decided_at TEXT,
    terminal_reason TEXT,
    source TEXT NOT NULL,
    child_session_id TEXT,
    delivery_generation INTEGER NOT NULL DEFAULT 0 CHECK(delivery_generation >= 0),
    dispatch_attempts INTEGER NOT NULL DEFAULT 0 CHECK(dispatch_attempts >= 0),
    last_transport_id TEXT,
    authorization_context_json TEXT,
    updated_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    UNIQUE(run_id, correlation_key)
);
CREATE INDEX IF NOT EXISTS idx_interaction_session_status ON interaction_requests(session_id,status,created_at);
CREATE INDEX IF NOT EXISTS idx_interaction_run_status ON interaction_requests(run_id,status);
CREATE INDEX IF NOT EXISTS idx_interaction_delivery ON interaction_requests(status,delivery_window_ends_at);
CREATE INDEX IF NOT EXISTS idx_interaction_decision ON interaction_requests(status,decision_deadline_at);

-- data：WS 重连绑定恢复（V018；last_activity_at 库级默认 datetime('now')
-- 为旧运行态原样保留——应用层写入恒用 6 位微秒 ISO 时间戳覆盖）。
CREATE TABLE IF NOT EXISTS websocket_session_binding (
    principal_name TEXT NOT NULL,
    app_session_id TEXT NOT NULL,
    binding_epoch INTEGER NOT NULL DEFAULT 0,
    last_activity_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (principal_name)
);
CREATE INDEX IF NOT EXISTS idx_ws_binding_session ON websocket_session_binding(app_session_id);

-- data：权限授予（V019；5 个表级 CHECK + 2 个部分唯一索引。注意
-- §12.8 记「4 个表级 CHECK」，实际运行态为 5——多出
-- CHECK(scope != 'WORKSPACE' OR analyzer_id != 'bash-v2')；以运行态为准）。
CREATE TABLE IF NOT EXISTS permission_grants (
    grant_id TEXT PRIMARY KEY,
    grant_kind TEXT NOT NULL CHECK(grant_kind IN ('EXACT_GUARDED','TOOL_GUARDED','READ_CAPABILITY','EDIT_CAPABILITY')),
    scope TEXT NOT NULL CHECK(scope IN ('RUN','SESSION','WORKSPACE')),
    delegation_policy TEXT NOT NULL CHECK(delegation_policy IN ('DIRECT_ONLY','ROOT_AND_DESCENDANTS')),
    root_session_id TEXT REFERENCES sessions(id) ON DELETE CASCADE,
    root_run_id TEXT REFERENCES run_envelopes(id) ON DELETE CASCADE,
    actor_run_id TEXT REFERENCES run_envelopes(id) ON DELETE CASCADE,
    workspace_key TEXT,
    authorization_schema_version INTEGER NOT NULL CHECK(authorization_schema_version=1),
    analyzer_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    action TEXT NOT NULL,
    effects_json TEXT NOT NULL,
    operation_hash TEXT,
    capability_hash TEXT,
    constraints_json TEXT NOT NULL,
    risk_class TEXT NOT NULL CHECK(risk_class IN ('SAFE','GUARDED')),
    created_by_interaction_id TEXT REFERENCES interaction_requests(interaction_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    CHECK((grant_kind='EXACT_GUARDED' AND operation_hash IS NOT NULL AND capability_hash IS NULL
           AND scope IN ('RUN','SESSION'))
       OR (grant_kind='TOOL_GUARDED' AND operation_hash IS NOT NULL AND capability_hash IS NULL
           AND scope IN ('RUN','SESSION'))
       OR (grant_kind IN ('READ_CAPABILITY','EDIT_CAPABILITY')
           AND operation_hash IS NULL AND capability_hash IS NOT NULL)),
    CHECK((scope='RUN' AND root_run_id IS NOT NULL AND root_session_id IS NULL AND workspace_key IS NULL)
       OR (scope='SESSION' AND root_run_id IS NULL AND root_session_id IS NOT NULL AND workspace_key IS NULL)
       OR (scope='WORKSPACE' AND root_run_id IS NULL AND root_session_id IS NULL AND workspace_key IS NOT NULL)),
    CHECK((delegation_policy='DIRECT_ONLY' AND scope='RUN' AND actor_run_id IS NOT NULL)
       OR (delegation_policy='ROOT_AND_DESCENDANTS' AND actor_run_id IS NULL)),
    CHECK(scope != 'WORKSPACE' OR grant_kind IN ('TOOL_GUARDED','READ_CAPABILITY','EDIT_CAPABILITY')),
    CHECK(scope != 'WORKSPACE' OR analyzer_id != 'bash-v2')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_exact_grant ON permission_grants(
    scope,COALESCE(root_run_id,''),COALESCE(root_session_id,''),COALESCE(workspace_key,''),
    COALESCE(actor_run_id,''),delegation_policy,authorization_schema_version,
    analyzer_id,tool_name,action,operation_hash)
WHERE revoked_at IS NULL AND grant_kind='EXACT_GUARDED';
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_capability_grant ON permission_grants(
    grant_kind,scope,COALESCE(root_run_id,''),COALESCE(root_session_id,''),
    COALESCE(workspace_key,''),COALESCE(actor_run_id,''),delegation_policy,
    authorization_schema_version,analyzer_id,tool_name,action,capability_hash)
WHERE revoked_at IS NULL AND grant_kind IN ('READ_CAPABILITY','EDIT_CAPABILITY');
CREATE INDEX IF NOT EXISTS idx_permission_grants_match ON permission_grants(scope,root_session_id,root_run_id,workspace_key,revoked_at,expires_at);

-- data：Root Run ↔ 工作台消息关联（V021）。
CREATE TABLE IF NOT EXISTS run_workbench_bindings (
    root_run_id TEXT PRIMARY KEY REFERENCES run_envelopes(id) ON DELETE CASCADE,
    request_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    result_message_id TEXT REFERENCES messages(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- data：验收条款与证据归属（V021）。
CREATE TABLE IF NOT EXISTS run_acceptance_criteria (
    criterion_id TEXT PRIMARY KEY,
    root_run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    criterion_type TEXT NOT NULL CHECK(criterion_type IN ('business')),
    source_text TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('passed','failed','partial','not_verified')),
    evidence_bundle_id TEXT REFERENCES evidence_bundles(bundle_id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(root_run_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_run_acceptance_root ON run_acceptance_criteria(root_run_id, ordinal);

-- global：长期记忆（V001 建表；V002_AddMemorySource 对旧库补 source 列，
-- 此处内联为建表列，累积态一致）。
CREATE TABLE IF NOT EXISTS memories (
    id           TEXT PRIMARY KEY,
    category     TEXT NOT NULL,
    title        TEXT NOT NULL,
    content      TEXT NOT NULL,
    keywords     TEXT,
    scope        TEXT DEFAULT 'global',
    project_path TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    source       TEXT DEFAULT 'USER'
);
CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories(scope);
CREATE INDEX IF NOT EXISTS idx_memories_category ON memories(category);
CREATE INDEX IF NOT EXISTS idx_memories_source ON memories(source);

-- global：用户批准的项目根目录（V020）。
CREATE TABLE IF NOT EXISTS projects (
    id             TEXT NOT NULL PRIMARY KEY,
    name           TEXT NOT NULL,
    workspace_root TEXT NOT NULL UNIQUE,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_projects_created_at ON projects(created_at DESC, id DESC);

-- global：加密 token 存储（V001；旧系统无运行时读写，仅建表预留——
-- 用户裁定全量基线仍照建。密钥内容由应用层加密后方可落库）。
CREATE TABLE IF NOT EXISTS auth_tokens (
    key             TEXT PRIMARY KEY,
    encrypted_value TEXT NOT NULL,
    expires_at      TEXT
);
