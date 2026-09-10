-- zkcode 绿地最终 schema：单份基线、一步建成、不兼容历史数据。
-- 任何 checksum 变更均通过删除开发数据库后重建处理，不提供回填、双写
-- 或旧状态兼容路径。数据库是 Session/Task/Run/Result/Invocation/Usage
-- 的唯一权威；内存 tracker 与 WebSocket 只能是可丢弃的加速层。
-- This identity row is deliberately committed in the same migration as the
-- complete baseline. Startup rejects non-empty databases without this exact
-- identity, so refinery can never layer the final schema over a legacy DB.
CREATE TABLE zk_schema_metadata (
    singleton                  INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version             INTEGER NOT NULL CHECK(schema_version = 2),
    schema_kind                TEXT NOT NULL CHECK(schema_kind = 'greenfieldFinal'),
    migration_mode             TEXT NOT NULL CHECK(migration_mode = 'greenfieldOnly'),
    legacy_write_compatibility INTEGER NOT NULL DEFAULT 0
                                      CHECK(legacy_write_compatibility = 0)
) WITHOUT ROWID;
INSERT INTO zk_schema_metadata
    (singleton, schema_version, schema_kind, migration_mode, legacy_write_compatibility)
VALUES (1, 2, 'greenfieldFinal', 'greenfieldOnly', 0);

CREATE TABLE IF NOT EXISTS sessions (
    id                    TEXT PRIMARY KEY,
    kind                  TEXT NOT NULL DEFAULT 'root'
                              CHECK(kind IN ('root','internal')),
    parent_session_id     TEXT REFERENCES sessions(id) ON DELETE CASCADE,
    parent_task_id        TEXT REFERENCES tasks(id) ON DELETE CASCADE
                              DEFERRABLE INITIALLY DEFERRED,
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
    updated_at            TEXT NOT NULL,
    CHECK((kind='root' AND parent_session_id IS NULL AND parent_task_id IS NULL)
       OR (kind='internal' AND parent_session_id IS NOT NULL AND parent_task_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_working_dir ON sessions(working_dir);
CREATE INDEX IF NOT EXISTS idx_sessions_parent_task ON sessions(parent_task_id);
CREATE TABLE IF NOT EXISTS messages (
    id           TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role         TEXT NOT NULL,
    content_json TEXT NOT NULL,
    stop_reason  TEXT,
    input_tokens  INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    task_id      TEXT REFERENCES tasks(id) ON DELETE SET NULL
                       DEFERRABLE INITIALLY DEFERRED,
    run_id       TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL
                       DEFERRABLE INITIALLY DEFERRED,
    origin       TEXT NOT NULL DEFAULT 'conversation'
                      CHECK(origin IN ('conversation','tool_result','task_result','runtime')),
    source_task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL
                         DEFERRABLE INITIALLY DEFERRED,
    created_at   TEXT NOT NULL,
    seq_num      INTEGER NOT NULL,
    UNIQUE(session_id, seq_num)
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, seq_num);
CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id, seq_num);
CREATE INDEX IF NOT EXISTS idx_messages_run ON messages(run_id, seq_num);
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

-- 统一 TaskRuntime 逻辑任务。Task 身份跨 Run attempt 保持稳定；
-- output/error 不做可变投影，不可变正文由 task_results 承载。
CREATE TABLE IF NOT EXISTS tasks (
    id                    TEXT PRIMARY KEY,
    session_id            TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    parent_task_id        TEXT REFERENCES tasks(id) ON DELETE CASCADE,
    root_task_id          TEXT NOT NULL REFERENCES tasks(id) DEFERRABLE INITIALLY DEFERRED,
    current_run_id        TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL
                                   DEFERRABLE INITIALLY DEFERRED,
    creator_run_id        TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL
                                   DEFERRABLE INITIALLY DEFERRED,
    creator_tool_use_id   TEXT,
    ordinal               INTEGER NOT NULL DEFAULT 0 CHECK(ordinal >= 0),
    description           TEXT NOT NULL,
    prompt                TEXT,
    task_type             TEXT NOT NULL DEFAULT 'agent'
                               CHECK(task_type IN ('agent','team','swarm','cron')),
    status                TEXT NOT NULL DEFAULT 'queued'
                               CHECK(status IN ('queued','running','waitingDependencies',
                                   'waitingInteraction','cancelling','needsAttention',
                                   'succeeded','partial','failed','cancelled')),
    reason                TEXT,
    plan_json             TEXT,
    execution_config_json TEXT NOT NULL DEFAULT '{}',
    lifecycle_policy      TEXT NOT NULL DEFAULT 'attached'
                               CHECK(lifecycle_policy IN ('attached','detached')),
    reported_progress     REAL NOT NULL DEFAULT 0.0
                               CHECK(reported_progress >= 0.0 AND reported_progress <= 1.0),
    cleanup_status        TEXT NOT NULL DEFAULT 'notRequired'
                               CHECK(cleanup_status IN ('notRequired','pending','confirmed','unconfirmed')),
    verification_status   TEXT NOT NULL DEFAULT 'notRequested'
                               CHECK(verification_status IN ('notRequested','pending','passed',
                                   'failed','stale','blocked')),
    -- Root rows own the hard budget account. Child rows store the allocation granted
    -- from that account, so every executor can enforce its local ceiling without an
    -- in-memory tracker. NULL means that dimension is intentionally unbounded.
    token_budget_limit     INTEGER CHECK(token_budget_limit IS NULL OR token_budget_limit > 0),
    cost_budget_nanos_usd  INTEGER CHECK(cost_budget_nanos_usd IS NULL OR cost_budget_nanos_usd > 0),
    deadline_at_ms         INTEGER CHECK(deadline_at_ms IS NULL OR deadline_at_ms > 0),
    budget_reserved_tokens INTEGER NOT NULL DEFAULT 0 CHECK(budget_reserved_tokens >= 0),
    budget_reserved_cost_nanos_usd INTEGER NOT NULL DEFAULT 0
                                      CHECK(budget_reserved_cost_nanos_usd >= 0),
    budget_consumed_tokens INTEGER NOT NULL DEFAULT 0 CHECK(budget_consumed_tokens >= 0),
    budget_consumed_cost_nanos_usd INTEGER NOT NULL DEFAULT 0
                                      CHECK(budget_consumed_cost_nanos_usd >= 0),
    budget_version         INTEGER NOT NULL DEFAULT 0 CHECK(budget_version >= 0),
    usage_complete        INTEGER NOT NULL DEFAULT 1 CHECK(usage_complete IN (0,1)),
    version               INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    terminal_at           TEXT,
    CHECK(token_budget_limit IS NULL
       OR budget_reserved_tokens + budget_consumed_tokens <= token_budget_limit),
    CHECK(cost_budget_nanos_usd IS NULL
       OR budget_reserved_cost_nanos_usd + budget_consumed_cost_nanos_usd
          <= cost_budget_nanos_usd),
    CHECK((status IN ('succeeded','partial','failed','cancelled') AND terminal_at IS NOT NULL)
       OR (status NOT IN ('succeeded','partial','failed','cancelled') AND terminal_at IS NULL)),
    CHECK((parent_task_id IS NULL AND root_task_id=id)
       OR (parent_task_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_tasks_root_status ON tasks(root_task_id, status, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_submission
    ON tasks(creator_run_id, creator_tool_use_id, ordinal)
    WHERE creator_run_id IS NOT NULL AND creator_tool_use_id IS NOT NULL;

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
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE
                          DEFERRABLE INITIALLY DEFERRED,
    attempt INTEGER NOT NULL DEFAULT 1 CHECK(attempt >= 1),
    startup_epoch INTEGER NOT NULL DEFAULT 0 CHECK(startup_epoch >= 0),
    checkpoint_id TEXT,
    parent_run_id TEXT REFERENCES run_envelopes(id),
    status TEXT NOT NULL CHECK(status IN ('queued','running','waitingDependencies','waitingInteraction','cancelling','completed','failed','cancelled','interrupted')),
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    agent_type TEXT,
    model TEXT NOT NULL,
    prompt_hash TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    terminal_at TEXT,
    exit_reason TEXT CHECK(exit_reason IS NULL OR exit_reason IN
        ('modelFinished','timeout','maxTurns','budgetExhausted','providerError','toolError',
         'userCancelled','parentCancelled','serviceRestart','internalError')),
    requested_exit_reason TEXT CHECK(requested_exit_reason IS NULL OR requested_exit_reason IN
        ('modelFinished','timeout','maxTurns','budgetExhausted','providerError','toolError',
         'userCancelled','parentCancelled','serviceRestart','internalError')),
    verification_status TEXT NOT NULL DEFAULT 'notRequested'
        CHECK(verification_status IN ('notRequested','pending','passed','failed','stale','blocked')),
    cleanup_status TEXT NOT NULL DEFAULT 'notRequired'
        CHECK(cleanup_status IN ('notRequired','pending','confirmed','unconfirmed')),
    waiting_reason TEXT,
    abort_reason TEXT CHECK(abort_reason IS NULL OR abort_reason IN
        ('modelFinished','timeout','maxTurns','budgetExhausted','providerError','toolError',
         'userCancelled','parentCancelled','serviceRestart','internalError')),
    total_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd REAL NOT NULL DEFAULT 0.0,
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens >= 0),
    cache_read_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cache_read_tokens >= 0),
    cache_create_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cache_create_tokens >= 0),
    cost_nanos_usd INTEGER NOT NULL DEFAULT 0 CHECK(cost_nanos_usd >= 0),
    usage_complete INTEGER NOT NULL DEFAULT 1 CHECK(usage_complete IN (0,1)),
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
CREATE UNIQUE INDEX IF NOT EXISTS uq_run_task_attempt ON run_envelopes(task_id, attempt);
CREATE UNIQUE INDEX IF NOT EXISTS uq_run_one_active_per_task ON run_envelopes(task_id)
    WHERE status IN ('queued','running','waitingDependencies','waitingInteraction','cancelling');

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

-- Task 之间的持久依赖。v1 只创建 attached 边，schema 保留
-- detached 类型以便未来审批后开放；required 与 lifecycle 正交。
CREATE TABLE IF NOT EXISTS task_dependencies (
    parent_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    child_task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id) ON DELETE CASCADE,
    lifecycle_policy TEXT NOT NULL DEFAULT 'attached'
        CHECK(lifecycle_policy IN ('attached','detached')),
    required INTEGER NOT NULL DEFAULT 1 CHECK(required IN (0,1)),
    consumed_result_version INTEGER CHECK(consumed_result_version IS NULL OR consumed_result_version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(parent_task_id, child_task_id),
    CHECK(parent_task_id <> child_task_id)
);
CREATE INDEX IF NOT EXISTS idx_task_dependencies_parent
    ON task_dependencies(parent_task_id, child_task_id);

-- One durable allocation per attached child. `active` allocations count against the
-- root account. A terminal child with authoritative usage becomes `settled` and returns
-- only its unused allocation. Missing usage becomes `incomplete`; its allocation stays
-- charged until an operator reconciles it, preventing unsafe budget reuse.
CREATE TABLE IF NOT EXISTS task_budget_reservations (
    child_task_id          TEXT PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    root_task_id           TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    reserved_tokens        INTEGER CHECK(reserved_tokens IS NULL OR reserved_tokens >= 0),
    reserved_cost_nanos_usd INTEGER
                               CHECK(reserved_cost_nanos_usd IS NULL OR reserved_cost_nanos_usd >= 0),
    used_tokens            INTEGER CHECK(used_tokens IS NULL OR used_tokens >= 0),
    used_cost_nanos_usd    INTEGER CHECK(used_cost_nanos_usd IS NULL OR used_cost_nanos_usd >= 0),
    usage_complete         INTEGER NOT NULL DEFAULT 0 CHECK(usage_complete IN (0,1)),
    status                 TEXT NOT NULL DEFAULT 'active'
                               CHECK(status IN ('active','settled','incomplete')),
    version                INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    created_at             TEXT NOT NULL,
    settled_at             TEXT,
    CHECK(child_task_id <> root_task_id),
    CHECK((status='active' AND settled_at IS NULL)
       OR (status IN ('settled','incomplete') AND settled_at IS NOT NULL)),
    CHECK((status='settled' AND usage_complete=1 AND used_tokens IS NOT NULL
            AND used_cost_nanos_usd IS NOT NULL)
       OR status<>'settled')
);
CREATE INDEX IF NOT EXISTS idx_task_budget_reservations_root_status
    ON task_budget_reservations(root_task_id, status, child_task_id);

-- 内容寻址的大结果正文；引用行删除后由仓储做无引用回收。
CREATE TABLE IF NOT EXISTS task_result_blobs (
    sha256 TEXT PRIMARY KEY CHECK(length(sha256)=64),
    payload BLOB NOT NULL,
    byte_len INTEGER NOT NULL CHECK(byte_len >= 0 AND byte_len <= 16777216),
    created_at TEXT NOT NULL,
    CHECK(length(payload)=byte_len)
);

-- 每个 (task_id,result_version) 是不可变结果。64KiB 内联，更大的正文
-- 指向 task_result_blobs；16MiB 硬上限由 CHECK 和仓储 API 共同强制。
CREATE TABLE IF NOT EXISTS task_results (
    result_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    result_version INTEGER NOT NULL CHECK(result_version >= 1),
    status TEXT NOT NULL CHECK(status IN ('complete','partial','error','cancelled')),
    inline_text TEXT,
    blob_sha256 TEXT REFERENCES task_result_blobs(sha256),
    byte_len INTEGER NOT NULL CHECK(byte_len >= 0 AND byte_len <= 16777216),
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
    media_type TEXT NOT NULL DEFAULT 'text/markdown',
    error_code TEXT,
    final_message_id TEXT REFERENCES messages(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    UNIQUE(task_id, result_version),
    CHECK((inline_text IS NOT NULL AND blob_sha256 IS NULL AND length(CAST(inline_text AS BLOB))=byte_len)
       OR (inline_text IS NULL AND blob_sha256 IS NOT NULL)),
    CHECK(status!='complete' OR final_message_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_task_results_task_version
    ON task_results(task_id, result_version DESC);
CREATE INDEX IF NOT EXISTS idx_task_results_run ON task_results(run_id, result_version);

-- 一个 producer result version 对同一 consumer 只能摄取一次。
CREATE TABLE IF NOT EXISTS task_result_receipts (
    receipt_id TEXT PRIMARY KEY,
    consumer_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    producer_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    result_version INTEGER NOT NULL CHECK(result_version >= 1),
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    result_sha256 TEXT NOT NULL CHECK(length(result_sha256)=64),
    created_at TEXT NOT NULL,
    UNIQUE(consumer_task_id, producer_task_id, result_version),
    FOREIGN KEY(producer_task_id, result_version)
        REFERENCES task_results(task_id, result_version) ON DELETE CASCADE,
    CHECK(consumer_task_id <> producer_task_id)
);
CREATE INDEX IF NOT EXISTS idx_task_receipts_producer
    ON task_result_receipts(producer_task_id, result_version);

-- SendMessage 的持久 inbox，不以进程内 mailbox 存活为可用性依据。
CREATE TABLE IF NOT EXISTS task_inbox_messages (
    message_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    target_run_id TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL,
    sender_task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    content TEXT NOT NULL CHECK(length(content) > 0),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK(status IN ('queued','delivered','consumed','rejected')),
    delivery_generation INTEGER NOT NULL DEFAULT 0 CHECK(delivery_generation >= 0),
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    consumed_at TEXT,
    rejection_reason TEXT,
    CHECK((status='queued' AND delivered_at IS NULL AND consumed_at IS NULL)
       OR (status='delivered' AND delivered_at IS NOT NULL AND consumed_at IS NULL)
       OR (status='consumed' AND delivered_at IS NOT NULL AND consumed_at IS NOT NULL)
       OR (status='rejected' AND rejection_reason IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_task_inbox_delivery
    ON task_inbox_messages(task_id, status, created_at);

-- 物理工具调用账本；preparing 表示参数尚未闭合，running 只允许
-- 在完整 input_json 持久化之后进入。
CREATE TABLE IF NOT EXISTS tool_invocations (
    invocation_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    tool_use_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'preparing'
        CHECK(status IN ('preparing','queued','running','succeeded','failed','cancelled','interrupted')),
    input_json TEXT,
    output_ref TEXT,
    error_code TEXT,
    side_effect_class TEXT NOT NULL DEFAULT 'unknown'
        CHECK(side_effect_class IN ('none','read','write','unknown')),
    cleanup_status TEXT NOT NULL DEFAULT 'notRequired'
        CHECK(cleanup_status IN ('notRequired','pending','confirmed','unconfirmed')),
    directory_generation INTEGER CHECK(directory_generation IS NULL OR directory_generation >= 0),
    connection_generation INTEGER CHECK(connection_generation IS NULL OR connection_generation >= 0),
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    started_at TEXT,
    terminal_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, tool_use_id),
    CHECK(status NOT IN ('queued','running','succeeded') OR input_json IS NOT NULL),
    CHECK((status IN ('succeeded','failed','cancelled','interrupted') AND terminal_at IS NOT NULL)
       OR (status NOT IN ('succeeded','failed','cancelled','interrupted') AND terminal_at IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_run
    ON tool_invocations(run_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_active
    ON tool_invocations(task_id, status)
    WHERE status IN ('preparing','queued','running');

-- A terminal tool_result may require durable projections (Artifact, Research,
-- Evidence).  The obligation is created in the same transaction as the
-- terminal invocation and immutable message.  A crash can therefore never
-- make those projections silently optional: task completion and parent-result
-- ingestion fail closed until this row reaches completed.
CREATE TABLE IF NOT EXISTS tool_result_postprocessing (
    invocation_id TEXT PRIMARY KEY
        REFERENCES tool_invocations(invocation_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    result_message_id TEXT NOT NULL UNIQUE REFERENCES messages(id) ON DELETE CASCADE,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','completed')),
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    CHECK((status='pending' AND completed_at IS NULL)
       OR (status='completed' AND completed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_tool_result_postprocessing_pending
    ON tool_result_postprocessing(task_id, run_id, status)
    WHERE status='pending';

-- Research quality ledger. One successful WebSearch/WebFetch invocation owns
-- exactly one immutable capture. The capture hash makes retries idempotent and
-- rejects a producer attempting to rewrite its observed result.
CREATE TABLE IF NOT EXISTS research_captures (
    producer_invocation_id TEXT PRIMARY KEY CHECK(length(producer_invocation_id)=36)
        REFERENCES tool_invocations(invocation_id) ON DELETE CASCADE,
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    capture_kind TEXT NOT NULL CHECK(capture_kind IN ('webSearch','webFetch')),
    query TEXT CHECK(query IS NULL OR
        length(CAST(query AS BLOB)) BETWEEN 1 AND 4096),
    fetched_at TEXT NOT NULL CHECK(length(fetched_at) BETWEEN 1 AND 64),
    receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
    created_at TEXT NOT NULL,
    CHECK((capture_kind='webSearch' AND query IS NOT NULL)
       OR (capture_kind='webFetch' AND query IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_research_captures_root
    ON research_captures(root_task_id, fetched_at, producer_invocation_id);

CREATE TABLE IF NOT EXISTS research_sources (
    source_id TEXT PRIMARY KEY CHECK(length(source_id)=36),
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    producer_invocation_id TEXT NOT NULL
        REFERENCES research_captures(producer_invocation_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 9),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('searchResult','fetchedPage')),
    url TEXT NOT NULL CHECK(length(CAST(url AS BLOB)) BETWEEN 1 AND 4096),
    title TEXT CHECK(title IS NULL OR length(CAST(title AS BLOB)) <= 4096),
    provider TEXT CHECK(provider IS NULL OR length(CAST(provider AS BLOB)) <= 512),
    fetched_at TEXT NOT NULL CHECK(length(fetched_at) BETWEEN 1 AND 64),
    http_status INTEGER CHECK(http_status IS NULL OR http_status BETWEEN 100 AND 599),
    content_type TEXT CHECK(content_type IS NULL OR
        length(CAST(content_type AS BLOB)) <= 256),
    truncated INTEGER NOT NULL DEFAULT 0 CHECK(truncated IN (0,1)),
    created_at TEXT NOT NULL,
    UNIQUE(producer_invocation_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_research_sources_root
    ON research_sources(root_task_id, fetched_at, source_id);

-- Findings are bounded excerpts/citations, not page archives. Full WebFetch
-- content remains only in the ordinary tool result / result-blob lifecycle.
CREATE TABLE IF NOT EXISTS research_findings (
    finding_id TEXT PRIMARY KEY CHECK(length(finding_id)=36),
    source_id TEXT NOT NULL REFERENCES research_sources(source_id) ON DELETE CASCADE,
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    producer_invocation_id TEXT NOT NULL
        REFERENCES research_captures(producer_invocation_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 9),
    finding_kind TEXT NOT NULL
        CHECK(finding_kind IN ('searchSnippet','fetchExcerpt','citation','claim')),
    excerpt TEXT NOT NULL CHECK(length(CAST(excerpt AS BLOB)) BETWEEN 1 AND 8192),
    rank INTEGER CHECK(rank IS NULL OR rank BETWEEN 1 AND 10),
    created_at TEXT NOT NULL,
    UNIQUE(producer_invocation_id, ordinal, finding_kind)
);
CREATE INDEX IF NOT EXISTS idx_research_findings_root
    ON research_findings(root_task_id, created_at, finding_id);

CREATE TABLE IF NOT EXISTS research_conflicts (
    conflict_id TEXT PRIMARY KEY CHECK(length(conflict_id)=36),
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    left_finding_id TEXT REFERENCES research_findings(finding_id) ON DELETE SET NULL,
    right_finding_id TEXT REFERENCES research_findings(finding_id) ON DELETE SET NULL,
    summary TEXT NOT NULL CHECK(length(CAST(summary AS BLOB)) BETWEEN 1 AND 8192),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','resolved','dismissed')),
    resolution TEXT CHECK(resolution IS NULL OR length(CAST(resolution AS BLOB)) <= 8192),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_conflicts_root
    ON research_conflicts(root_task_id, status, created_at);

CREATE TABLE IF NOT EXISTS research_open_questions (
    question_id TEXT PRIMARY KEY CHECK(length(question_id)=36),
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    run_id TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL,
    question TEXT NOT NULL CHECK(length(CAST(question AS BLOB)) BETWEEN 1 AND 4096),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','resolved','blocked')),
    resolution TEXT CHECK(resolution IS NULL OR length(CAST(resolution AS BLOB)) <= 8192),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_questions_root
    ON research_open_questions(root_task_id, status, created_at);

CREATE TABLE IF NOT EXISTS research_requirement_coverage (
    coverage_id TEXT PRIMARY KEY CHECK(length(coverage_id)=36),
    root_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    requirement_key TEXT NOT NULL
        CHECK(length(CAST(requirement_key AS BLOB)) BETWEEN 1 AND 256),
    requirement_text TEXT NOT NULL
        CHECK(length(CAST(requirement_text AS BLOB)) BETWEEN 1 AND 8192),
    status TEXT NOT NULL DEFAULT 'uncovered'
        CHECK(status IN ('uncovered','partial','covered','blocked')),
    supporting_finding_id TEXT REFERENCES research_findings(finding_id) ON DELETE SET NULL,
    notes TEXT CHECK(notes IS NULL OR length(CAST(notes AS BLOB)) <= 8192),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(root_task_id, requirement_key)
);
CREATE INDEX IF NOT EXISTS idx_research_coverage_root
    ON research_requirement_coverage(root_task_id, status, requirement_key);

CREATE TRIGGER IF NOT EXISTS trg_research_capture_producer_insert
BEFORE INSERT ON research_captures
WHEN NOT EXISTS(
    SELECT 1 FROM tool_invocations invocation
    JOIN run_envelopes run ON run.id=invocation.run_id
    JOIN tasks task ON task.id=invocation.task_id
    WHERE invocation.invocation_id=NEW.producer_invocation_id
      AND invocation.status='succeeded'
      AND invocation.side_effect_class='read'
      AND invocation.tool_name IN ('WebSearch','WebFetch')
      AND invocation.task_id=NEW.task_id
      AND invocation.run_id=NEW.run_id
      AND run.task_id=NEW.task_id
      AND task.root_task_id=NEW.root_task_id
      AND ((invocation.tool_name='WebSearch' AND NEW.capture_kind='webSearch')
        OR (invocation.tool_name='WebFetch' AND NEW.capture_kind='webFetch'))
)
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_PRODUCER_INVOCATION_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_captures_immutable
BEFORE UPDATE ON research_captures
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_CAPTURE_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_source_owner_insert
BEFORE INSERT ON research_sources
WHEN NOT EXISTS(
    SELECT 1 FROM research_captures capture
    WHERE capture.producer_invocation_id=NEW.producer_invocation_id
      AND capture.root_task_id=NEW.root_task_id
      AND capture.task_id=NEW.task_id
      AND capture.run_id=NEW.run_id
      AND ((capture.capture_kind='webSearch' AND NEW.source_kind='searchResult')
        OR (capture.capture_kind='webFetch' AND NEW.source_kind='fetchedPage'))
)
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_SOURCE_OWNER_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_sources_immutable
BEFORE UPDATE ON research_sources
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_SOURCE_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_finding_owner_insert
BEFORE INSERT ON research_findings
WHEN NOT EXISTS(
    SELECT 1 FROM research_sources source
    WHERE source.source_id=NEW.source_id
      AND source.root_task_id=NEW.root_task_id
      AND source.task_id=NEW.task_id
      AND source.run_id=NEW.run_id
      AND source.producer_invocation_id=NEW.producer_invocation_id
      AND source.ordinal=NEW.ordinal
      AND ((source.source_kind='searchResult' AND NEW.finding_kind IN ('searchSnippet','citation','claim'))
        OR (source.source_kind='fetchedPage' AND NEW.finding_kind IN ('fetchExcerpt','citation','claim')))
)
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_FINDING_OWNER_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_findings_immutable
BEFORE UPDATE ON research_findings
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_FINDING_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_conflict_root_insert
BEFORE INSERT ON research_conflicts
WHEN NOT EXISTS(SELECT 1 FROM tasks task
                WHERE task.id=NEW.root_task_id AND task.root_task_id=task.id
                  AND task.parent_task_id IS NULL)
  OR (NEW.left_finding_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM research_findings finding
        WHERE finding.finding_id=NEW.left_finding_id
          AND finding.root_task_id=NEW.root_task_id))
  OR (NEW.right_finding_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM research_findings finding
        WHERE finding.finding_id=NEW.right_finding_id
          AND finding.root_task_id=NEW.root_task_id))
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_ROOT_TASK_INVALID');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_question_root_insert
BEFORE INSERT ON research_open_questions
WHEN NOT EXISTS(SELECT 1 FROM tasks task
                WHERE task.id=NEW.root_task_id AND task.root_task_id=task.id
                  AND task.parent_task_id IS NULL)
  OR (NEW.task_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM tasks task
        WHERE task.id=NEW.task_id AND task.root_task_id=NEW.root_task_id))
  OR (NEW.run_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM run_envelopes run JOIN tasks task ON task.id=run.task_id
        WHERE run.id=NEW.run_id AND task.root_task_id=NEW.root_task_id
          AND (NEW.task_id IS NULL OR NEW.task_id=task.id)))
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_ROOT_TASK_INVALID');
END;

CREATE TRIGGER IF NOT EXISTS trg_research_coverage_root_insert
BEFORE INSERT ON research_requirement_coverage
WHEN NOT EXISTS(SELECT 1 FROM tasks task
                WHERE task.id=NEW.root_task_id AND task.root_task_id=task.id
                  AND task.parent_task_id IS NULL)
  OR (NEW.supporting_finding_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM research_findings finding
        WHERE finding.finding_id=NEW.supporting_finding_id
          AND finding.root_task_id=NEW.root_task_id))
BEGIN
    SELECT RAISE(ABORT,'RESEARCH_ROOT_TASK_INVALID');
END;

-- Supervisor 拥有的外部资源。只有 released 才算已证实清理，
-- unconfirmed 是显式终态，不伪装为成功。
CREATE TABLE IF NOT EXISTS execution_resources (
    resource_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    invocation_id TEXT REFERENCES tool_invocations(invocation_id) ON DELETE CASCADE,
    resource_kind TEXT NOT NULL
        CHECK(resource_kind IN ('process','processGroup','worktree','workspaceLease','stream')),
    external_id TEXT,
    status TEXT NOT NULL DEFAULT 'allocated'
        CHECK(status IN ('allocated','stopping','released','unconfirmed')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    released_at TEXT,
    CHECK((status='released' AND released_at IS NOT NULL)
       OR (status!='released' AND released_at IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_execution_resources_active
    ON execution_resources(run_id, status)
    WHERE status IN ('allocated','stopping');

-- 每次物理 Provider 请求一行；usage 未知时保持 NULL 并显式
-- usage_complete=0，禁止用 0 伪造完整账本。
CREATE TABLE IF NOT EXISTS llm_calls (
    call_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES run_envelopes(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    route TEXT,
    provider_request_id TEXT,
    status TEXT NOT NULL DEFAULT 'started'
        CHECK(status IN ('started','completed','failed','cancelled')),
    input_tokens INTEGER CHECK(input_tokens IS NULL OR input_tokens >= 0),
    output_tokens INTEGER CHECK(output_tokens IS NULL OR output_tokens >= 0),
    cache_read_tokens INTEGER CHECK(cache_read_tokens IS NULL OR cache_read_tokens >= 0),
    cache_create_tokens INTEGER CHECK(cache_create_tokens IS NULL OR cache_create_tokens >= 0),
    cost_nanos_usd INTEGER CHECK(cost_nanos_usd IS NULL OR cost_nanos_usd >= 0),
    -- Worst-case admission reserved before this physical stream is polled. Terminal
    -- rows retain the values for audit; only status='started' rows count as active.
    reserved_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(reserved_input_tokens >= 0),
    reserved_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(reserved_output_tokens >= 0),
    reserved_cost_nanos_usd INTEGER NOT NULL DEFAULT 0 CHECK(reserved_cost_nanos_usd >= 0),
    usage_complete INTEGER NOT NULL DEFAULT 0 CHECK(usage_complete IN (0,1)),
    error_code TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK((status='started' AND finished_at IS NULL)
       OR (status!='started' AND finished_at IS NOT NULL)),
    CHECK(usage_complete=0 OR
        (input_tokens IS NOT NULL AND output_tokens IS NOT NULL
         AND cache_read_tokens IS NOT NULL AND cache_create_tokens IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_llm_provider_request
    ON llm_calls(provider, provider_request_id)
    WHERE provider_request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id, started_at);

-- Cross-table invariants that cannot be expressed as column CHECK constraints.
-- `serviceRestart` is an execution interruption, never an error result.  The
-- sole legal projection is the post-drain restart reconciler, which leaves the
-- logical Task in needsAttention without fabricating output.
CREATE TRIGGER IF NOT EXISTS trg_service_restart_result_forbidden
BEFORE INSERT ON task_results
WHEN EXISTS(
    SELECT 1 FROM run_envelopes run
     WHERE run.id=NEW.run_id
       AND run.task_id=NEW.task_id
       AND run.requested_exit_reason='serviceRestart'
)
BEGIN
    SELECT RAISE(ABORT,'SERVICE_RESTART_REQUIRES_RECONCILIATION');
END;

CREATE TRIGGER IF NOT EXISTS trg_tasks_terminal_immutable
BEFORE UPDATE OF status ON tasks
WHEN OLD.status IN ('succeeded','partial','failed','cancelled') AND NEW.status <> OLD.status
BEGIN
    SELECT RAISE(ABORT,'TASK_TERMINAL_IMMUTABLE');
END;

-- A Task can only become terminal through the commit_task_result transaction.
-- A terminal INSERT would necessarily precede its Task-owned Run/Result because
-- both rows reference the Task, so reject that construction outright.
CREATE TRIGGER IF NOT EXISTS trg_task_terminal_insert_forbidden
BEFORE INSERT ON tasks
WHEN NEW.status IN ('succeeded','partial','failed','cancelled')
BEGIN
    SELECT RAISE(ABORT,'TASK_TERMINAL_INSERT_FORBIDDEN');
END;

-- commit_task_result inserts the immutable result, terminalizes its owning Run,
-- then enters the Task terminal state. No other write ordering can satisfy this
-- exact Task/current-Run/result-status mapping.
CREATE TRIGGER IF NOT EXISTS trg_task_terminal_requires_current_run_result
BEFORE UPDATE OF status ON tasks
WHEN OLD.status NOT IN ('succeeded','partial','failed','cancelled')
 AND NEW.status IN ('succeeded','partial','failed','cancelled')
 AND NOT EXISTS(
    SELECT 1
      FROM run_envelopes run
      JOIN task_results result
        ON result.task_id=NEW.id AND result.run_id=run.id
     WHERE run.id=NEW.current_run_id
       AND run.task_id=NEW.id
       AND ((NEW.status='succeeded' AND run.status='completed' AND result.status='complete')
         OR (NEW.status='partial' AND run.status='completed' AND result.status='partial')
         OR (NEW.status='failed' AND run.status='failed' AND result.status='error')
         OR (NEW.status='cancelled' AND run.status='cancelled' AND result.status='cancelled'))
 )
BEGIN
    SELECT RAISE(ABORT,'TASK_TERMINAL_REQUIRES_CURRENT_RUN_RESULT');
END;

CREATE TRIGGER IF NOT EXISTS trg_runs_terminal_immutable
BEFORE UPDATE OF status ON run_envelopes
WHEN OLD.status IN ('completed','failed','cancelled','interrupted') AND NEW.status <> OLD.status
BEGIN
    SELECT RAISE(ABORT,'RUN_TERMINAL_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_run_task_session_insert
BEFORE INSERT ON run_envelopes
WHEN NOT EXISTS(
    SELECT 1 FROM tasks t JOIN sessions s ON s.id=NEW.session_id
    WHERE t.id=NEW.task_id AND (
        (s.kind='root' AND s.id=t.session_id)
        OR (s.kind='internal' AND s.parent_session_id=t.session_id AND s.parent_task_id=t.id)
    )
)
BEGIN
    SELECT RAISE(ABORT,'RUN_TASK_SESSION_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_internal_session_owner_insert
BEFORE INSERT ON sessions
WHEN NEW.kind='internal' AND NOT EXISTS(
    SELECT 1 FROM tasks t
    WHERE t.id=NEW.parent_task_id AND t.session_id=NEW.parent_session_id
)
BEGIN
    SELECT RAISE(ABORT,'INTERNAL_SESSION_OWNER_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_parent_tree_insert
BEFORE INSERT ON tasks
WHEN NEW.parent_task_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM tasks p
    WHERE p.id=NEW.parent_task_id AND p.session_id=NEW.session_id
      AND p.root_task_id=NEW.root_task_id
)
BEGIN
    SELECT RAISE(ABORT,'TASK_PARENT_TREE_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_dependency_identity_insert
BEFORE INSERT ON task_dependencies
WHEN NOT EXISTS(
    SELECT 1 FROM tasks c
    WHERE c.id=NEW.child_task_id AND c.parent_task_id=NEW.parent_task_id
)
BEGIN
    SELECT RAISE(ABORT,'TASK_DEPENDENCY_IDENTITY_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_budget_reservation_identity_insert
BEFORE INSERT ON task_budget_reservations
WHEN NOT EXISTS(
    SELECT 1 FROM tasks c JOIN tasks r ON r.id=NEW.root_task_id
    WHERE c.id=NEW.child_task_id
      AND c.parent_task_id IS NOT NULL
      AND c.root_task_id=NEW.root_task_id
      AND r.parent_task_id IS NULL
      AND r.root_task_id=r.id
)
BEGIN
    SELECT RAISE(ABORT,'TASK_BUDGET_RESERVATION_IDENTITY_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_budget_reservation_identity_immutable
BEFORE UPDATE OF child_task_id,root_task_id,reserved_tokens,reserved_cost_nanos_usd
ON task_budget_reservations
WHEN NEW.child_task_id <> OLD.child_task_id
  OR NEW.root_task_id <> OLD.root_task_id
  OR NEW.reserved_tokens IS NOT OLD.reserved_tokens
  OR NEW.reserved_cost_nanos_usd IS NOT OLD.reserved_cost_nanos_usd
BEGIN
    SELECT RAISE(ABORT,'TASK_BUDGET_RESERVATION_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_result_run_owner_insert
BEFORE INSERT ON task_results
WHEN NOT EXISTS(
    SELECT 1 FROM run_envelopes r
    WHERE r.id=NEW.run_id AND r.task_id=NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT,'TASK_RESULT_RUN_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_result_final_message_insert
BEFORE INSERT ON task_results
WHEN NEW.final_message_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM messages m
    WHERE m.id=NEW.final_message_id
      AND m.role='assistant'
      AND m.task_id=NEW.task_id
      AND m.run_id=NEW.run_id
)
BEGIN
    SELECT RAISE(ABORT,'TASK_RESULT_FINAL_MESSAGE_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_results_immutable
BEFORE UPDATE ON task_results
BEGIN
    SELECT RAISE(ABORT,'TASK_RESULT_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_task_result_blob_gc
AFTER DELETE ON task_results
WHEN OLD.blob_sha256 IS NOT NULL
BEGIN
    DELETE FROM task_result_blobs
     WHERE sha256=OLD.blob_sha256
       AND NOT EXISTS(SELECT 1 FROM task_results WHERE blob_sha256=OLD.blob_sha256);
END;

CREATE TRIGGER IF NOT EXISTS trg_tool_invocation_owner_insert
BEFORE INSERT ON tool_invocations
WHEN NOT EXISTS(
    SELECT 1 FROM run_envelopes r
    WHERE r.id=NEW.run_id AND r.task_id=NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT,'TOOL_INVOCATION_RUN_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_execution_resource_owner_insert
BEFORE INSERT ON execution_resources
WHEN NOT EXISTS(
    SELECT 1 FROM run_envelopes r
    WHERE r.id=NEW.run_id AND r.task_id=NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT,'EXECUTION_RESOURCE_RUN_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_llm_call_owner_insert
BEFORE INSERT ON llm_calls
WHEN NOT EXISTS(
    SELECT 1 FROM run_envelopes r
    WHERE r.id=NEW.run_id AND r.task_id=NEW.task_id
)
BEGIN
    SELECT RAISE(ABORT,'LLM_CALL_RUN_MISMATCH');
END;

-- data：RV-1 证据包（V007 建表；V021 ALTER 增 run_id 列，此处内联为
-- 建表列，累积态一致）。session_id 为逻辑外键（无 DDL）。
CREATE TABLE IF NOT EXISTS evidence_bundles (
    bundle_id    TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL,
    agent_id     TEXT,
    kind         TEXT NOT NULL,
    claim        TEXT,
    origin       TEXT NOT NULL CHECK(origin IN ('machine','modelAssertion','human')),
    producer_invocation_id TEXT REFERENCES tool_invocations(invocation_id) ON DELETE RESTRICT,
    verdict      TEXT NOT NULL CHECK(verdict IN
        ('pending','verified','failed','inconclusive','unavailable','stale')),
    created_at   TEXT NOT NULL,
    run_id       TEXT REFERENCES run_envelopes(id) ON DELETE SET NULL,
    CHECK(origin!='modelAssertion' OR verdict IN ('pending','inconclusive')),
    CHECK(producer_invocation_id IS NULL OR run_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_session ON evidence_bundles(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_run ON evidence_bundles(run_id, created_at DESC);

CREATE TRIGGER IF NOT EXISTS trg_evidence_bundles_update_immutable
BEFORE UPDATE ON evidence_bundles
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_BUNDLE_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_bundles_delete_immutable
BEFORE DELETE ON evidence_bundles
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_BUNDLE_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_invocation_owner_insert
BEFORE INSERT ON evidence_bundles
WHEN NEW.producer_invocation_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM tool_invocations invocation
    JOIN run_envelopes run ON run.id=invocation.run_id
    WHERE invocation.invocation_id=NEW.producer_invocation_id
      AND invocation.run_id=NEW.run_id
      AND run.session_id=NEW.session_id
      AND invocation.status='succeeded'
)
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_PRODUCER_INVOCATION_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_machine_evidence_commit_gate_insert
BEFORE INSERT ON evidence_bundles
WHEN NEW.origin='machine' AND NEW.verdict IN ('verified','failed') AND (
    NEW.run_id IS NULL OR NEW.producer_invocation_id IS NULL OR NOT EXISTS(
        SELECT 1 FROM tool_invocations invocation
        JOIN run_envelopes run ON run.id=invocation.run_id
        WHERE invocation.invocation_id=NEW.producer_invocation_id
          AND invocation.run_id=NEW.run_id
          AND run.session_id=NEW.session_id
          AND invocation.status='succeeded'
    )
)
BEGIN
    SELECT RAISE(ABORT,'MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_invocation_owner_update
BEFORE UPDATE OF producer_invocation_id,run_id,origin,verdict ON evidence_bundles
WHEN NEW.producer_invocation_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM tool_invocations invocation
    JOIN run_envelopes run ON run.id=invocation.run_id
    WHERE invocation.invocation_id=NEW.producer_invocation_id
      AND invocation.run_id=NEW.run_id
      AND run.session_id=NEW.session_id
      AND invocation.status='succeeded'
)
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_PRODUCER_INVOCATION_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_machine_evidence_commit_gate_update
BEFORE UPDATE OF producer_invocation_id,run_id,origin,verdict ON evidence_bundles
WHEN NEW.origin='machine' AND NEW.verdict IN ('verified','failed') AND (
    NEW.run_id IS NULL OR NEW.producer_invocation_id IS NULL OR NOT EXISTS(
        SELECT 1 FROM tool_invocations invocation
        JOIN run_envelopes run ON run.id=invocation.run_id
        WHERE invocation.invocation_id=NEW.producer_invocation_id
          AND invocation.run_id=NEW.run_id
          AND run.session_id=NEW.session_id
          AND invocation.status='succeeded'
    )
)
BEGIN
    SELECT RAISE(ABORT,'MACHINE_EVIDENCE_REQUIRES_SUCCEEDED_INVOCATION');
END;

-- data：证据条目（V007；bundle_id 为逻辑外键）。
CREATE TABLE IF NOT EXISTS evidence_items (
    id           TEXT PRIMARY KEY,
    bundle_id    TEXT NOT NULL REFERENCES evidence_bundles(bundle_id) ON DELETE CASCADE,
    producer_invocation_id TEXT REFERENCES tool_invocations(invocation_id) ON DELETE RESTRICT,
    type         TEXT NOT NULL,
    summary      TEXT,
    blob_sha256  TEXT,
    meta_json    TEXT,
    sort_order   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_evidence_items_bundle ON evidence_items(bundle_id, sort_order);

CREATE TRIGGER IF NOT EXISTS trg_evidence_item_producer_insert
BEFORE INSERT ON evidence_items
WHEN NEW.producer_invocation_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM evidence_bundles bundle
    JOIN tool_invocations invocation
      ON invocation.invocation_id=NEW.producer_invocation_id
    WHERE bundle.bundle_id=NEW.bundle_id
      AND bundle.run_id=invocation.run_id
      AND invocation.status='succeeded'
)
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_ITEM_PRODUCER_INVOCATION_MISMATCH');
END;

CREATE TRIGGER IF NOT EXISTS trg_machine_evidence_item_commit_gate_insert
BEFORE INSERT ON evidence_items
WHEN NEW.producer_invocation_id IS NULL AND EXISTS(
    SELECT 1 FROM evidence_bundles bundle
    WHERE bundle.bundle_id=NEW.bundle_id
      AND bundle.origin='machine'
      AND bundle.verdict IN ('verified','failed')
)
BEGIN
    SELECT RAISE(ABORT,'MACHINE_EVIDENCE_ITEM_REQUIRES_SUCCEEDED_INVOCATION');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_items_immutable
BEFORE UPDATE ON evidence_items
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_ITEM_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_items_delete_immutable
BEFORE DELETE ON evidence_items
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_ITEM_IMMUTABLE');
END;

-- Verification conclusions evolve without mutating the observation that
-- produced them.  Each event names the exact event it supersedes and carries
-- both the actor (`origin`) and the projected evidence origin.  Version is a
-- bundle-local monotonic sequence and is the authoritative read projection.
CREATE TABLE IF NOT EXISTS evidence_verdict_events (
    event_id          TEXT PRIMARY KEY,
    bundle_id         TEXT NOT NULL REFERENCES evidence_bundles(bundle_id) ON DELETE RESTRICT,
    version           INTEGER NOT NULL CHECK(version > 0),
    supersedes_event_id TEXT REFERENCES evidence_verdict_events(event_id) ON DELETE RESTRICT,
    verdict           TEXT NOT NULL CHECK(verdict IN
        ('pending','verified','failed','inconclusive','unavailable','stale')),
    origin            TEXT NOT NULL CHECK(origin IN ('human','artifactIntegrity','machine','system')),
    effective_origin  TEXT NOT NULL CHECK(effective_origin IN ('machine','modelAssertion','human')),
    reason            TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    UNIQUE(bundle_id, version)
);
CREATE INDEX IF NOT EXISTS idx_evidence_verdict_events_latest
    ON evidence_verdict_events(bundle_id, version DESC);

CREATE TRIGGER IF NOT EXISTS trg_evidence_verdict_events_chain
BEFORE INSERT ON evidence_verdict_events
WHEN (
    (NEW.version=1 AND NEW.supersedes_event_id IS NOT NULL)
    OR (NEW.version>1 AND NOT EXISTS(
        SELECT 1 FROM evidence_verdict_events previous
        WHERE previous.event_id=NEW.supersedes_event_id
          AND previous.bundle_id=NEW.bundle_id
          AND previous.version=NEW.version-1
    ))
)
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_VERDICT_EVENT_CHAIN_INVALID');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_verdict_events_update_immutable
BEFORE UPDATE ON evidence_verdict_events
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_VERDICT_EVENT_IMMUTABLE');
END;

CREATE TRIGGER IF NOT EXISTS trg_evidence_verdict_events_delete_immutable
BEFORE DELETE ON evidence_verdict_events
BEGIN
    SELECT RAISE(ABORT,'EVIDENCE_VERDICT_EVENT_IMMUTABLE');
END;

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
    producer_invocation_id TEXT REFERENCES tool_invocations(invocation_id) ON DELETE RESTRICT,
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

CREATE TRIGGER IF NOT EXISTS trg_artifact_invocation_owner_insert
BEFORE INSERT ON artifact_entries
WHEN NEW.producer_invocation_id IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM tool_invocations invocation
    JOIN artifact_manifests manifest ON manifest.manifest_id=NEW.manifest_id
    WHERE invocation.invocation_id=NEW.producer_invocation_id
      AND invocation.run_id=manifest.run_id
      AND invocation.tool_use_id=NEW.tool_use_id
      AND invocation.status='succeeded'
      AND invocation.side_effect_class IN ('write','unknown')
)
BEGIN
    SELECT RAISE(ABORT,'ARTIFACT_PRODUCER_INVOCATION_MISMATCH');
END;

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

-- data：长期记忆。SQLite 是唯一权威；项目作用域是默认且必须携带项目路径，
-- global 作用域只能由调用方显式选择且不得携带项目路径。
CREATE TABLE IF NOT EXISTS memories (
    id           TEXT PRIMARY KEY,
    category     TEXT NOT NULL,
    title        TEXT NOT NULL,
    content      TEXT NOT NULL,
    keywords     TEXT,
    scope        TEXT NOT NULL DEFAULT 'project' CHECK(scope IN ('project','global')),
    project_path TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT 'USER',
    CHECK(
        (scope = 'project' AND project_path IS NOT NULL AND trim(project_path) <> '')
        OR (scope = 'global' AND project_path IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_memories_scope_project_updated
    ON memories(scope, project_path, updated_at DESC);
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

-- Persistent Cron definitions. SQLite is the only authority: there is no
-- process-local job ledger and no JSON mirror. All timestamps below are epoch
-- milliseconds so claim comparisons are exact and independent of locale.
CREATE TABLE IF NOT EXISTS cron_jobs (
    job_id                TEXT PRIMARY KEY,
    owner_session_id      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    cron_expression       TEXT NOT NULL CHECK(trim(cron_expression) <> ''),
    timezone              TEXT NOT NULL CHECK(trim(timezone) <> ''),
    prompt                TEXT NOT NULL CHECK(trim(prompt) <> ''),
    recurring             INTEGER NOT NULL DEFAULT 1 CHECK(recurring IN (0,1)),
    overlap_policy        TEXT NOT NULL DEFAULT 'skip' CHECK(overlap_policy = 'skip'),
    missed_policy         TEXT NOT NULL DEFAULT 'skip' CHECK(missed_policy = 'skip'),
    status                TEXT NOT NULL DEFAULT 'active'
                               CHECK(status IN ('active','paused','deleted')),
    model                 TEXT NOT NULL CHECK(trim(model) <> ''),
    working_dir           TEXT NOT NULL CHECK(trim(working_dir) <> ''),
    next_scheduled_at_ms  INTEGER CHECK(next_scheduled_at_ms IS NULL OR next_scheduled_at_ms > 0),
    created_at_ms         INTEGER NOT NULL CHECK(created_at_ms > 0),
    updated_at_ms         INTEGER NOT NULL CHECK(updated_at_ms > 0),
    version               INTEGER NOT NULL DEFAULT 0 CHECK(version >= 0),
    CHECK((status='active' AND next_scheduled_at_ms IS NOT NULL)
       OR status IN ('paused','deleted'))
);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_due
    ON cron_jobs(status, next_scheduled_at_ms, job_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_owner
    ON cron_jobs(owner_session_id, status, created_at_ms);

-- One immutable scheduling decision for one wall-clock occurrence. A skipped
-- occurrence intentionally has no Task; an executable occurrence owns exactly
-- one root Session/Task/Run created in the same writer transaction.
CREATE TABLE IF NOT EXISTS cron_occurrences (
    occurrence_id   TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL REFERENCES cron_jobs(job_id) ON DELETE CASCADE,
    scheduled_at_ms INTEGER NOT NULL CHECK(scheduled_at_ms > 0),
    started_at_ms   INTEGER CHECK(started_at_ms IS NULL OR started_at_ms > 0),
    finished_at_ms  INTEGER CHECK(finished_at_ms IS NULL OR finished_at_ms > 0),
    task_id          TEXT UNIQUE REFERENCES tasks(id) ON DELETE SET NULL,
    status           TEXT NOT NULL
                          CHECK(status IN ('submitted','running','succeeded','partial',
                                           'failed','cancelled','skipped')),
    reason           TEXT,
    created_at_ms    INTEGER NOT NULL CHECK(created_at_ms > 0),
    updated_at_ms    INTEGER NOT NULL CHECK(updated_at_ms > 0),
    UNIQUE(job_id, scheduled_at_ms),
    CHECK((status='skipped' AND task_id IS NULL AND finished_at_ms IS NOT NULL AND reason IS NOT NULL)
       OR (status IN ('submitted','running') AND task_id IS NOT NULL AND finished_at_ms IS NULL)
       OR (status IN ('succeeded','partial','failed','cancelled')
           AND task_id IS NOT NULL AND finished_at_ms IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_cron_occurrences_job
    ON cron_occurrences(job_id, scheduled_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_cron_occurrences_active
    ON cron_occurrences(status, updated_at_ms)
    WHERE status IN ('submitted','running');
