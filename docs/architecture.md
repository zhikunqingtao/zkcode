# Architecture

zkcode is a macOS-local application with three cooperating components:

1. `zk-server`, a Rust/Axum process that owns REST, native WebSocket, SSE,
   conversations, authorization, tools, agents, MCP, and persistence.
2. A React/Vite frontend bound to `127.0.0.1:5273` and proxied to the Rust
   backend on `127.0.0.1:8082`.
3. A Python 3.11 capability service managed by `zk-server` over a
   permission-`0600` Unix Domain Socket.

SQLite is the single durable business database. Session, Run, Task,
Checkpoint, Snapshot, Evidence, Artifact, Workbench, authorization, MCP, and
observability records share that database. In-memory state is limited to
cancellation tokens, process handles, WebSocket clients, and bounded queues.

All tool execution enters one admission pipeline: frozen input, operation
analysis, invariant checks, grants or user interaction, execution-time
revalidation, execution, and audit recording. PRE hooks may reject or rewrite
input; rewritten input is analyzed again before execution.

Agents create child Session/Run records and inherit the same database,
authorization, hooks, accounting, snapshots, summaries, and cancellation tree.
Worktree isolation and explicit delivery are enabled after the real Git
integration gate. Creating or finishing a task never implicitly commits or
merges changes; those actions require explicit operations. Git processes have
durable session/task ownership and a start gate released only after PID binding.

The V4 WebSocket contract has 60 downstream event kinds, including task
boundaries, message metadata and assistant segments. Session merges seal source
snapshots and create the target transactionally; the target inherits the primary
session permission. Attached and detached tasks remain owned and budgeted.

Global Skill switches and memory entries are stored transactionally in SQLite.
Markdown memory documents are reversible views with revision/CAS writes. Source
history and handoff snapshots remain data, never new authorization.

The public contracts are recorded under `docs/parity/` and verified by
`scripts/parity/check-contracts.sh`.

The greenfield durable task model, V4 tool/WS contract, transaction boundaries,
and release gates are frozen in [`task-runtime-v4.md`](task-runtime-v4.md).
Current implementation and measured acceptance results are recorded in the
[migration ledger](migration/2026-10-migration.md). The
[architecture poster](zkcode-architecture-overview.html) is explicitly a
2026-08-31 historical snapshot; its counts and feature switches are not current.
