# Architecture

zkcode is a macOS-local application with three cooperating components:

1. `zk-server`, a Rust/Axum process that owns REST, native WebSocket, SSE,
   conversations, authorization, tools, agents, MCP, and persistence.
2. A React/Vite frontend bound to `127.0.0.1:5273` and proxied to the Rust
   backend on `127.0.0.1:8081`.
3. A Python 3.11/3.12 capability service managed by `zk-server` over a
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
Worktree remains disabled until its real Git integration gate is complete.

The public contracts are recorded under `docs/parity/` and verified by
`scripts/parity/check-contracts.sh`.
