# TaskRuntime V4 contract

This document is the implementation contract for the greenfield zkcode runtime.
It deliberately defines no upgrade, backfill, dual-write, or legacy execution
path. A database without the exact `greenfieldFinal` identity is rejected at
startup and must be recreated.

## Authority and identity

SQLite is authoritative. In-memory maps, cancellation tokens, notifications,
WebSocket queues, and provider semaphores are disposable execution aids and may
never answer a lifecycle query on their own.

All persisted identifiers are canonical lower-case UUID v4 strings. Public JSON
uses lowerCamelCase. Only root Sessions are listed as conversations; every child
transcript is a Session with `kind=internal` and an explicit parent Task.

```text
root Session
  └─ root Task ── current Run (attempt 1..n)
       ├─ attached dependency ── child Task ── child Run ── internal Session
       └─ attached dependency ── child Task ── child Run ── internal Session

Run ──< ToolInvocation ──< ExecutionResource
Run ──< LlmCall
Task ──< immutable TaskResult ──> content-addressed TaskResultBlob
parent Task ──< ResultReceipt >── child Task + resultVersion
Run ──< RunEventLog (transactional outbox / recovery cursor)
```

One Task has at most one active Run and at most one Run for each attempt. A
submission is idempotent by `(creatorRunId, creatorToolUseId, ordinal)`. A result
receipt is idempotent by `(consumerTaskId, producerTaskId, resultVersion)`.

## Orthogonal state

- Task: `queued | running | waitingDependencies | waitingInteraction |
  cancelling | needsAttention | succeeded | partial | failed | cancelled`
- Run: `queued | running | waitingDependencies | waitingInteraction |
  cancelling | completed | failed | cancelled | interrupted`
- Result: `complete | partial | error | cancelled`
- Cleanup: `notRequired | pending | confirmed | unconfirmed`
- Verification: `notRequested | pending | passed | failed | stale | blocked`
- Exit reason: `modelFinished | timeout | maxTurns | budgetExhausted |
  providerError | toolError | userCancelled | parentCancelled |
  serviceRestart | internalError`

`Run.completed` means the execution loop ended normally. It does not assert that
the user request was satisfied or verified. `Task.succeeded` requires an
immutable complete TaskResult whose content is bound to the final Assistant
message. Verification and cleanup remain independent dimensions. Terminal Run
states never transition; another attempt always receives a new Run row.

## Transaction boundaries

1. Submission commits Task, queued Run, internal Session, attached dependency,
   budget reservation, creation event, and the parent's
   `waitingDependencies` transition before returning IDs. Replaying a submit
   key must match the complete normalized request. A committed queued attempt
   with no local driver may be reattached once; a claimed or terminal attempt
   is observation-only.
2. Dispatcher admission claims Task and Run with one CAS transaction. Queued
   work consumes no execution permit.
3. A parent with attached work transitions to `waitingDependencies` and Agent,
   TaskOutput, TaskGet, TaskList, TaskStop, TaskUpdate, and SendMessage waits do
   not consume a leaf-tool permit.
4. Child termination commits the Run terminal state, Task terminal state,
   immutable TaskResult/blob reference, budget settlement, and outbox event in
   one transaction.
5. A tool terminal transition and its attributed `tool_result` message commit
   together. When Artifact, Research, or Evidence projection is required, that
   transaction also creates a durable post-processing obligation. Task
   termination and parent ingestion reject a pending obligation.
6. Parent ingestion is deferred until the creating tool invocation has a paired
   durable tool result and completed post-processing. The result message and
   receipt commit together. The last unresolved dependency performs one CAS
   wakeup.
7. A cancelling or terminal parent retains late child results but never consumes
   them and is never reawakened.

TaskResult bodies of at most 64 KiB are inline. Larger results use a
content-addressed SQLite BLOB and cursor reads. Input beyond 16 MiB is persisted
as an explicit partial result with `RESULT_LIMIT_EXCEEDED`; silent truncation is
forbidden.

## Model-call and resource ownership

Every physical model request, including summaries, retries, fallback routes,
root calls, and child calls, owns an `llm_calls` row before provider execution.
Actual routing, usage, and `costNanosUsd` are recorded. Missing provider usage or
unknown pricing marks the call, Run, Task, and aggregate projection incomplete;
it is never represented as zero.

Every physical process/resource is owned by `(taskId, runId, toolUseId)` before
it can execute. Cancellation retains a reaper and process-group cleanup duty.
The default sequence is TERM for 5 seconds, KILL confirmation for 2 seconds, and
pipe collection for 1 second. An unconfirmed stop produces
`cleanupStatus=unconfirmed`, never a false `cancelled` claim.

The application owns one process-wide `ExecutionSupervisor`. Its intake closes
before runtime cancellation; it retains every leaf and nested process
`JoinHandle`, including when the caller drops its receiver. Outbound MCP calls
register the transport's real JSON-RPC request ID as an execution resource.
Only a successful response proves release; cancellation, timeout, disconnect,
or an unacknowledged remote stop is `unconfirmed`.

Graceful shutdown is ordered as: close leaf intake, close TaskRuntime intake,
persist `serviceRestart`, signal cancellation trees, drain runtime owners,
drain leaf/process owners, reconcile active rows, then close MCP/Python
transports. A shutdown-intent storage failure does not skip process-local
cleanup; it is returned only after cancellation and bounded drain have run.
Normal service restart never creates an immutable business-failure result.

## V4 tools

- `Agent({prompt, description?, subagentType?, model?, waitMode, isolation})`
- `TaskCreate({description, prompt, taskType:"agent"})`
- `TaskGet({taskId})`
- `TaskList({status?})`
- `TaskOutput({taskId, waitMs?, resultVersion?, cursor?, maxBytes?})`
- `TaskStop({taskId})`
- `TaskUpdate({taskId, description?, plan?, reportedProgress?})`
- `SendMessage({taskId, message})`

The execution boundary rejects unknown fields and the retired
`run_in_background`, `block`, `timeout`, and `to` parameters. Queries validate
root ancestry. Wait expiry is a successful response with `waitExpired=true`.
Public successes expose the common Task fields at the top level; failures are
`{code,message,retryable,details}`.

V1 supports attached, non-recursive child Agents only. Child capability is the
intersection of the live tool-directory generation, parent authorization,
agent policy, and per-call restriction. Read-only children include Read, Grep,
Glob, ListDir, WebSearch, and WebFetch.

## WebSocket and restore

Protocol V4 events contain `eventId`, `protocolVersion`, `sessionId`, `taskId`,
`runId`, `sourceTaskId`, `sourceRunId`, `toolUseId`, `type`, `payload`, and `ts`.
`run_event_log.id` is the global durable cursor. The client deduplicates by that
cursor and partitions state by Task, Run, and tool invocation. Bind restore reads
messages, the Task tree, recursive Run tree, active tools, usage/cost,
interactions, and the event high-water mark in one SQLite read transaction. An
empty projection is authoritative and clears stale client state. A root Run in
`waitingDependencies` may still restore active tool cards owned by child Runs.

Tool UI state is `preparing` while arguments are assembled. `running` is emitted
only after complete arguments are durable, admission succeeded, and capability
generations were rechecked. Every running event has one terminal counterpart.

## Limits and release gates

The runtime limits active Agents to 8 globally and 4 per root Task, leaf tools to
16, verified providers to 4, and unknown providers to 2. The root always owns a
deadline and durable usage account. Token and cost ceilings are optional; when
configured, the root retains 20% and each direct child can reserve at most 20%
subject to unreserved balance. Without a spend ceiling, child reservation rows
remain auditable but do not stop execution. Missing usage still fails closed.

The production defaults keep recursive delegation, Swarm, shared-workspace
writes, automatic recovery, automatic worktree merge, and Cron dispatch off.
These capabilities may only be enabled after their own production wiring and
fault-injection gates. A restart interrupts old Runs; only a future explicitly
proven safe recovery path may create a higher attempt. Unknown side effects
always leave the Task in `needsAttention`.

Evidence bundles and items are observations and are insert-only. An identical
retry is idempotent; reusing an ID with different content fails closed. Human
reviews and artifact-integrity invalidation append versioned verdict events
rather than rewriting evidence. Machine `passed`/`failed` projection requires a
succeeded producer invocation and updates the owning Run/Task transactionally;
a later passing check cannot erase an earlier unresolved failure.

Shared-workspace execution is controlled independently by
`ZK_SHARED_WORKSPACE_ENABLED=false`. It becomes executable only when Agent,
child-write admission, this independent switch, and the process-wide production
workspace lease are all active. `ZK_AGENT_WRITE_ENABLED` alone never enables
shared-workspace isolation. `ZK_AUTO_RESUME_SAFE_TASKS=false` is parsed for
fail-closed deployment visibility, but this release has no automatic recovery
entry: health reports the capability as unconfigured and unexecutable, and an
operator request to enable it makes startup fail after restart reconciliation
instead of creating a child attempt whose interrupted parent could never consume
the result.
