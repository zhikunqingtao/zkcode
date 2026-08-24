# ZhikunCode compatibility decisions

zkcode is an independent Rust implementation derived from ZhikunCode's public
behavior. Compatibility is defined by observable REST, WebSocket, tool, and
SQLite contracts rather than by reproducing its Java/Spring internals.

## 1. Protocol and identifiers

Public JSON field names, UUID identifiers, error codes, WebSocket message kinds,
and ordering rules remain compatible where represented by the contract fixtures.

## 2. Configuration and workspace

Environment variables use the `ZK_` prefix. Project and Session workspaces are
server-authorized canonical paths; request bodies cannot substitute arbitrary
working directories. The supported Beta deployment is loopback-only.

## 3. LLM providers and streaming

OpenAI-compatible and native Anthropic transports normalize provider streams to
one internal event model. Provider errors, usage, thinking, attachments, and
cancellation retain structured semantics.

## 4. Tools and process execution

Rust tools replace Java implementations while preserving public tool schemas and
result envelopes. Process cancellation is tree-aware and bounded; file writes use
atomic replacement and read-before-write conflict checks.

## 5. Command and path security

Command parsing, path normalization, sensitive-data checks, grants, hooks, and
execution-time revalidation form a single admission path. Behavioral differences
that close a known unsafe or non-functional legacy path are intentional.

## 6. Python capabilities

The Python service uses a local Unix Domain Socket instead of a TCP port. Only
Python 3.11 and 3.12 are accepted; health, imports, socket permissions, restart
limits, and dynamic capability registration fail closed.

## 7. Persistence

SQLite repositories are authoritative. Restoration and rewind operations are
transactional and idempotent. Restart marks active work interrupted rather than
silently resuming execution.

## 8. Authorization and interaction

Tool inputs are frozen before analysis. Grants are scoped and persistent;
unknown dynamic tools do not receive broad implicit approval. Interactive and
non-interactive query modes use the same service and authorization pipeline.

## 9. User interaction tools

Questions, todo state, task output, evidence, artifacts, verification, and
workbench projections are durable and share the same root Run identity.

Machine-enforced counts and routes live in `docs/parity/`; response-shape fixtures
that remain necessary for regression tests live in `docs/baseline/samples/`.
