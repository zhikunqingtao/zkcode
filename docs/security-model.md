# Security model

zkcode protects operations with application-level controls: canonical workspace
identity, path traversal and symlink checks, command analysis, sensitive-data
redaction, scoped grants, PRE hooks, execution-time revalidation, cancellation,
and durable audit events.

These controls are not an operating-system sandbox. A permitted process inherits
the current macOS user's access. The supported Beta therefore binds only to
loopback and is intended for trusted local projects. See the root
`SECURITY.md` for deployment limits and vulnerability reporting.

Dynamic MCP tools, Skills, hooks, and Python capabilities are treated as code.
Unknown identities fail closed and write/process/network operations require the
same admission pipeline as built-in tools.
