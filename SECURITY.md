# Security policy

## Supported release

Security fixes are provided for the latest `0.1.x` Beta release only.

## Important local-execution boundary

zkcode is a local macOS application. It does **not** provide a container,
virtual machine, or operating-system sandbox. File tools, shell commands,
Python capabilities, MCP servers, hooks, and agents run with the permissions of
the macOS user who started zkcode.

The admission pipeline, path checks, approval prompts, command analysis, and
audit records reduce accidental or unauthorized operations, but they are not a
security boundary against malicious code. Use a dedicated workspace, inspect
requested permissions, and do not run untrusted repositories with sensitive
files accessible to the same macOS account.

The supported configuration binds the backend and frontend to `127.0.0.1`.
Remote hosting, LAN exposure, reverse-proxy deployment, Docker, and multi-user
operation are not supported by this Beta.

## Reporting a vulnerability

Do not open a public Issue for a vulnerability. Use GitHub's private
"Report a vulnerability" entry on this repository. If that is unavailable,
email `alizhikun@gmail.com` with:

- the affected version;
- reproducible steps or a minimal proof of concept;
- the expected impact;
- any suggested mitigation.

We aim to acknowledge reports within 48 hours and provide an initial assessment
within seven business days.

## User responsibilities

- Never commit `.env`, API keys, access tokens, databases, or `.zk/` runtime data.
- Keep zkcode bound to loopback and do not expose ports 5273 or 8081 publicly.
- Review every write, shell, hook, MCP, and network permission request.
- Treat third-party MCP servers, Skills, hooks, and project scripts as executable code.
- Keep macOS and all locked project dependencies updated.

Local storage locations, outbound data flows, backup, and reset guidance are
documented in [docs/data-and-privacy.md](docs/data-and-privacy.md).
