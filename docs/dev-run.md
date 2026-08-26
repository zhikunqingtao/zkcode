# Local development

zkcode's supported development topology is macOS local-only: Vite on
`127.0.0.1:5273`, `zk-server` on `127.0.0.1:8082`, and the Python sidecar on a
mode `0600` Unix-domain socket. Run all commands from the repository root.

## One-time setup

```sh
./scripts/setup-macos.sh
```

The setup installs locked frontend, Python, Playwright Chromium, and Rust
dependencies. A missing or failed download is an installation failure and must
not be bypassed. Edit the ignored root `.env` file and provide at least one
provider API key and model. See [configuration.md](configuration.md). Never
commit or paste credentials into logs.

## Start and stop

```sh
./start.sh
curl --fail http://127.0.0.1:8082/api/health
./stop.sh
```

Open <http://127.0.0.1:5273> while the services are running. Runtime PIDs and
logs live under the ignored `.runtime/` directory.

## Release gates

```sh
./scripts/doctor.sh
./scripts/parity/run-local-gates.sh
```

The full gate includes Rust formatting, clippy, tests and release build;
frontend lint, tests, build and audit; Python tests with coverage; contract
checks; secret scanning; and Rust dependency policy checks.

`run-local-gates.sh` expects `cargo-deny` and `gitleaks` on `PATH`. On macOS with
Homebrew they can be installed with `brew install cargo-deny gitleaks`; the
setup script deliberately does not mutate global developer tooling.

Real model and browser smoke tests are separate opt-in release gates because
they require credentials and running local services. See
`scripts/parity/run-qwen-smoke.sh`, `scripts/parity/run-kimi-smoke.sh`, and
`.github/workflows/real-smoke.yml`. Both provider scripts make one bounded
request, parse `.env` as data, and fail on provider errors.
