# Local development

zkcode's source-development Beta topology is macOS local-only: Vite on
`127.0.0.1:5273`, `zk-server` on `127.0.0.1:8082`, and the Python sidecar on a
mode `0600` Unix-domain socket. Run all commands from the repository root.

## One-time bootstrap

```sh
./dev bootstrap --start
```

The bootstrap reuses supported toolchains, installs missing versions
side-by-side, consumes the repository lock files, installs Playwright Headless
Shell plus FFmpeg under `.runtime/playwright`, builds the current source, and
starts all services. It never changes Git state or overwrites an existing
`.env`. Configure your own provider API key before real chat validation. See
[configuration.md](configuration.md).

If Command Line Tools are absent, macOS opens Apple's installer. If native
Homebrew is absent, a normal bootstrap asks for administrator authorization in
the controlling Terminal before running Homebrew's installer non-interactively.
The password is handled by `sudo`, is not echoed, and is never read or stored by
zkcode. Never run `sudo ./dev ...`.

`--yes` skips zkcode's installation-plan confirmation but never prompts for a
sudo password. It is suitable for unattended use only when Homebrew already
exists or an administrator has preconfigured cached/passwordless sudo or a
trusted `SUDO_ASKPASS`; otherwise it fails before running the installer.

CI exercises the same `./dev sync --build` and `./dev doctor --deep` core on an
Apple Silicon macOS runner. A Mac with no Command Line Tools, Homebrew, or
language runtimes remains a separate clean-machine release acceptance case; do
not describe that path as generally verified until the release checklist passes.

## Start and stop

```sh
./dev up
./dev status
./dev doctor --deep
./dev restart
./dev stop
```

Open <http://127.0.0.1:5273> while the services are running. Runtime PIDs and
logs live under the ignored `.runtime/` directory. With Python enabled,
`./dev up` reuses a backend only when the sidecar reports `UP`; an incomplete
recorded backend/sidecar pair is stopped with identity checks and restarted,
while an already healthy Vite process is preserved.

## Release gates

```sh
./dev doctor --deep
./scripts/parity/run-local-gates.sh
```

The full gate includes Rust formatting, clippy, tests and release build;
frontend lint, tests, build and audit; Python tests with coverage; contract
checks; secret scanning; and Rust dependency policy checks.

`run-local-gates.sh` expects `cargo-deny` and `gitleaks` on `PATH`. On macOS with
Homebrew they can be installed with `brew install cargo-deny gitleaks`; the
setup script deliberately does not mutate global developer tooling.

`./scripts/setup-macos.sh`, `./scripts/doctor.sh`, `./start.sh`, and `./stop.sh`
remain compatibility forwards to `./dev`; new documentation and automation
must use `./dev` directly.

Real model and browser smoke tests are separate opt-in release gates because
they require credentials and running local services. See
`scripts/parity/run-qwen-smoke.sh`, `scripts/parity/run-kimi-smoke.sh`, and
`.github/workflows/real-smoke.yml`. Both provider scripts make one bounded
request, parse `.env` as data, and fail on provider errors.
