# Contributing to zkcode

Thank you for contributing. zkcode is currently a macOS-local Beta, so changes
should preserve its loopback-only deployment and explicit authorization model.

## Development setup

The currently verified environment is Apple Silicon macOS 26.5.2 with Rust
1.97.1, Node.js 22.14, npm 10.9, and Python 3.11.15. Other Mac configurations
may work but are not yet claimed as supported.

```bash
git clone https://github.com/zhikunqingtao/zkcode.git
cd zkcode
./scripts/setup-macos.sh
# Edit the generated .env and configure at least one provider.
./start.sh
```

Run `./scripts/doctor.sh` for diagnostics and `./stop.sh` to stop services.
The setup script also installs the locked Playwright Chromium runtime. Do not
skip or replace a failed dependency download; fix the installation condition
and rerun the script. Configuration details are in
[docs/configuration.md](docs/configuration.md).

## Pull requests

- Open an Issue before starting a large feature or public-contract change.
- Keep one logical change per pull request.
- Add positive, negative, security, and persistence tests where applicable.
- Do not replace a real integration test with a mock result.
- Do not include credentials, user data, absolute personal paths, generated
  artifacts, databases, or local logs.

Before opening a pull request, run:

```bash
./scripts/parity/run-local-gates.sh
```

Tests requiring a real model must stay short, use credentials only from the
local environment, and never print them. They are not run for untrusted pull
requests.

## License

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
