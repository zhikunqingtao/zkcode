# Contributing to zkcode

Thank you for contributing. zkcode's source-development entry is currently a
macOS-local Beta, so changes should preserve its loopback-only deployment and
explicit authorization model.

## Development setup

Supported toolchain ranges are declared in
[`configuration/dev-toolchain.toml`](configuration/dev-toolchain.toml) and
verified by `./dev doctor`. Clean machines without Command Line Tools,
Homebrew, or language runtimes remain a separate release acceptance case.

```bash
git clone https://github.com/zhikunqingtao/zkcode.git
cd zkcode
./dev bootstrap --start
# Edit the generated .env and configure at least one provider.
./dev restart
```

Run `./dev doctor --deep` for diagnostics and `./dev stop` to stop services.
The bootstrap installs the locked Playwright Headless Shell runtime. Do not
skip or replace a failed dependency download; fix the installation condition
and rerun the script. Configuration details are in
[docs/configuration.md](docs/configuration.md).

When Homebrew is missing, a normal bootstrap requests administrator
authorization through `sudo` in the current Terminal. `--yes` never prompts for
a sudo password and therefore requires Homebrew or administrator-provisioned
non-interactive authorization. Never run the project itself with `sudo`; see
[docs/troubleshooting.md](docs/troubleshooting.md) for the supported recovery
path.

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
