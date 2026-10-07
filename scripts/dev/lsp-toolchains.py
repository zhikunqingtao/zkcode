#!/usr/bin/env python3
"""Install checksum-locked private macOS LSP tools; never alter global toolchains."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid

MAX_DOWNLOAD = 512 * 1024 * 1024


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def atomic_json(path: Path, value: object) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile('w', dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def download(spec: dict, cache: Path, offline: bool) -> Path:
    destination = cache / spec['sha256']
    if destination.is_file() and digest(destination) == spec['sha256']:
        return destination
    if offline:
        raise RuntimeError('LSP archive unavailable offline; run ./dev bootstrap')
    with tempfile.NamedTemporaryFile(dir=cache, delete=False) as output:
        temporary = Path(output.name)
    try:
        process = None
        for _attempt in range(6):
            process = subprocess.run([
                '/usr/bin/curl', '--fail', '--location', '--proto', '=https',
                '--continue-at', '-', '--connect-timeout', '20', '--max-time', '240',
                '--max-filesize', str(MAX_DOWNLOAD), '--silent', '--show-error',
                '--output', str(temporary), spec['url'],
            ], timeout=260, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if process.returncode == 0:
                break
        if process is None or process.returncode:
            raise RuntimeError('LSP archive download failed: ' + (process.stderr[-600:] if process else 'not attempted'))
        if digest(temporary) != spec['sha256']:
            raise RuntimeError('LSP archive checksum mismatch')
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    return destination


def unpack(archive: Path, spec: dict, target: Path) -> None:
    target.mkdir()
    if spec['format'] == 'gz':
        executable = target / spec['executable']
        with gzip.open(archive, 'rb') as source, executable.open('wb') as output:
            shutil.copyfileobj(source, output)
        executable.chmod(0o755)
        return
    unpacked = target / '.unpack'
    unpacked.mkdir()
    with tarfile.open(archive, 'r:gz') as source:
        # Python's data filter forbids absolute/traversing links and device nodes.
        if sum(member.size for member in source.getmembers()) > 2 * 1024**3:
            raise RuntimeError('LSP archive expands beyond limit')
        source.extractall(unpacked, filter='data')
    if spec.get('stripRoot'):
        roots = list(unpacked.iterdir())
        if len(roots) != 1 or not roots[0].is_dir() or roots[0].is_symlink():
            raise RuntimeError('Unexpected archive root')
        source = roots[0]
    else:
        source = unpacked
    for entry in source.iterdir():
        shutil.move(str(entry), target / entry.name)
    shutil.rmtree(unpacked)


def checked(argv: list[str], *, env: dict, cwd: Path, timeout: int = 900) -> str:
    process = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=timeout)
    if process.returncode:
        raise RuntimeError(f'LSP installation command failed ({Path(argv[0]).name}): {process.stderr[-3000:]}')
    return process.stdout.strip()


def fingerprint(root: Path) -> str:
    value = hashlib.sha256()
    for relative in ('configuration/lsp-toolchain.json', 'configuration/lsp-npm/package.json',
                     'configuration/lsp-npm/package-lock.json', 'scripts/dev/lsp-toolchains.py'):
        value.update(relative.encode())
        value.update((root / relative).read_bytes())
    return value.hexdigest()


def probe(root: Path, home: Path) -> dict:
    try:
        current = json.loads((home / 'current.json').read_text())
        if current['fingerprint'] != fingerprint(root):
            raise RuntimeError('LSP lock or installer changed')
        bundle = (home / current['bundle']).resolve(strict=True)
        if not bundle.is_relative_to(home.resolve()):
            raise RuntimeError('LSP bundle escapes its private directory')
        for relative, expected in current['identities'].items():
            path = (bundle / relative).resolve(strict=True)
            if not path.is_relative_to(bundle) or digest(path) != expected:
                raise RuntimeError('LSP installed executable identity mismatch')
        rust_root = Path(current['rustToolchainRoot']).resolve(strict=True)
        if (digest(rust_root / 'bin/rustc') != current['rustCompilerSha256']
                or digest(rust_root / 'bin/cargo') != current['cargoSha256']
                or not (rust_root / 'lib/rustlib/src/rust/library').is_dir()):
            raise RuntimeError('LSP pinned Rust compiler or source identity is unavailable')
        if set(current['servers']) != {'typescript', 'python', 'rust', 'go', 'java'}:
            raise RuntimeError('LSP language set is incomplete')
        return {'ok': True, 'fingerprint': current['fingerprint'], 'versions': current['versions'],
                'manifest': str(home / 'current.json')}
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        return {'ok': False, 'reason': str(error), 'remediation': './dev bootstrap'}


def install(root: Path, home: Path, offline: bool) -> dict:
    if platform.system() != 'Darwin' or platform.machine() != 'arm64':
        raise RuntimeError('Private language toolchains require Apple Silicon macOS')
    prior = probe(root, home)
    if prior['ok']:
        return prior
    home.mkdir(parents=True, exist_ok=True)
    cache = home / 'downloads'
    cache.mkdir(exist_ok=True)
    policy = json.loads((root / 'configuration/lsp-toolchain.json').read_text())
    stamp = fingerprint(root)
    stage = Path(tempfile.mkdtemp(prefix='stage-', dir=home))
    bundle = home / stamp
    try:
        for name, spec in policy['artifacts'].items():
            print(f'LSP: installing {name} {spec["version"]}', flush=True, file=sys.stderr)
            unpack(download(spec, cache, offline), spec, stage / name)
        npm_dir = stage / 'npm'
        shutil.copytree(root / 'configuration/lsp-npm', npm_dir, ignore=shutil.ignore_patterns('node_modules'))
        env = dict(os.environ)
        for unsafe in ('NODE_OPTIONS', 'NODE_PATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'GOFLAGS', 'GOWORK'):
            env.pop(unsafe, None)
        env['PATH'] = str(stage / 'node/bin') + os.pathsep + str(stage / 'go/bin') + os.pathsep + env.get('PATH', '')
        rust_version = policy['rustCompiler']
        rustc = Path(checked(['rustup', 'which', '--toolchain', rust_version, 'rustc'], env=env, cwd=stage)).resolve(strict=True)
        rust_root = rustc.parent.parent
        components = checked(['rustup', 'component', 'list', '--installed', '--toolchain', rust_version], env=env, cwd=stage)
        if 'rust-src' not in components.splitlines():
            if offline:
                raise RuntimeError('Pinned rust-src is unavailable offline; run ./dev bootstrap')
            checked(['rustup', 'component', 'add', 'rust-src', '--toolchain', rust_version], env=env, cwd=stage)
        env['NPM_CONFIG_CACHE'] = str(home / 'npm-cache')
        npm = stage / 'node/bin/npm'
        checked([str(npm), 'ci', '--ignore-scripts', '--no-audit', '--no-fund'] + (['--offline'] if offline else []), env=env, cwd=npm_dir)
        go_bin = stage / 'bin'
        go_bin.mkdir()
        env.update({'GOBIN': str(go_bin), 'GOPATH': str(home / 'go-build'),
                    'GOMODCACHE': str(home / 'go-build/pkg/mod'), 'GOCACHE': str(home / 'go-build/cache'),
                    'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOFLAGS': '', 'CGO_ENABLED': '0',
                    'GOPROXY': 'off' if offline else 'https://proxy.golang.org', 'GOSUMDB': 'sum.golang.org'})
        go = stage / 'go/bin/go'
        if offline:
            module = home / 'go-build/pkg/mod' / (policy['gopls']['module'] + '@' + policy['gopls']['version'])
            if not module.is_dir():
                raise RuntimeError('Pinned gopls source is unavailable offline')
            checked([str(go), 'build', '-mod=readonly', '-trimpath', '-o', str(go_bin/'gopls'), '.'], env=env, cwd=module)
        else:
            checked([str(go), 'install', policy['gopls']['module'] + '@' + policy['gopls']['version']], env=env, cwd=stage)
        launchers = list((stage / 'jdtls/plugins').glob('org.eclipse.equinox.launcher_*.jar'))
        if len(launchers) != 1:
            raise RuntimeError('JDT language server launcher is ambiguous')
        java_home = stage / 'java/Contents/Home'
        relative_java = str((java_home / 'bin/java').relative_to(stage))
        launcher = str(launchers[0].relative_to(stage))
        servers = {
            'typescript': {'program': 'node/bin/node', 'args': ['{bundle}/npm/node_modules/typescript-language-server/lib/cli.mjs', '--stdio']},
            'python': {'program': 'node/bin/node', 'args': ['{bundle}/npm/node_modules/pyright/dist/pyright-langserver.js', '--stdio']},
            'rust': {'program': 'rust-analyzer/rust-analyzer', 'args': []},
            'go': {'program': 'bin/gopls', 'args': ['serve']},
            'java': {'program': relative_java, 'args': ['-Declipse.application=org.eclipse.jdt.ls.core.id1', '-Dosgi.bundles.defaultStartLevel=4', '-Dosgi.install.area={bundle}/jdtls', '-Dlog.protocol=false', '-Djava.io.tmpdir={state}/tmp', '-Declipse.product=org.eclipse.jdt.ls.core.product', '-Xmx1024m', '--add-modules=ALL-SYSTEM', '--add-opens', 'java.base/java.util=ALL-UNNAMED', '--add-opens', 'java.base/java.lang=ALL-UNNAMED', '-jar', '{bundle}/'+launcher, '-configuration', '{state}/java-config', '-data', '{state}/java']},
        }
        if not (stage / 'jdtls/config_mac_arm').is_dir():
            raise RuntimeError('JDT language server lacks Apple Silicon configuration')
        versions = {name: spec['version'] for name, spec in policy['artifacts'].items()}
        versions.update(json.loads((npm_dir / 'package.json').read_text())['dependencies'])
        versions['gopls'] = policy['gopls']['version']
        versions['rustCompiler'] = rust_version
        identities = {str(path.relative_to(stage)): digest(path) for path in [
            stage / 'node/bin/node', stage / 'go/bin/go', java_home / 'bin/java', launchers[0],
            stage / 'rust-analyzer/rust-analyzer', stage / 'bin/gopls',
            npm_dir / 'node_modules/typescript-language-server/lib/cli.mjs',
            npm_dir / 'node_modules/typescript/lib/tsserver.js', npm_dir / 'node_modules/pyright/dist/pyright-langserver.js',
        ]}
        checked([str(stage/'node/bin/node'), '--version'], env=env, cwd=stage)
        checked([str(go), 'version'], env=env, cwd=stage)
        checked([str(java_home/'bin/java'), '-version'], env=env, cwd=stage)
        checked([str(stage/'rust-analyzer/rust-analyzer'), '--version'], env=env, cwd=stage)
        checked([str(stage/'bin/gopls'), 'version'], env=env, cwd=stage)
        record = {'schemaVersion': 1, 'fingerprint': stamp, 'bundle': bundle.name,
                  'versions': versions, 'identities': identities, 'servers': servers, 'rustToolchainRoot': str(rust_root),
                  'rustCompilerSha256': digest(rustc), 'cargoSha256': digest(rust_root/'bin/cargo')}
        atomic_json(stage / 'manifest.json', record)
        if bundle.exists():
            # Keep an old damaged version recoverable; never delete user/global paths.
            backup = home / (stamp + '-previous-' + uuid.uuid4().hex)
            os.replace(bundle, backup)
        os.replace(stage, bundle)
        atomic_json(home / 'current.json', record)
        return probe(root, home)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--home', type=Path)
    parser.add_argument('--install', action='store_true')
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    root = args.root.resolve()
    home = args.home or root / '.runtime/lsp'
    try:
        result = install(root, home, args.offline) if args.install else probe(root, home)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        result = {'ok': False, 'reason': str(error), 'remediation': './dev bootstrap'}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
