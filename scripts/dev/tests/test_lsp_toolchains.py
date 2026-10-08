"""Pinned installation must reject corrupt archives and escaping links offline."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC=importlib.util.spec_from_file_location('lsp_toolchains',Path(__file__).resolve().parents[1]/'lsp-toolchains.py')
MODULE=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ToolchainIntegrityTests(unittest.TestCase):
    def test_install_does_not_add_rust_src_to_the_global_toolchain(self):
        class StopBeforeNpm(RuntimeError): pass
        with tempfile.TemporaryDirectory() as directory:
            root = Path(__file__).resolve().parents[3]
            home = Path(directory) / 'private'
            rustc = Path(directory) / 'global/bin/rustc'
            rustc.parent.mkdir(parents=True)
            rustc.write_bytes(b'compiler')
            calls = []
            def checked(argv, **_kwargs):
                calls.append(argv)
                if argv[:2] == ['rustup', 'which']: return str(rustc)
                if argv[:3] == ['rustup', 'component', 'list']: return ''
                if argv[:3] == ['rustup', 'component', 'add']: return ''
                raise StopBeforeNpm()
            with patch.object(MODULE.platform, 'system', return_value='Darwin'), patch.object(MODULE.platform, 'machine', return_value='arm64'), \
                 patch.object(MODULE, 'probe', return_value={'ok': False}), patch.object(MODULE, 'download', return_value=Path('fixture')), \
                 patch.object(MODULE, 'unpack', side_effect=lambda _a, _s, target: target.mkdir()), patch.object(MODULE, 'checked', side_effect=checked):
                with self.assertRaises(StopBeforeNpm): MODULE.install(root, home, False)
            self.assertFalse(any(argv[:3] == ['rustup', 'component', 'add'] for argv in calls), calls)

    def test_probe_rejects_legacy_manifest_and_empty_rust_compiler_version(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory); bundle = home / 'bundle'; bundle.mkdir()
            rust_root = home / 'global'; (rust_root / 'bin').mkdir(parents=True)
            (rust_root / 'bin/rustc').write_bytes(b'rustc'); (rust_root / 'bin/cargo').write_bytes(b'cargo')
            (rust_root / 'lib/rustlib/src/rust/library').mkdir(parents=True)
            for schema, version in [(1, '1.97.1'), (2, '')]:
                record = {'schemaVersion': schema, 'fingerprint': 'fixture', 'bundle': 'bundle', 'identities': {},
                    'rustToolchainRoot': str(rust_root), 'rustCompilerSha256': MODULE.digest(rust_root / 'bin/rustc'),
                    'cargoSha256': MODULE.digest(rust_root / 'bin/cargo'), 'versions': {'rustCompiler': version},
                    'servers': {name: {} for name in ['typescript', 'python', 'rust', 'go', 'java']}}
                MODULE.atomic_json(home / 'current.json', record)
                with patch.object(MODULE, 'fingerprint', return_value='fixture'):
                    self.assertFalse(MODULE.probe(home, home)['ok'], record)

    def test_private_source_identity_rejects_content_changes_and_links(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory); source = home / 'bundle/source'; source.mkdir(parents=True)
            (source / 'lib.rs').write_text('pub fn a() {}')
            rust_root = home / 'compiler'; (rust_root / 'bin').mkdir(parents=True)
            (rust_root / 'bin/rustc').write_bytes(b'rustc'); (rust_root / 'bin/cargo').write_bytes(b'cargo')
            record = {'schemaVersion': 2, 'fingerprint': 'fixture', 'bundle': 'bundle', 'identities': {},
                'rustSourceRoot': 'source', 'rustSourceSha256': MODULE.tree_digest(source),
                'rustToolchainRoot': str(rust_root), 'rustCompilerSha256': MODULE.digest(rust_root / 'bin/rustc'),
                'cargoSha256': MODULE.digest(rust_root / 'bin/cargo'), 'versions': {'rustCompiler': '1.97.1', 'rust-src': '1.97.1'},
                'servers': {name: {} for name in ['typescript', 'python', 'rust', 'go', 'java']}}
            MODULE.atomic_json(home / 'current.json', record)
            with patch.object(MODULE, 'fingerprint', return_value='fixture'):
                self.assertTrue(MODULE.probe(home, home)['ok'])
                (source / 'lib.rs').write_text('pub fn b() {}')
                self.assertFalse(MODULE.probe(home, home)['ok'])
                (source / 'lib.rs').unlink(); (source / 'lib.rs').symlink_to(rust_root / 'bin/rustc')
                self.assertFalse(MODULE.probe(home, home)['ok'])

    def test_tree_identity_binds_relative_names_and_matches_the_rust_algorithm(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory); (source / 'core').mkdir(); (source / 'core/lib.rs').write_bytes(b'core')
            (source / 'std').mkdir(); (source / 'std/lib.rs').write_bytes(b'std')
            self.assertEqual(MODULE.tree_digest(source), '497c27d4b48df7fe32f8276b04c85a12f9b37e0a9e3cb498945f0def2df1bba0')

    def test_tree_identity_sorts_relative_posix_names_not_path_components(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / 'a').mkdir()
            (source / 'a/lib.rs').write_bytes(b'nested')
            (source / 'a.rs').write_bytes(b'flat')
            # Rust BTreeMap<String> compares the complete relative name:
            # 'a.rs' precedes 'a/lib.rs', unlike Python's sorted(Path).
            self.assertEqual(MODULE.tree_digest(source), '88f8acf7a2d56929bdb269b7fe9c4b3033d62837300ce705b0fa98cb992a65ae')

    def test_offline_cache_accepts_only_exact_pinned_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            cache=Path(directory)
            expected='0'*64
            (cache/expected).write_bytes(b'corrupt executable')
            with self.assertRaisesRegex(RuntimeError,'unavailable offline'):
                MODULE.download({'sha256':expected,'url':'https://unused.example/tool'},cache,True)
            data=cache/'source'
            data.write_bytes(b'known bytes')
            digest=MODULE.digest(data)
            data.rename(cache/digest)
            self.assertEqual(MODULE.download({'sha256':digest},cache,True),cache/digest)

    def test_archive_link_cannot_write_outside_private_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);archive=root/'escape.tar.gz'
            with tarfile.open(archive,'w:gz') as output:
                entry=tarfile.TarInfo('outside');entry.type=tarfile.SYMTYPE;entry.linkname='../sentinel';output.addfile(entry)
                entry=tarfile.TarInfo('outside/changed');entry.size=4;output.addfile(entry,io.BytesIO(b'evil'))
            sentinel=root/'sentinel';sentinel.mkdir()
            with self.assertRaises(tarfile.FilterError):
                MODULE.unpack(archive,{'format':'tar.gz'},root/'stage')
            self.assertEqual(list(sentinel.iterdir()),[])

    def test_atomic_manifest_failure_leaves_previous_content(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'current.json';MODULE.atomic_json(path,{'bundle':'valid'})
            with self.assertRaises(TypeError):MODULE.atomic_json(path,{'bundle':object()})
            self.assertEqual(json.loads(path.read_text()),{'bundle':'valid'})


if __name__=='__main__':unittest.main()
