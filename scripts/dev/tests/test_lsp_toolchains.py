"""Pinned installation must reject corrupt archives and escaping links offline."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

SPEC=importlib.util.spec_from_file_location('lsp_toolchains',Path(__file__).resolve().parents[1]/'lsp-toolchains.py')
MODULE=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ToolchainIntegrityTests(unittest.TestCase):
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
