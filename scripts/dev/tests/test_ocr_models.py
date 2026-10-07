import hashlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'ocr-models.py'
spec = importlib.util.spec_from_file_location('ocr_models', SOURCE)
OCR = importlib.util.module_from_spec(spec)
spec.loader.exec_module(OCR)


class OcrModelInstallTests(unittest.TestCase):
    def test_preserves_valid_models_without_network(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            for language in ('chi_sim', 'chi_tra'):
                (root / f'{language}.traineddata').write_bytes(b'newer-valid-model')
            with patch.object(OCR, 'valid_model', return_value=True), patch.object(OCR.urllib.request, 'urlopen') as download:
                OCR.install(root, {'ocr_model_version':'4.1.0'})
                download.assert_not_called()
            self.assertEqual((root / 'chi_sim.traineddata').read_bytes(), b'newer-valid-model')

    def test_hash_mismatch_never_replaces_existing_content(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            target = root / 'chi_sim.traineddata'
            target.write_bytes(b'original')
            with patch.object(OCR, 'valid_model', return_value=False), patch.object(OCR.urllib.request, 'urlopen', return_value=io.BytesIO(b'wrong')):
                with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                    OCR.install(root, {'ocr_model_version':'4.1.0', 'ocr_chi_sim_sha256':'0'*64})
            self.assertEqual(target.read_bytes(), b'original')
            self.assertEqual(list(root.iterdir()), [target])

    def test_verified_model_is_atomically_installed(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            content = b'valid-official-model'
            policy = {'ocr_model_version':'4.1.0', **{f'ocr_{lang}_sha256': hashlib.sha256(content).hexdigest() for lang in ('chi_sim','chi_tra')}}
            with patch.object(OCR, 'valid_model', side_effect=lambda path:path.is_file()), patch.object(OCR.urllib.request, 'urlopen', side_effect=lambda *a,**kw:io.BytesIO(content)):
                OCR.install(root, policy)
            self.assertEqual((root/'chi_sim.traineddata').read_bytes(),content)
            self.assertEqual((root/'chi_tra.traineddata').read_bytes(),content)
            self.assertEqual(len(list(root.iterdir())),2)

    def test_symlink_target_is_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            original = root/'original'
            original.write_bytes(b'keep')
            (root/'chi_sim.traineddata').symlink_to(original)
            with self.assertRaisesRegex(ValueError, 'symlink'):
                OCR.install(root, {'ocr_model_version':'4.1.0'})
            self.assertEqual(original.read_bytes(), b'keep')
