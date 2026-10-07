#!/usr/bin/env python3
"""Install pinned official Chinese OCR models; preserve healthy existing models."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib
import urllib.request


def valid_model(path: Path) -> bool:
    if not path.is_file() or path.is_symlink():
        return False
    try:
        return subprocess.run(['combine_tessdata', '-d', str(path)], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def install(data_dir: Path, policy: dict):
    version = policy['ocr_model_version']
    if not re.fullmatch(r'[0-9]+(?:\.[0-9]+)+', version):
        raise ValueError('invalid OCR model version')
    data_dir.mkdir(parents=True, exist_ok=True)
    for language in ('chi_sim', 'chi_tra'):
        target = data_dir / f'{language}.traineddata'
        if target.is_symlink():
            raise ValueError(f'refusing to replace a symlink: {target}')
        if valid_model(target):
            print(f'OCR {language}: preserving healthy existing model', flush=True)
            continue
        digest = policy[f'ocr_{language}_sha256']
        if not re.fullmatch(r'[a-f0-9]{64}', digest):
            raise ValueError('invalid OCR model digest')
        url = f'https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/{version}/{language}.traineddata'
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=data_dir, prefix=f'.{language}-', suffix='.traineddata', delete=False) as output:
                temporary = Path(output.name)
                with urllib.request.urlopen(url, timeout=45) as response:
                    data = response.read(16 * 1024 * 1024 + 1)
                if len(data) > 16 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError(f'OCR {language}: downloaded model hash mismatch')
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            if not valid_model(temporary):
                raise ValueError(f'OCR {language}: downloaded model is not loadable')
            # Do not overwrite a valid model installed by a concurrent setup.
            if not valid_model(target):
                os.replace(temporary, target)
            print(f'OCR {language}: installed official {version} model', flush=True)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--policy', type=Path, required=True)
    args = parser.parse_args()
    with args.policy.open('rb') as source:
        install(args.data_dir, tomllib.load(source))


if __name__ == '__main__':
    main()
