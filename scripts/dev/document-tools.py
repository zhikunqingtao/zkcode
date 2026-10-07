#!/usr/bin/env python3
"""Discover the native document/media toolchain without modifying the host."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

COMMANDS = {
    'libreoffice': ('soffice', '--version'), 'pdf': ('pdftoppm', '-v'),
    'pdf-text': ('pdftotext', '-v'), 'pdf-info': ('pdfinfo', '-v'),
    'qpdf': ('qpdf', '--version'), 'media': ('ffmpeg', '-version'),
    'images': ('magick', '-version'), 'diagrams': ('dot', '-V'),
    'documents': ('pandoc', '--version'), 'ocr': ('tesseract', '--version'),
}

def inspect_tools():
    checks = []
    for name, (binary, flag) in COMMANDS.items():
        path = shutil.which(binary)
        if binary == 'soffice' and not path:
            candidate = Path('/Applications/LibreOffice.app/Contents/MacOS/soffice')
            path = str(candidate) if candidate.is_file() else None
        detail = 'not installed'
        ok = False
        if path:
            try:
                result = subprocess.run([path, flag], capture_output=True, text=True, timeout=30)
                detail = (result.stdout or result.stderr).strip().splitlines()[0]
                ok = result.returncode == 0
            except (OSError, subprocess.SubprocessError, IndexError) as error:
                detail = type(error).__name__
        checks.append({'name': name, 'ok': ok, 'path': path, 'detail': detail})
    tesseract = shutil.which('tesseract')
    languages = ''
    if tesseract:
        try:
            result = subprocess.run([tesseract, '--list-langs'], capture_output=True, text=True, timeout=30)
            languages = result.stdout + result.stderr
        except (OSError, subprocess.SubprocessError):
            pass
    checks.append({'name': 'ocr-chinese', 'ok': all(name in languages.splitlines() for name in ('chi_sim','chi_tra')), 'detail': 'chi_sim and chi_tra required'})
    fonts = [Path.home()/'Library/Fonts', Path('/Library/Fonts'), Path('/System/Library/Fonts')]
    font_paths = [str(p) for root in fonts if root.is_dir() for p in root.rglob('*') if p.is_file() and ('NotoSansCJK' in p.name or 'PingFang' in p.name)]
    available = bool(font_paths)
    checks.append({'name': 'cjk-fonts', 'ok': available, 'detail': 'Noto Sans CJK or native PingFang', 'paths':font_paths})
    return checks

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    checks = inspect_tools()
    print(json.dumps(checks) if args.json else '\n'.join(f"{check['name']}: {'ok' if check['ok'] else 'missing'} — {check['detail']}" for check in checks))
    sys.exit(0 if all(check['ok'] for check in checks) else 1)
