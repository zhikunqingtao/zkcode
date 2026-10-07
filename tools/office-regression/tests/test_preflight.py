"""Native capability checks; the application lock remains the Python authority."""
import importlib.metadata
import importlib.util
import json
import hashlib
import subprocess
from pathlib import Path
import re
import sys
import r12lib as R12


def test_preflight_toolchain_and_fonts(r12):
    path = Path(r12['repo'])/'scripts/dev/document-tools.py'
    spec = importlib.util.spec_from_file_location('document_tools', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    checks = module.inspect_tools()
    identities = []
    for check in checks:
        for raw in ([check['path']] if check.get('path') else check.get('paths', [])):
            path = Path(raw).resolve()
            if path.is_file():
                identities.append({'name':check['name'], 'path':str(path), 'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    languages = subprocess.check_output(['tesseract','--list-langs'], text=True)
    tessdata = Path(re.search(r'\"([^\"]+)\"', languages).group(1))
    for language in ('chi_sim','chi_tra'):
        path = tessdata/f'{language}.traineddata'
        if path.is_file():
            identities.append({'name':'ocr-'+language, 'path':str(path), 'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    R12.write_evidence(r12['evidence'], 'preflight-toolchain', {'checks':checks, 'identities':identities})
    assert all(check['ok'] for check in checks), json.dumps([check for check in checks if not check['ok']])


def test_preflight_python_packages(r12):
    text = (Path(r12['repo'])/'python-service/requirements.lock').read_text()
    pins = dict(re.findall(r'^([a-zA-Z0-9_-]+)==([^\s]+)$', text, re.M))
    packages = ('playwright', 'pytest', 'pytest-timeout', 'fastapi', 'starlette', 'uvicorn', 'pydantic')
    actual = {name:importlib.metadata.version(name) for name in packages}
    R12.write_evidence(r12['evidence'], 'preflight-python-packages', {'python':sys.executable, 'packages':actual})
    assert all(actual[name] == pins[name] for name in packages), actual
