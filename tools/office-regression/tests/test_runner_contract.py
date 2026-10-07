"""Native runner rejects unsafe output directories and preserves failed evidence."""
import importlib.util
import json
from pathlib import Path
import subprocess
import pytest

@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location('native_runner', Path(__file__).resolve().parents[1]/'native_runner.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

@pytest.fixture
def repo(tmp_path):
    root = tmp_path/'checkout'
    root.mkdir(); (root/'Cargo.toml').write_text('[workspace]\n')
    return root

@pytest.mark.parametrize('location', ['inside', 'symlink', 'occupied', 'nonempty'])
def test_runner_preserves_unsafe_or_occupied_output(runner, repo, tmp_path, location):
    out = tmp_path/'evidence'
    if location == 'inside': out = repo/'evidence'
    elif location == 'symlink':
        target = repo/'evidence'; target.mkdir(); out.symlink_to(target)
    elif location == 'occupied': (out/'.office-active').mkdir(parents=True)
    else:
        out.mkdir(); (out/'keep.txt').write_text('user evidence')
    with pytest.raises(ValueError): runner.run(repo, out, [])
    if location == 'nonempty': assert (out/'keep.txt').read_text() == 'user evidence'
    if location == 'occupied': assert (out/'.office-active').is_dir()

@pytest.mark.parametrize('code', [0, 7])
def test_runner_preserves_real_child_status_and_diagnostics(runner, repo, tmp_path, monkeypatch, code):
    class Child:
        stdout = iter(['diagnostic line\n'])
        def wait(self): return code
    calls = []
    def launch(command, **kwargs):
        calls.append((command, kwargs))
        return Child()
    monkeypatch.setattr(runner.subprocess, 'Popen', launch)
    out = tmp_path/'evidence'
    assert runner.run(repo, out, ['-q']) == code
    assert 'diagnostic line' in (out/'pytest.log').read_text()
    assert json.loads((out/'run-identity.json').read_text())['exit_code'] == code
    assert calls[0][0][1:3] == ['-m', 'pytest']
    assert not (out/'.office-active').exists()
