"""CLI 权限模式与后端 PermissionMode 协议的契约测试。"""

import inspect

from typer.testing import CliRunner

import cli.main as cli_main
from cli.main import PermissionMode, main


runner = CliRunner()


def test_cli_permission_modes_match_backend_contract():
    assert {mode.value for mode in PermissionMode} == {
        "default",
        "plan",
        "accept_edits",
        "dont_ask",
        "auto_approve",
    }


def test_cli_does_not_expose_permission_bypass_flag():
    """AUTO_APPROVE 只是一种权限模式，不引入跳过全部安全检查的入口。"""
    assert "no_permissions" not in inspect.signature(main).parameters
    assert "skip_all_prompts" not in {mode.value for mode in PermissionMode}


def test_cli_sends_explicit_mode_and_omits_default_for_server_resolution(monkeypatch):
    bodies = []

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def sync_query(self, body):
            bodies.append(body)
            return {"sessionId": "session-1", "result": "ok"}

    monkeypatch.setattr(cli_main, "ZkcodeClient", FakeClient)
    monkeypatch.setattr(
        cli_main.SessionCache,
        "save_last_session",
        lambda *_args, **_kwargs: None,
    )

    auto_result = runner.invoke(
        cli_main.app,
        ["hello", "--project-id", "project-1", "--output-format", "json", "--permission-mode", "auto_approve"],
    )
    default_result = runner.invoke(
        cli_main.app,
        ["hello", "--project-id", "project-1", "--output-format", "json"],
    )

    assert auto_result.exit_code == 0, auto_result.output
    assert default_result.exit_code == 0, default_result.output
    assert bodies[0]["permissionMode"] == "AUTO_APPROVE"
    assert "permissionMode" not in bodies[1]
    assert bodies[1]["maxTurns"] == 99
    assert bodies[1]["timeoutSeconds"] == 300
