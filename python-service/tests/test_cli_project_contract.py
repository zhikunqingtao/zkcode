"""CLI Project/Session request contract tests."""

import httpx
import pytest
from typer.testing import CliRunner

import cli.client as client_module
import cli.main as cli_main


runner = CliRunner()


def status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://localhost:8080/api/projects")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(
        f"Project API returned HTTP {status}",
        request=request,
        response=response,
    )


class FakeClient:
    def __init__(
        self,
        projects=None,
        *,
        list_effects=None,
        create_error=None,
    ) -> None:
        self.projects = [] if projects is None else projects
        self.list_effects = list(list_effects or [])
        self.create_error = create_error
        self.project_calls = []
        self.query_body = None

    def list_projects(self):
        self.project_calls.append(("list_projects",))
        if self.list_effects:
            effect = self.list_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect
        return self.projects

    def create_project(self, name, workspace_root):
        self.project_calls.append(
            ("create_project", name, workspace_root)
        )
        if self.create_error:
            raise self.create_error
        return {"id": "created-project"}

    def sync_query(self, body):
        self.query_body = body
        return {
            "sessionId": body.get("sessionId", "created-session"),
            "result": "ok",
        }

    def stream_query(self, body):
        self.query_body = body
        yield {
            "type": "message_complete",
            "sessionId": body.get("sessionId", "created-session"),
        }


def install_client(monkeypatch, fake):
    monkeypatch.setattr(
        cli_main,
        "ZkcodeClient",
        lambda **_kwargs: fake,
    )
    monkeypatch.setattr(
        cli_main.SessionCache,
        "save_last_session",
        lambda *_args, **_kwargs: None,
    )


def test_aica_client_exposes_only_project_list_and_create_helpers(monkeypatch):
    calls = []

    class StubResponse:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    def fake_get(url, **kwargs):
        calls.append(("GET", url, kwargs))
        return StubResponse([{"id": "project-1"}])

    def fake_post(url, **kwargs):
        calls.append(("POST", url, kwargs))
        return StubResponse({"id": "project-2"})

    monkeypatch.setattr(client_module.httpx, "get", fake_get)
    monkeypatch.setattr(client_module.httpx, "post", fake_post)

    client = client_module.ZkcodeClient(
        server="http://localhost:8080/",
        token="secret",
        timeout=90,
    )

    assert client.list_projects() == [{"id": "project-1"}]
    assert client.create_project("repo", "/workspace/repo") == {
        "id": "project-2"
    }
    assert calls[0][0:2] == (
        "GET",
        "http://localhost:8080/api/projects",
    )
    assert calls[1][0:2] == (
        "POST",
        "http://localhost:8080/api/projects",
    )
    assert calls[1][2]["json"] == {
        "name": "repo",
        "workspaceRoot": "/workspace/repo",
    }


@pytest.mark.parametrize(
    "server",
    [
        "http://localhost:8080",
        "http://LOCALHOST:8080",
        "http://127.0.0.1:8080",
        "http://127.42.0.7:8080",
        "http://[::1]:8080",
        "[::1]:8080",
    ],
)
def test_loopback_server_detection(server):
    assert cli_main._is_loopback_server(server) is True


@pytest.mark.parametrize(
    "server",
    [
        "http://0.0.0.0:8080",
        "http://192.168.1.10:8080",
        "http://remote.internal:8080",
        "https://remote.example",
        "localhost.example:8080",
    ],
)
def test_non_loopback_hosts_are_not_inferred_to_be_local(server):
    assert cli_main._is_loopback_server(server) is False


def test_first_local_run_without_working_dir_uses_server_default(
    monkeypatch,
    tmp_path,
):
    fake = FakeClient()
    install_client(monkeypatch, fake)
    monkeypatch.setattr(cli_main.os, "getcwd", lambda: str(tmp_path))

    result = runner.invoke(
        cli_main.app,
        ["hello", "--output-format", "json"],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == []
    assert "projectId" not in fake.query_body
    assert "sessionId" not in fake.query_body
    assert "workingDirectory" not in fake.query_body


def test_explicit_local_working_dir_registers_canonical_project(
    monkeypatch,
    tmp_path,
):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--working-dir",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    canonical = str(tmp_path.resolve())
    assert fake.project_calls == [
        ("list_projects",),
        ("create_project", tmp_path.name, canonical),
    ]
    assert fake.query_body["projectId"] == "created-project"
    assert "workingDirectory" not in fake.query_body


def test_existing_project_for_canonical_working_directory_is_reused(
    monkeypatch,
    tmp_path,
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    alias = tmp_path / "workspace-alias"
    alias.symlink_to(workspace, target_is_directory=True)
    canonical = str(workspace.resolve())
    fake = FakeClient([{
        "id": "existing-project",
        "workspaceRoot": canonical,
    }])
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--working-dir",
            str(alias),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == [("list_projects",)]
    assert fake.query_body["projectId"] == "existing-project"
    assert "workingDirectory" not in fake.query_body


def test_create_conflict_relists_and_reuses_racing_project(
    monkeypatch,
    tmp_path,
):
    canonical = str(tmp_path.resolve())
    fake = FakeClient(
        list_effects=[
            [],
            [{
                "id": "racing-project",
                "workspaceRoot": canonical,
            }],
        ],
        create_error=status_error(409),
    )
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--working-dir",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == [
        ("list_projects",),
        ("create_project", tmp_path.name, canonical),
        ("list_projects",),
    ]
    assert fake.query_body["projectId"] == "racing-project"


@pytest.mark.parametrize("failure_stage", ["list", "create"])
def test_project_api_404_for_explicit_local_directory_exits_without_query(
    monkeypatch,
    tmp_path,
    failure_stage,
):
    fake = FakeClient(
        list_effects=(
            [status_error(404)] if failure_stage == "list" else None
        ),
        create_error=(
            status_error(404) if failure_stage == "create" else None
        ),
    )
    install_client(monkeypatch, fake)
    monkeypatch.setattr(cli_main.os, "getcwd", lambda: str(tmp_path))

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--working-dir",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 1
    assert "selected working directory cannot be authorized" in result.output
    assert "HTTP 404" in result.output
    assert fake.query_body is None


@pytest.mark.parametrize("status", [400, 403])
def test_rejected_local_directory_does_not_fall_back(
    monkeypatch,
    tmp_path,
    status,
):
    fake = FakeClient(create_error=status_error(status))
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        ["hello", "--working-dir", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "selected working directory cannot be authorized" in result.output
    assert fake.query_body is None


def test_project_api_5xx_does_not_downgrade_to_server_default(
    monkeypatch,
    tmp_path,
):
    fake = FakeClient(create_error=status_error(503))
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        ["hello", "--working-dir", str(tmp_path)],
    )

    assert result.exit_code == 1
    assert "HTTP 503" in result.output
    assert fake.query_body is None


def test_project_api_connection_failure_does_not_run_query(
    monkeypatch,
    tmp_path,
):
    request = httpx.Request(
        "GET",
        "http://localhost:8080/api/projects",
    )
    fake = FakeClient(list_effects=[
        httpx.ConnectError("offline", request=request)
    ])
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        ["hello", "--working-dir", str(tmp_path)],
    )

    assert result.exit_code == 3
    assert "Backend not reachable" in result.output
    assert fake.query_body is None


def test_remote_server_rejects_client_local_working_directory(
    monkeypatch,
    tmp_path,
):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--server",
            "https://remote.example",
            "--working-dir",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 2
    assert fake.project_calls == []
    assert fake.query_body is None
    assert "use --project-id for a remote backend" in result.output


@pytest.mark.parametrize(
    "extra_args",
    [
        ["--project-id", "project-1"],
        ["--session-id", "session-1"],
        ["--resume", "session-2"],
        ["--continue"],
    ],
)
def test_remote_working_directory_is_rejected_with_project_or_session_options(
    monkeypatch,
    tmp_path,
    extra_args,
):
    client_created = False

    def create_client(**_kwargs):
        nonlocal client_created
        client_created = True
        return FakeClient()

    monkeypatch.setattr(cli_main, "ZkcodeClient", create_client)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--server",
            "https://remote.example",
            "--working-dir",
            str(tmp_path),
            *extra_args,
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 2
    assert "use --project-id for a remote backend" in result.output
    assert client_created is False


def test_local_working_directory_cannot_be_combined_with_project_id(
    monkeypatch,
    tmp_path,
):
    client_created = False

    def create_client(**_kwargs):
        nonlocal client_created
        client_created = True
        return FakeClient()

    monkeypatch.setattr(cli_main, "ZkcodeClient", create_client)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--working-dir",
            str(tmp_path),
            "--project-id",
            "project-1",
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 2
    assert "--working-dir cannot be combined with --project-id" in result.output
    assert client_created is False


def test_remote_server_without_local_path_uses_server_default(monkeypatch):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--server",
            "https://remote.example",
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == []
    assert "projectId" not in fake.query_body
    assert "sessionId" not in fake.query_body
    assert "workingDirectory" not in fake.query_body


def test_explicit_project_skips_local_project_api_and_query_creates_session(
    monkeypatch,
):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--project-id",
            "project-1",
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == []
    assert fake.query_body["projectId"] == "project-1"
    assert "sessionId" not in fake.query_body
    assert "workingDirectory" not in fake.query_body


@pytest.mark.parametrize(
    ("session_args", "expected_session"),
    [
        (["--session-id", "session-1"], "session-1"),
        (["--resume", "session-2"], "session-2"),
    ],
)
def test_existing_session_skips_project_api(
    monkeypatch,
    session_args,
    expected_session,
):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        ["hello", *session_args, "--output-format", "json"],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == []
    assert fake.query_body["sessionId"] == expected_session
    assert "projectId" not in fake.query_body
    assert "workingDirectory" not in fake.query_body


def test_continue_cache_hit_skips_project_api(monkeypatch, tmp_path):
    fake = FakeClient()
    install_client(monkeypatch, fake)
    monkeypatch.setattr(
        cli_main.SessionCache,
        "get_last_session",
        lambda _self, _working_dir: "continued-session",
    )

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--continue",
            "--working-dir",
            str(tmp_path),
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake.project_calls == []
    assert fake.query_body["sessionId"] == "continued-session"
    assert "projectId" not in fake.query_body


def test_existing_session_cannot_be_combined_with_project_id(monkeypatch):
    client_created = False

    def create_client(**_kwargs):
        nonlocal client_created
        client_created = True
        return FakeClient()

    monkeypatch.setattr(cli_main, "ZkcodeClient", create_client)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--session-id",
            "session-1",
            "--project-id",
            "project-1",
        ],
    )

    assert result.exit_code == 2
    assert (
        "--project-id cannot be combined with an existing session"
        in result.output
    )
    assert client_created is False


def test_stream_query_never_sends_working_directory(monkeypatch, tmp_path):
    fake = FakeClient()
    install_client(monkeypatch, fake)

    result = runner.invoke(
        cli_main.app,
        [
            "hello",
            "--server",
            "https://remote.example",
            "--output-format",
            "stream-json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "workingDirectory" not in fake.query_body
