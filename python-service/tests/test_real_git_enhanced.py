"""Real GitPython integration tests using an isolated temporary repository."""

from __future__ import annotations

import git
import pytest

from routers.git_enhanced import (
    BlameRequest,
    DiffRequest,
    LogRequest,
    git_blame,
    git_diff,
    git_log,
)
from services.git_enhanced_service import GitEnhancedService


@pytest.fixture
def committed_repo(tmp_path):
    repo = git.Repo.init(tmp_path)
    with repo.config_writer() as config:
        config.set_value("user", "name", "zkcode tests")
        config.set_value("user", "email", "tests@zkcode.local")

    source = tmp_path / "sample.txt"
    source.write_text("alpha\n", encoding="utf-8")
    repo.index.add(["sample.txt"])
    repo.index.commit("initial fixture")

    source.write_text("alpha\nbeta\n", encoding="utf-8")
    repo.index.add(["sample.txt"])
    repo.index.commit("add beta")
    return tmp_path


def test_real_git_service_diff_log_and_blame(committed_repo):
    service = GitEnhancedService()
    diff = service.semantic_diff(str(committed_repo))
    assert "sample.txt" in diff["summary"]
    assert "+beta" in diff["detailed"]
    assert diff["files_changed"] == 1

    log = service.enhanced_log(str(committed_repo), max_count=10)
    assert log["total"] == 2
    assert log["commits"][0]["message"] == "add beta"
    assert log["commits"][0]["files"] == ["sample.txt"]

    blame = service.file_blame(str(committed_repo), "sample.txt")
    assert blame["total_lines"] == 2
    assert [line["content"] for line in blame["lines"]] == ["alpha", "beta"]
    assert all(len(line["sha"]) == 8 for line in blame["lines"])


@pytest.mark.asyncio
async def test_real_git_router_success_and_fail_closed(committed_repo, tmp_path):
    diff = await git_diff(DiffRequest(repo_path=str(committed_repo)))
    assert diff.success is True
    assert diff.data["files_changed"] == 1

    log = await git_log(LogRequest(repo_path=str(committed_repo), max_count=1))
    assert log.success is True
    assert log.data["total"] == 1

    blame = await git_blame(
        BlameRequest(repo_path=str(committed_repo), file_path="sample.txt")
    )
    assert blame.success is True
    assert blame.data["total_lines"] == 2

    not_repo = tmp_path / "plain"
    not_repo.mkdir()
    invalid = await git_log(LogRequest(repo_path=str(not_repo)))
    assert invalid.success is False
    assert invalid.error_code == "INVALID_INPUT"

    bad_ref = await git_diff(
        DiffRequest(repo_path=str(committed_repo), ref1="missing-ref", ref2="HEAD")
    )
    assert bad_ref.success is False
    assert bad_ref.error_code == "INTERNAL_ERROR"


def test_git_service_path_guards(committed_repo, tmp_path):
    service = GitEnhancedService()
    assert service._validate_repo_path(str(committed_repo)) == str(committed_repo.resolve())
    with pytest.raises(ValueError, match="Unsafe repo path"):
        service._validate_repo_path("/")
    with pytest.raises(ValueError, match="Not a directory"):
        service._validate_repo_path(str(tmp_path / "absent"))
