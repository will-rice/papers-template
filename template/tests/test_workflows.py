from __future__ import annotations

import re
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped, unused-ignore]

from papers_pipeline.pipeline import PipelinePaths, _managed_paths


class WorkflowLoader(yaml.SafeLoader):  # type: ignore[misc]
    """Load workflow YAML without treating the key `on` as a boolean."""


WorkflowLoader.yaml_implicit_resolvers = {
    key: list(resolvers)
    for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for first_character in "OoYyNn":
    WorkflowLoader.yaml_implicit_resolvers[first_character] = [
        resolver
        for resolver in WorkflowLoader.yaml_implicit_resolvers.get(first_character, [])
        if resolver[0] != "tag:yaml.org,2002:bool"
    ]

WORKFLOWS = Path(".github/workflows")
SCRIPTS = Path(".github/scripts")
PINNED_ACTION = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
MANAGED_PATHS = (
    "papers",
    "papers.csv",
    ".papers-state.yml",
    "README.md",
    ".convert-batch",
    "inputs",
)


def workflow(name: str) -> dict[str, Any]:
    loaded = yaml.load(
        (WORKFLOWS / name).read_text(encoding="utf-8"),
        Loader=WorkflowLoader,
    )
    assert isinstance(loaded, dict)
    return loaded


def test_actions_are_sha_pinned_and_jobs_and_steps_have_timeouts() -> None:
    paths = sorted(WORKFLOWS.glob("*.yml"))
    assert {path.name for path in paths} == {
        "ci.yml",
        "format-corpus.yml",
        "nightly.yml",
    }
    for path in paths:
        data = workflow(path.name)
        for job in data["jobs"].values():
            assert isinstance(job.get("timeout-minutes"), int)
            for step in job["steps"]:
                assert isinstance(step.get("timeout-minutes"), int)
                if "uses" in step:
                    assert PINNED_ACTION.fullmatch(step["uses"])


def test_ci_triggers_and_permissions_are_read_only() -> None:
    data = workflow("ci.yml")
    assert data["on"] == {
        "pull_request": None,
        "push": {"branches": ["main"]},
    }
    assert data["permissions"] == {"contents": "read"}


def test_all_python_workflows_use_locked_dependencies_and_preflight() -> None:
    for name in ("ci.yml", "nightly.yml", "format-corpus.yml"):
        text = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert "uv==" in text
        assert "uv sync --locked --extra dev" in text

    nightly = (WORKFLOWS / "nightly.yml").read_text(encoding="utf-8")
    assert nightly.index("papers-pipeline validate") < nightly.index("nightly.sh")


def test_nightly_has_non_overlapping_mutation_concurrency() -> None:
    data = workflow("nightly.yml")
    assert set(data["on"]) == {"schedule", "workflow_dispatch"}
    assert data["concurrency"] == {
        "group": "nightly-papers",
        "cancel-in-progress": False,
    }
    assert data["permissions"] == {"contents": "write"}


def test_nightly_never_runs_a_complete_corpus_glob() -> None:
    text = (WORKFLOWS / "nightly.yml").read_text(encoding="utf-8")
    assert "format-corpus" not in text
    assert "papers/*.md" not in text
    assert "papers/**/*.md" not in text


def test_format_corpus_is_manual_only_and_opens_a_pr() -> None:
    data = workflow("format-corpus.yml")
    assert set(data["on"]) == {"workflow_dispatch"}
    assert data["permissions"] == {"contents": "read"}
    assert "permissions" not in data["jobs"]["plan"]
    assert "permissions" not in data["jobs"]["format"]
    assert data["jobs"]["combine"]["permissions"] == {
        "contents": "write",
        "pull-requests": "write",
    }
    assert data["concurrency"]["cancel-in-progress"] is False
    text = (WORKFLOWS / "format-corpus.yml").read_text(encoding="utf-8")
    assert "git push" not in text
    assert "peter-evans/create-pull-request@" in text
    assert "base: ${{ needs.plan.outputs.base_branch }}" in text


def test_feature_branch_dispatch_cannot_contaminate_formatting_pr() -> None:
    data = workflow("format-corpus.yml")
    text = (WORKFLOWS / "format-corpus.yml").read_text(encoding="utf-8")
    assert "github.sha" not in text
    assert "github.ref" not in text
    jobs = data["jobs"]
    plan = jobs["plan"]
    plan_checkout = plan["steps"][0]
    assert plan_checkout["with"]["ref"] == (
        "${{ github.event.repository.default_branch }}"
    )
    assert plan["outputs"] == {
        "base_branch": "${{ github.event.repository.default_branch }}",
        "base_sha": "${{ steps.base.outputs.sha }}",
        "matrix": "${{ steps.matrix.outputs.matrix }}",
    }
    assert jobs["format"]["needs"] == "plan"
    assert set(jobs["combine"]["needs"]) == {"plan", "format"}
    for job_name in ("format", "combine"):
        checkout = jobs[job_name]["steps"][0]
        assert checkout["with"]["ref"] == "${{ needs.plan.outputs.base_sha }}"


def test_format_corpus_validates_and_builds_deterministic_shards() -> None:
    data = workflow("format-corpus.yml")
    plan = data["jobs"]["plan"]
    matrix_script = next(
        step["run"] for step in plan["steps"] if step.get("id") == "matrix"
    )
    assert "1 <= count <= 32" in matrix_script
    assert "for index in range(count)" in matrix_script
    assert data["jobs"]["format"]["strategy"]["fail-fast"] is False


def test_format_corpus_combines_binary_patches_in_numeric_order() -> None:
    text = (WORKFLOWS / "format-corpus.yml").read_text(encoding="utf-8")
    assert "git diff --binary" in text
    assert "sort -zV" in text
    assert '[[ -s "$patch" ]]' in text
    assert "git apply --index" in text


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )


def _nightly_scenario(tmp_path: Path, *, dirty_path: str | None) -> tuple[int, int]:
    origin = tmp_path / "origin.git"
    work = tmp_path / "work"
    fake_bin = tmp_path / "bin"
    origin.mkdir()
    _git(origin, "init", "--bare", "-q")
    work.mkdir()
    _git(work, "init", "-q", "-b", "main")
    _git(work, "config", "user.name", "Test")
    _git(work, "config", "user.email", "test@example.test")
    (work / "papers.csv").write_text("initial\n", encoding="utf-8")
    (work / ".gitignore").write_text(".cache/\n", encoding="utf-8")
    _git(work, "add", "papers.csv", ".gitignore")
    _git(work, "commit", "-q", "-m", "initial")
    _git(work, "remote", "add", "origin", str(origin))
    _git(work, "push", "-q", "-u", "origin", "main")

    fake_bin.mkdir()
    uv = fake_bin / "uv"
    dirty_command = ""
    if dirty_path is not None:
        target = (
            f"{dirty_path}/leftover"
            if dirty_path in {"papers", ".convert-batch", "inputs", ".cache"}
            else dirty_path
        )
        (work / target).parent.mkdir(parents=True, exist_ok=True)
        dirty_command = f'printf "inconsistent\\n" >> "{target}"\n'
    uv.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        'printf "committed\\n" >> papers.csv\n'
        "git add papers.csv\n"
        'git commit -q -m "pipeline batch"\n'
        f"{dirty_command}"
        "exit 23\n",
        encoding="utf-8",
    )
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    script = SCRIPTS / "nightly.sh"
    git_executable = shutil.which("git")
    bash_executable = shutil.which("bash")
    assert git_executable is not None and bash_executable is not None
    completed = subprocess.run(
        [str(script.resolve())],
        cwd=work,
        env={
            "PATH": (
                f"{fake_bin}:{Path(git_executable).parent}:"
                f"{Path(bash_executable).parent}"
            )
        },
        check=False,
    )
    remote_count = int(
        _git(
            tmp_path,
            f"--git-dir={origin}",
            "rev-list",
            "--count",
            "refs/heads/main",
        ).stdout.strip()
    )
    return completed.returncode, remote_count


@pytest.mark.parametrize("dirty_path", MANAGED_PATHS)
def test_nightly_rejects_dirty_pipeline_paths_after_failure(
    tmp_path: Path,
    dirty_path: str,
) -> None:
    status, remote_count = _nightly_scenario(tmp_path, dirty_path=dirty_path)
    assert status != 0
    assert remote_count == 1


@pytest.mark.parametrize("dirty_path", [None, ".cache/http/response"])
def test_nightly_pushes_consistent_commits_with_clean_or_ignored_cache(
    tmp_path: Path,
    dirty_path: str | None,
) -> None:
    status, remote_count = _nightly_scenario(tmp_path, dirty_path=dirty_path)
    assert status != 0
    assert remote_count == 2


def test_nightly_guard_matches_pipeline_managed_paths(tmp_path: Path) -> None:
    paths = PipelinePaths(
        root=tmp_path,
        config=tmp_path / "papers.yml",
        state=tmp_path / ".papers-state.yml",
        inventory=tmp_path / "papers.csv",
        summary=None,
    )
    assert tuple(path.relative_to(tmp_path).as_posix() for path in _managed_paths(paths)) == (
        "papers.csv",
        ".papers-state.yml",
        "README.md",
        "papers",
        ".convert-batch",
        "inputs",
    )
    script = (SCRIPTS / "nightly.sh").read_text(encoding="utf-8")
    for path in MANAGED_PATHS:
        assert f'"{path}"' in script


def test_nightly_script_is_executable() -> None:
    script = SCRIPTS / "nightly.sh"
    assert script.stat().st_mode & stat.S_IXUSR
