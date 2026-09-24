from pathlib import Path
import os
import shutil
import stat
import subprocess
import sys
import warnings

from copier import run_copy
from copier.errors import DirtyLocalWarning
import yaml


def _git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _clean_template_source(source: Path) -> str:
    source.mkdir()
    tracked = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "copier.yml",
            "template",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    for relative in tracked:
        if not Path(relative).is_file():
            continue
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(relative, target)

    _git(source, "init", "--quiet")
    _git(source, "config", "user.name", "Template Test")
    _git(source, "config", "user.email", "template@example.test")
    _git(source, "add", ".")
    _git(source, "commit", "--quiet", "-m", "test template")
    return _git(source, "rev-parse", "HEAD")


def test_template_renders_python_package(tmp_path: Path) -> None:
    destination = tmp_path / "sample-papers"
    run_copy(
        ".",
        destination,
        data={
            "project_name": 'Sample "Papers": Demo',
            "project_slug": "sample-papers",
            "topic_description": 'sample "topic": demo',
            "template_version": "0.1.0",
        },
        vcs_ref="HEAD",
        defaults=True,
        unsafe=True,
    )
    assert (destination / "pyproject.toml").is_file()
    assert (destination / "src/papers_pipeline/__init__.py").is_file()
    assert (destination / "src/papers_pipeline/cli.py").is_file()
    assert (destination / "papers.yml").is_file()
    assert (destination / "papers.schema.json").is_file()
    assert (destination / "topic_plugin.py").is_file()
    assert (destination / ".github/workflows/ci.yml").is_file()
    assert (destination / ".github/workflows/nightly.yml").is_file()
    assert (destination / ".github/workflows/format-corpus.yml").is_file()
    nightly_script = destination / ".github/scripts/nightly.sh"
    assert nightly_script.is_file()
    assert nightly_script.stat().st_mode & stat.S_IXUSR
    env = os.environ.copy()
    env["PYTHONPATH"] = str(destination / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from papers_pipeline.cli import app; raise SystemExit(app(['validate', '--config', 'papers.yml']))",
        ],
        cwd=destination,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert completed.stdout == "valid: papers.yml\n"
    rendered_config = yaml.safe_load((destination / "papers.yml").read_text())
    assert rendered_config == {
        "repository": {
            "name": 'Sample "Papers": Demo',
            "slug": "sample-papers",
            "description": 'sample "topic": demo',
        },
        "adapters": [
            {
                "name": "arxiv",
                "enabled": True,
                "secret_env": None,
                "lookback_days": 7,
                "page_size": 100,
                "max_pages": 5,
                "max_results": 500,
                "filters": {},
            }
        ],
        "topic": {
            "include_any": ['sample "topic": demo'],
            "include_all": [],
            "exclude_any": [],
            "categories": [],
            "plugin": None,
        },
        "fetch": {
            "request_timeout_seconds": 30,
            "retries": 3,
            "backoff_seconds": 1,
            "total_deadline_seconds": 900,
        },
        "conversion": {
            "max_batches_per_run": 4,
            "max_papers": 10,
            "max_cost": 100,
            "html_cost": 2,
            "latex_cost": 4,
            "pdf_cost": 20,
        },
        "concurrency": {"html": 4, "latex": 2, "pdf": 1},
    }
    answers = (destination / ".copier-answers.yml").read_text()
    parsed_answers = yaml.safe_load(answers)
    assert parsed_answers["_src_path"] == "."
    assert parsed_answers["_commit"]
    assert "template_version: 0.1.0" in answers


def test_template_renders_protected_state_files(tmp_path: Path) -> None:
    destination = tmp_path / "sample-papers"
    run_copy(
        ".",
        destination,
        data={
            "project_name": "Sample Papers",
            "project_slug": "sample-papers",
            "topic_description": "sample topic",
            "template_version": "0.1.0",
        },
        vcs_ref="HEAD",
        defaults=True,
        unsafe=True,
    )

    papers_csv = destination / "papers.csv"
    state_file = destination / ".papers-state.yml"
    assert papers_csv.is_file()
    assert state_file.is_file()
    assert papers_csv.read_text() == (
        "identifier,title,abstract,authors,published,url,source,input_format,input_url,categories,doi,arxiv_id\n"
    )
    assert state_file.read_text() == "cursors: {}\nfailures: {}\n"


def test_generated_repository_passes_offline_suite(tmp_path: Path) -> None:
    source = tmp_path / "template-source"
    source_ref = _clean_template_source(source)
    destination = tmp_path / "sample-papers"
    with warnings.catch_warnings():
        warnings.simplefilter("error", DirtyLocalWarning)
        run_copy(
            str(source),
            destination,
            data={
                "project_name": "Sample Papers",
                "project_slug": "sample-papers",
                "topic_description": "sample topic",
                "template_version": "0.1.0",
            },
            vcs_ref=source_ref,
            defaults=True,
            unsafe=True,
        )

    _git(destination, "init", "--quiet")
    _git(destination, "add", ".")
    environment = os.environ.copy()
    environment["UV_OFFLINE"] = "true"
    subprocess.run(
        ["uv", "sync", "--locked", "--extra", "dev"],
        cwd=destination,
        env=environment,
        check=True,
    )
    subprocess.run(
        ["uv", "run", "pre-commit", "run", "--all-files"],
        cwd=destination,
        env=environment,
        check=True,
    )
    subprocess.run(
        ["uv", "run", "pytest"],
        cwd=destination,
        env=environment,
        check=True,
    )


def test_generated_readme_documents_operations_and_migration_gate(
    tmp_path: Path,
) -> None:
    source = tmp_path / "template-source"
    source_ref = _clean_template_source(source)
    destination = tmp_path / "sample-papers"
    with warnings.catch_warnings():
        warnings.simplefilter("error", DirtyLocalWarning)
        run_copy(
            str(source),
            destination,
            data={
                "project_name": "Sample Papers",
                "project_slug": "sample-papers",
                "topic_description": "sample topic",
                "template_version": "0.1.0",
            },
            vcs_ref=source_ref,
            defaults=True,
            unsafe=True,
        )

    readme = (destination / "README.md").read_text(encoding="utf-8")
    for heading in (
        "# Sample Papers",
        "## Architecture",
        "## Configuration",
        "## Run locally",
        "## State, backlog, and recovery",
        "## Formatting",
        "## Automation and summaries",
        "## Updating from the template",
        "## Ownership boundary",
        "## Migration gate",
    ):
        assert heading in readme
    assert "PDF concurrency is always exactly 1" in readme
    assert "Do not begin a migration" in readme
