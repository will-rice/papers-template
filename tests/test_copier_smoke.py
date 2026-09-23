from pathlib import Path
import os
import subprocess
import sys

from copier import run_copy
import yaml  # type: ignore[import-untyped]


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
    assert "_src_path:" in answers
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
