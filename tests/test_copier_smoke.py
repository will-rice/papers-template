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


def test_template_links_a_shared_alphaxiv_folder(tmp_path: Path) -> None:
    source = tmp_path / "template-source"
    source_ref = _clean_template_source(source)
    destination = tmp_path / "sample-papers"
    folder_id = "019f564f-9833-7a27-b1ea-aec52fe9ef9f"
    run_copy(
        str(source),
        destination,
        data={
            "project_name": "Sample Papers",
            "project_slug": "sample-papers",
            "topic_description": "sample topic",
            "alphaxiv_folder_id": folder_id,
        },
        vcs_ref=source_ref,
        defaults=True,
        unsafe=True,
    )

    readme = (destination / "README.md").read_text(encoding="utf-8")
    assert readme.startswith(
        "# Sample Papers\n\n"
        "Standalone paper discovery and conversion for sample topic.\n\n"
        "Browse the collection on alphaXiv:\n"
        f"https://www.alphaxiv.org/shared/folder/{folder_id}\n\n"
        "<!-- papers-index:start -->"
    )


def test_template_renders_python_package(tmp_path: Path) -> None:
    source = tmp_path / "template-source"
    source_ref = _clean_template_source(source)
    destination = tmp_path / "sample-papers"
    with warnings.catch_warnings():
        warnings.simplefilter("error", DirtyLocalWarning)
        run_copy(
            str(source),
            destination,
            data={
                "project_name": 'Sample "Papers": Demo',
                "project_slug": "sample-papers",
                "topic_description": 'sample "topic": demo',
                "template_version": "0.1.0",
            },
            vcs_ref=source_ref,
            defaults=True,
            unsafe=True,
        )
    assert (destination / "pyproject.toml").is_file()
    assert (destination / "src/papers_pipeline/__init__.py").is_file()
    assert (destination / "src/papers_pipeline/cli.py").is_file()
    assert (destination / "papers.yml").is_file()
    assert (destination / "papers.schema.json").is_file()
    assert (destination / "topic_plugin.py").is_file()
    assert (destination / "llms.txt").read_text(encoding="utf-8").startswith("# ")
    assert "alphaxiv.org/shared/folder" not in (destination / "README.md").read_text(
        encoding="utf-8"
    )
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
            "from papers_pipeline.cli import app; raise SystemExit(app(['validate', '--config', 'papers.yml', '--config-only']))",
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
            "timeout_seconds": 1800,
            "html_cost": 2,
            "latex_cost": 4,
            "pdf_cost": 20,
        },
        "concurrency": {"html": 4, "latex": 2, "pdf": 1},
    }
    answers = (destination / ".copier-answers.yml").read_text()
    parsed_answers = yaml.safe_load(answers)
    assert Path(parsed_answers["_src_path"]).resolve() == source.resolve()
    assert parsed_answers["_commit"]
    assert "template_version: 0.1.0" in answers


def test_template_renders_protected_state_files(tmp_path: Path) -> None:
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

    papers_csv = destination / "papers.csv"
    state_file = destination / ".papers-state.yml"
    assert papers_csv.is_file()
    assert state_file.is_file()
    assert papers_csv.read_text() == (
        "identifier,title,abstract,authors,published,url,source,input_format,input_url,categories,doi,arxiv_id\n"
    )
    assert state_file.read_text() == "continuations: {}\nfailures: {}\n"


def test_template_update_merges_template_edits_beside_generated_files(
    tmp_path: Path,
) -> None:
    source = tmp_path / "template-source"
    _clean_template_source(source)
    _git(source, "tag", "v1.0.0")
    template_readme = source / "template" / "README.md.jinja"
    template_readme.write_text(
        template_readme.read_text(encoding="utf-8").replace(
            "<!-- papers-index:start -->",
            "A new template line.\n\n<!-- papers-index:start -->",
        ),
        encoding="utf-8",
    )
    # The release also adds a dependency, editing the lock where a repository's
    # own dependencies change it.
    template_pyproject = source / "template" / "pyproject.toml.jinja"
    template_pyproject.write_text(
        template_pyproject.read_text(encoding="utf-8").replace(
            '  "httpx2>=2.5,<3",\n', '  "httpx2>=2.5,<3",\n  "idna>=3,<4",\n'
        ),
        encoding="utf-8",
    )
    template_lock = source / "template" / "uv.lock.jinja"
    template_lock.write_text(
        template_lock.read_text(encoding="utf-8")
        .replace(
            'source = { editable = "." }\ndependencies = [\n'
            '    { name = "httpx" },\n    { name = "httpx2" },\n',
            'source = { editable = "." }\ndependencies = [\n'
            '    { name = "httpx" },\n    { name = "httpx2" },\n'
            '    { name = "idna" },\n',
        )
        .replace(
            '    { name = "httpx2", specifier = ">=2.5,<3" },\n',
            '    { name = "httpx2", specifier = ">=2.5,<3" },\n'
            '    { name = "idna", specifier = ">=3,<4" },\n',
        ),
        encoding="utf-8",
    )
    _git(source, "commit", "--quiet", "-am", "edit beside generated files")
    _git(source, "tag", "v1.1.0")
    destination = tmp_path / "sample-papers"
    run_copy(
        str(source),
        destination,
        data={
            "project_name": "Sample Papers",
            "project_slug": "sample-papers",
            "topic_description": "sample topic",
        },
        vcs_ref="v1.0.0",
        defaults=True,
        unsafe=True,
    )
    readme = destination / "README.md"
    rendered = readme.read_text(encoding="utf-8")
    start = rendered.index("<!-- papers-index:start -->\n") + len(
        "<!-- papers-index:start -->\n"
    )
    end = rendered.index("<!-- papers-index:end -->")
    generated = "\n# Papers\n\nThe 1 most recent of 1 papers.\n\n"
    readme.write_text(rendered[:start] + generated + rendered[end:], encoding="utf-8")
    # A repository's own dependency rewrites part of uv.lock, as a topic
    # plugin's embedding model does.
    subprocess.run(
        ["uv", "add", "--quiet", "--no-sync", "six>=1.16,<2"],
        cwd=destination,
        check=True,
    )
    _git(destination, "init", "--quiet")
    _git(destination, "config", "user.name", "Template Test")
    _git(destination, "config", "user.email", "template@example.test")
    _git(destination, "add", ".")
    _git(destination, "commit", "--quiet", "-m", "generated repository")

    environment = os.environ.copy()
    environment["TEMPLATE_REF"] = "v1.1.0"
    environment["PRE_COMMIT_HOME"] = str(tmp_path / "pre-commit-home")
    # The update workflow installs Copier beside uv; this environment has it.
    environment["PATH"] = (
        f"{Path(sys.executable).parent}{os.pathsep}{os.environ['PATH']}"
    )
    subprocess.run(
        [str(destination / ".github" / "scripts" / "template-update.sh")],
        cwd=destination,
        env=environment,
        check=True,
    )

    assert (
        "A new template line.\n\n<!-- papers-index:start -->\n"
        + generated
        + "<!-- papers-index:end -->"
    ) in readme.read_text(encoding="utf-8")
    pyproject = (destination / "pyproject.toml").read_text(encoding="utf-8")
    assert '"idna>=3,<4"' in pyproject
    assert '"six>=1.16,<2"' in pyproject
    lock = (destination / "uv.lock").read_text(encoding="utf-8")
    assert '{ name = "idna", specifier = ">=3,<4" }' in lock
    assert '{ name = "six", specifier = ">=1.16,<2" }' in lock


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
    pre_commit_home = tmp_path / "pre-commit-home"
    pre_commit_home.mkdir()
    provisioning_environment = os.environ.copy()
    provisioning_environment.pop("UV_OFFLINE", None)
    subprocess.run(
        ["uv", "sync", "--locked", "--extra", "dev"],
        cwd=destination,
        env=provisioning_environment,
        check=True,
    )
    offline_environment = {
        key: value
        for key, value in os.environ.items()
        if not key.lower().endswith("_proxy")
    }
    offline_environment["UV_OFFLINE"] = "1"
    offline_environment["PRE_COMMIT_HOME"] = str(pre_commit_home)
    subprocess.run(
        ["uv", "run", "pre-commit", "run", "--all-files"],
        cwd=destination,
        env=offline_environment,
        check=True,
    )
    assert any(pre_commit_home.iterdir())
    subprocess.run(
        ["uv", "run", "pytest"],
        cwd=destination,
        env=offline_environment,
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
    assert readme.count("<!-- papers-index:start -->") == 1
    assert readme.count("<!-- papers-index:end -->") == 1
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
    assert "`will-rice/papers-template`" in readme
    assert "Do not begin a migration" in readme

    before = readme.encode()
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(destination / "src")
    command = (
        "from pathlib import Path; "
        "from papers_pipeline.indexing import write_index; "
        "write_index(Path.cwd(), ())"
    )
    subprocess.run(
        [sys.executable, "-c", command],
        cwd=destination,
        env=environment,
        check=True,
    )
    after = (destination / "README.md").read_bytes()
    start = before.index(b"<!-- papers-index:start -->")
    end = before.index(b"<!-- papers-index:end -->")
    after_start = after.index(b"<!-- papers-index:start -->")
    after_end = after.index(b"<!-- papers-index:end -->")
    assert after[:start] == before[:start]
    assert after[after_end:] == before[end:]

    subprocess.run(
        [sys.executable, "-c", command],
        cwd=destination,
        env=environment,
        check=True,
    )
    assert (destination / "README.md").read_bytes() == after
    assert after_start == start
