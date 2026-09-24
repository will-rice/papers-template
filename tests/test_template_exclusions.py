from pathlib import Path
import shutil
import subprocess
import warnings

from copier import run_copy, run_update
import yaml  # type: ignore[import-untyped]
from copier.errors import DirtyLocalWarning


def _git(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _commit(repository: Path, message: str) -> str:
    _git(repository, "add", ".")
    _git(repository, "commit", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def _create_template_repository(source: Path) -> str:
    source.mkdir()
    tracked = subprocess.run(
        ["git", "ls-files", "copier.yml", "template"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    for relative in tracked:
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(relative, target)

    _git(source, "init", "--quiet")
    _git(source, "config", "user.name", "Template Test")
    _git(source, "config", "user.email", "template@example.test")
    initial_ref = _commit(source, "initial template")
    _git(source, "tag", "1.0.0", initial_ref)

    owned_file = source / "template/topic_plugin.py.jinja"
    owned_file.write_text(
        owned_file.read_text().replace(
            "repository plugin accepted paper",
            "updated repository plugin accepted paper",
        )
    )
    template_owned_collisions = (
        "papers/manual.md",
        "papers.csv",
        ".papers-state.yml",
        ".cache/downloads/response.bin",
        ".cache/parsers/document.bin",
        ".cache/models/model.bin",
        ".cache/tools/converter.bin",
    )
    for relative in template_owned_collisions:
        collision = source / "template" / relative
        collision.parent.mkdir(parents=True, exist_ok=True)
        collision.write_bytes(b"template update must not replace repository data\n")
        _git(source, "add", "--force", str(collision.relative_to(source)))
    updated_ref = _commit(source, "update template-owned file")
    _git(source, "tag", "2.0.0", updated_ref)
    return initial_ref


def test_copier_update_preserves_repository_owned_data(tmp_path: Path) -> None:
    source = tmp_path / "template-source"
    initial_ref = _create_template_repository(source)
    destination = tmp_path / "sample-papers"
    answers = {
        "project_name": "Sample Papers",
        "project_slug": "sample-papers",
        "topic_description": "sample topic",
        "template_version": "0.1.0",
    }

    with warnings.catch_warnings():
        warnings.simplefilter("error", DirtyLocalWarning)
        run_copy(
            str(source),
            destination,
            data=answers,
            vcs_ref=initial_ref,
            defaults=True,
            unsafe=True,
        )

    assert (destination / "papers.csv").read_bytes() == (
        b"identifier,title,abstract,authors,published,url,source,input_format,"
        b"input_url,categories,doi,arxiv_id\n"
    )
    assert (destination / ".papers-state.yml").read_bytes() == (
        b"cursors: {}\nfailures: {}\n"
    )

    _git(destination, "init", "--quiet")
    _git(destination, "config", "user.name", "Template Test")
    _git(destination, "config", "user.email", "template@example.test")
    _commit(destination, "render template")

    protected = {
        "papers/manual.md": b"\x00manual corpus\r\n",
        "papers.csv": b"\xffrepository inventory\r\n",
        ".papers-state.yml": b"cursors:\n  arxiv: next\nfailures: {}\n",
        ".cache/downloads/response.bin": b"\x00\xffdownload cache",
        ".cache/parsers/document.bin": b"\x01parser cache\r\n",
        ".cache/models/model.bin": b"\x02model cache\n",
        ".cache/tools/converter.bin": b"\x03tool cache\r\n",
        ".pytest_cache/marker.bin": b"\x04pytest cache\r\n",
        ".mypy_cache/marker.bin": b"\x05mypy cache\n",
        ".ruff_cache/marker.bin": b"\x06ruff cache\r\n",
    }
    for relative, content in protected.items():
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    _commit(destination, "add repository data")

    with warnings.catch_warnings():
        warnings.simplefilter("error", DirtyLocalWarning)
        run_update(destination, defaults=True, overwrite=True, unsafe=True)

    assert {
        relative: (destination / relative).read_bytes() for relative in protected
    } == protected
    assert {Path(relative).parts[0] for relative in protected} == set(
        yaml.safe_load(Path("copier.yml").read_text())["_skip_if_exists"]
    )
    assert (
        "updated repository plugin accepted paper"
        in (destination / "topic_plugin.py").read_text()
    )
    assert "_commit: 2.0.0" in (destination / ".copier-answers.yml").read_text()
