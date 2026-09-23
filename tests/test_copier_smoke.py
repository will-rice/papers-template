from pathlib import Path

from copier import run_copy


def test_template_renders_python_package(tmp_path: Path) -> None:
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
        defaults=True,
        unsafe=True,
    )
    assert (destination / "pyproject.toml").is_file()
    assert (destination / "src/papers_pipeline/__init__.py").is_file()
    answers = (destination / ".copier-answers.yml").read_text()
    assert "_src_path:" in answers
    assert "template_version: 0.1.0" in answers
