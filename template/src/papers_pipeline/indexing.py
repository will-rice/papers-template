from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from papers_pipeline.models import Paper


def write_index(root: Path, papers: Sequence[Paper]) -> Path:
    path = root / "README.md"
    content = _render_index(papers)
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _render_index(papers: Sequence[Paper]) -> str:
    rows = [
        "# Papers",
        "",
        "| Published | Identifier | Title | Source |",
        "| --- | --- | --- | --- |",
    ]
    for paper in sorted(papers, key=_paper_sort_key, reverse=True):
        rows.append(
            "| "
            + " | ".join(
                [
                    paper.published.isoformat(),
                    _escape_cell(paper.identifier),
                    f"[{_escape_cell(paper.title)}]({_escape_cell(paper.url)})",
                    _escape_cell(paper.source),
                ]
            )
            + " |"
        )
    return "\n".join(rows) + "\n"


def _paper_sort_key(paper: Paper) -> tuple[object, ...]:
    return (paper.published, paper.identifier)


def _escape_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
