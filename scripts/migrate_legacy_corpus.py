"""Move a legacy papers repository's corpus into the template layout.

Legacy repositories keep ``papers.csv`` as ``arxiv_id,title,authors,
submitted,categories,url,abstract[,source]`` and markdown at
``papers/<year>/<id>.md``. Run this from the root of such a repository after
``copier copy`` has applied the template, while HEAD still holds the legacy
``papers.csv``::

    uv run python /path/to/papers-template/scripts/migrate_legacy_corpus.py

It rewrites ``papers.csv`` in the template schema, ``git mv``s every legacy
markdown file to the path the pipeline expects, rewrites in-corpus links,
removes the legacy per-year indexes, and writes an empty
``.papers-state.yml``. Identifiers come from the pipeline's own
``normalize`` and ``deduplicate``, so the first nightly run sees the corpus
as already generated.
"""

import collections
import csv
import io
import logging
import re
import subprocess
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from papers_pipeline.batching import expected_markdown, infer_backlog
from papers_pipeline.inventory import write_inventory
from papers_pipeline.models import Paper, PipelineState, SourceRecord
from papers_pipeline.normalize import clean, deduplicate, normalize
from papers_pipeline.state import save_state

LEGACY_REF = "HEAD"
# Legacy ID prefix -> template adapter name. Bare IDs are arXiv IDs.
SOURCES = {
    "": "arxiv",
    "s2": "semantic_scholar",
    "ss": "semantic_scholar",
    "dblp": "dblp",
    "biorxiv": "biorxiv_crossref",
}
ARXIV_DOI = re.compile(r"^10\.48550/arxiv\.(?P<arxiv_id>.+)$", re.IGNORECASE)
# ](../2024/<stem>.md) or ](<stem>.md)
LEGACY_LINK = re.compile(r"\]\((?:\.\./\d{4}/)?(?P<stem>[^()\s/]+)\.md\)")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    root = Path.cwd()
    legacy_csv = git(root, "show", f"{LEGACY_REF}:papers.csv")
    rows = list(csv.DictReader(io.StringIO(legacy_csv)))
    legacy = {
        legacy_stem(row["arxiv_id"]): normalize(legacy_record(row)) for row in rows
    }
    papers = deduplicate(legacy.values())
    targets = plan_targets(root, legacy, papers)

    markdown = {
        path.stem: path
        for path in (root / "papers").glob("*/*.md")
        if path.name != "README.md"
    }
    unknown = sorted(set(markdown) - set(targets))
    if unknown:
        raise ValueError(f"legacy markdown without a papers.csv row: {unknown}")

    for stem, source in sorted(markdown.items()):
        target = targets[stem]
        if target.exists():
            # Another legacy row of the same paper already claimed this file.
            git(root, "rm", "-q", str(source.relative_to(root)))
        else:
            git(
                root, "mv", str(source.relative_to(root)), str(target.relative_to(root))
            )

    for path in (root / "papers").glob("*.md"):
        text = path.read_text(encoding="utf-8")
        rewritten = LEGACY_LINK.sub(
            lambda match: (
                f"]({targets[match['stem']].name})"
                if match["stem"] in targets
                else match[0]
            ),
            text,
        )
        if rewritten != text:
            path.write_text(rewritten, encoding="utf-8")

    for index in [
        root / "papers" / "README.md",
        *(root / "papers").glob("*/README.md"),
    ]:
        if index.exists():
            git(root, "rm", "-q", str(index.relative_to(root)))
    # Legacy year directories hold only markdown; rmdir fails if any remains.
    for year in (root / "papers").glob("*/"):
        year.rmdir()

    write_inventory(root / "papers.csv", papers)
    save_state(root / ".papers-state.yml", PipelineState())

    backlog = infer_backlog(papers, root)
    pending = collections.Counter(paper.source for paper in backlog.pending)
    logging.info(
        "Migrated %d legacy rows into %d papers: %d generated, %d pending %s",
        len(rows),
        len(papers),
        len(backlog.generated),
        len(backlog.pending),
        dict(pending),
    )


def legacy_stem(legacy_id: str) -> str:
    """Return the legacy markdown file stem for a legacy ID."""
    return legacy_id.replace("/", "_")


def legacy_record(row: dict[str, str]) -> SourceRecord:
    prefix, _, source_id = row["arxiv_id"].rpartition(":")
    if prefix not in SOURCES:
        raise ValueError(f"unknown legacy ID prefix: {row['arxiv_id']}")
    source = SOURCES[prefix]
    url = row["url"]
    doi = (
        url.removeprefix("https://doi.org/")
        if url.startswith("https://doi.org/")
        else None
    )
    arxiv_doi = ARXIV_DOI.match(doi) if doi else None
    if source == "arxiv":
        arxiv_id: str | None = source_id
    elif arxiv_doi:
        arxiv_id = arxiv_doi["arxiv_id"]
    else:
        arxiv_id = None
    return SourceRecord(
        source=source,
        source_id=source_id,
        title=row["title"],
        abstract=row["abstract"],
        authors=tuple(
            author.strip() for author in row["authors"].split(",") if author.strip()
        ),
        published=datetime.fromisoformat(row["submitted"]).replace(tzinfo=UTC),
        url=url,
        input_format="pdf" if source == "arxiv" else "html",
        input_url=f"https://arxiv.org/pdf/{source_id}" if source == "arxiv" else url,
        categories=tuple(row["categories"].split()),
        doi=doi,
        arxiv_id=arxiv_id,
    )


def plan_targets(
    root: Path, legacy: dict[str, Paper], papers: list[Paper]
) -> dict[str, Path]:
    """Map each legacy stem to the markdown path of the paper it became."""
    kept = {paper.identifier: paper for paper in papers}
    component_keys = linked_keys(legacy.values())
    targets: dict[str, Path] = {}
    for stem, paper in legacy.items():
        survivor = kept.get(paper.identifier)
        if survivor is None:
            # deduplicate merged this row, possibly through other rows, into
            # the one survivor that shares a key with its linked rows.
            keys = component_keys[paper.identifier]
            matches = [
                candidate for candidate in papers if merge_keys(candidate) & keys
            ]
            if len(matches) != 1:
                raise ValueError(f"cannot place merged legacy paper {stem}: {matches}")
            survivor = matches[0]
        targets[stem] = expected_markdown(root, survivor)
    return targets


def linked_keys(papers: Iterable[Paper]) -> dict[str, set[str]]:
    """Return, per identifier, every merge key reachable through shared keys."""
    papers = list(papers)
    parent = list(range(len(papers)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    owner: dict[str, int] = {}
    for index, paper in enumerate(papers):
        for key in merge_keys(paper):
            parent[find(index)] = find(owner.setdefault(key, index))
    keys: dict[int, set[str]] = collections.defaultdict(set)
    for index, paper in enumerate(papers):
        keys[find(index)] |= merge_keys(paper)
    return {paper.identifier: keys[find(index)] for index, paper in enumerate(papers)}


def merge_keys(paper: Paper) -> set[str]:
    """Keys deduplicate merges on: arXiv ID, DOI, or title and date."""
    keys = {f"title:{clean(paper.title).casefold()}|{paper.published.date()}"}
    if paper.arxiv_id:
        keys.add(f"arxiv:{re.sub(r'v\d+$', '', paper.arxiv_id)}")
    if paper.doi:
        keys.add(f"doi:{paper.doi}")
    return keys


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


if __name__ == "__main__":
    main()
