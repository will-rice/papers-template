"""Move a legacy papers repository's corpus into the template layout.

Legacy repositories keep ``papers.csv`` as ``arxiv_id,title,authors,
submitted,categories,url,abstract[,source]`` and markdown at
``papers/<year>/<id>.md``. Run this from the root of such a repository after
``copier copy`` has applied the template, while HEAD still holds the legacy
``papers.csv``::

    uv run python /path/to/papers-template/scripts/migrate_legacy_corpus.py

It rewrites ``papers.csv`` in the template schema, ``git mv``s every legacy
markdown file to the path the pipeline expects, rewrites in-corpus links,
replaces legacy front matter with the pipeline's, removes the legacy
per-year indexes, and writes an empty ``.papers-state.yml``. Identifiers come
from the pipeline's own ``normalize`` and ``deduplicate``, so the first nightly
run sees every converted paper as already generated.

Papers without legacy markdown get conversion inputs the nightly can convert,
never a landing page: arXiv papers (including rows whose DOI is an arXiv DOI)
convert from the arXiv PDF and bioRxiv papers from the bioRxiv PDF, both
derived offline; a legacy URL whose path ends in ``.pdf`` converts as a PDF.
Semantic Scholar rows without markdown whose URL is a landing page are
resolved with the
Semantic Scholar Graph API batch endpoint (one request per 500 papers; set
``SEMANTIC_SCHOLAR_API_KEY`` to send an API key, otherwise the shared
unauthenticated rate limit applies and HTTP 429 aborts the migration). A paper
with an arXiv ID converts from the arXiv PDF, one with an open-access PDF from
that PDF, and one with neither gets a ``.fixme.txt`` marker in the pipeline's
format, so the nightly counts it as blocked instead of converting its landing
page.
"""

import collections
import csv
import io
import logging
import os
import re
import subprocess
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

from papers_pipeline.adapters.biorxiv_crossref import biorxiv_pdf_url
from papers_pipeline.batching import expected_markdown, infer_backlog
from papers_pipeline.convert import write_fixme
from papers_pipeline.front_matter import write_front_matter
from papers_pipeline.inventory import write_inventory
from papers_pipeline.models import FailureAttempt, Paper, PipelineState, SourceRecord
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
# ](../2024/<stem>.md), ](./<stem>.md) or ](<stem>.md)
LEGACY_LINK = re.compile(r"\]\((?:\.\./\d{4}/|\./)?(?P<stem>[^()\s/]+)\.md\)")
S2_BATCH_URL = "https://api.semanticscholar.org/graph/v1/paper/batch"
S2_BATCH_SIZE = 500
S2_TIMEOUT_SECONDS = 60.0
NO_FULL_TEXT = "no open-access full text: Semantic Scholar lists no arXiv ID or PDF"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    with httpx.Client(
        timeout=S2_TIMEOUT_SECONDS,
        headers={"x-api-key": api_key} if api_key else {},
    ) as client:
        migrate(Path.cwd(), client)


def migrate(root: Path, client: httpx.Client) -> None:
    """Migrate the legacy corpus in ``root``, resolving inputs through ``client``."""
    legacy_csv = git(root, "show", f"{LEGACY_REF}:papers.csv")
    rows = list(csv.DictReader(io.StringIO(legacy_csv)))
    markdown = {
        path.stem: path
        for path in (root / "papers").glob("*/*.md")
        if path.name != "README.md"
    }
    # Resolve inputs before deduplicate: an arXiv ID found here changes which
    # rows merge and which identifier survives.
    records, no_full_text = resolve_semantic_scholar(
        client,
        {legacy_stem(row["arxiv_id"]): legacy_record(row) for row in rows},
        converted=set(markdown),
    )
    legacy = {stem: normalize(record) for stem, record in records.items()}
    papers = deduplicate(legacy.values())
    targets = plan_targets(root, legacy, papers)

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
    # Replace legacy front matter with the pipeline's, so every paper is
    # self-describing in one format.
    write_front_matter(root, papers)
    save_state(root / ".papers-state.yml", PipelineState())

    now = datetime.now(UTC)
    for paper in infer_backlog(papers, root).pending:
        if paper.input_url in no_full_text:
            write_fixme(
                expected_markdown(root, paper).with_suffix(".fixme.txt"),
                identifier=paper.identifier,
                latest_error=NO_FULL_TEXT,
                attempts=[FailureAttempt(occurred_at=now, error=NO_FULL_TEXT)],
            )

    backlog = infer_backlog(papers, root)
    pending = collections.Counter(paper.source for paper in backlog.pending)
    logging.info(
        "Migrated %d legacy rows into %d papers: %d generated, %d blocked"
        " without full text, %d pending %s",
        len(rows),
        len(papers),
        len(backlog.generated),
        len(backlog.blocked),
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
    if arxiv_id:
        input_format, input_url = "pdf", arxiv_pdf_url(arxiv_id)
    elif source == "biorxiv_crossref":
        if doi is None:
            raise ValueError(f"bioRxiv row without a DOI URL: {row['arxiv_id']}")
        input_format, input_url = "pdf", biorxiv_pdf_url(doi)
    else:
        # dblp keeps its ee link, as the dblp adapter does, and
        # resolve_semantic_scholar replaces Semantic Scholar landing pages.
        # Either may already be a direct PDF.
        is_pdf = urlparse(url).path.casefold().endswith(".pdf")
        input_format, input_url = "pdf" if is_pdf else "html", url
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
        input_format=input_format,
        input_url=input_url,
        categories=tuple(row["categories"].split()),
        doi=doi,
        arxiv_id=arxiv_id,
    )


def arxiv_pdf_url(arxiv_id: str) -> str:
    return f"https://arxiv.org/pdf/{arxiv_id}"


def resolve_semantic_scholar(
    client: httpx.Client, records: dict[str, SourceRecord], converted: set[str]
) -> tuple[dict[str, SourceRecord], set[str]]:
    """Point unconverted Semantic Scholar landing pages at convertible full text.

    Args:
        client: HTTP client for the Semantic Scholar Graph API.
        records: Legacy records keyed by legacy markdown stem.
        converted: Legacy stems that already have markdown.

    Returns:
        The records with resolved inputs, and the input URLs of records that
        have no open-access full text.
    """
    stems = sorted(
        stem
        for stem, record in records.items()
        if record.source == "semantic_scholar"
        and record.input_format == "html"
        and stem not in converted
    )
    resolved = dict(records)
    no_full_text: set[str] = set()
    outcomes: collections.Counter[str] = collections.Counter()
    for start in range(0, len(stems), S2_BATCH_SIZE):
        batch = stems[start : start + S2_BATCH_SIZE]
        response = client.post(
            S2_BATCH_URL,
            params={"fields": "openAccessPdf,externalIds"},
            json={"ids": [records[stem].source_id for stem in batch]},
        )
        if response.status_code == 429:
            raise RuntimeError(
                "Semantic Scholar rate limit (HTTP 429): set"
                " SEMANTIC_SCHOLAR_API_KEY or retry later"
            )
        response.raise_for_status()
        for stem, item in zip(batch, response.json(), strict=True):
            record = records[stem]
            if item is None:
                raise ValueError(f"Semantic Scholar has no paper {record.source_id}")
            arxiv_id = item["externalIds"].get("ArXiv")
            pdf_url = item["openAccessPdf"]["url"]
            if arxiv_id:
                outcomes["arXiv"] += 1
                resolved[stem] = record.model_copy(
                    update={
                        "arxiv_id": arxiv_id,
                        "input_format": "pdf",
                        "input_url": arxiv_pdf_url(arxiv_id),
                    }
                )
            elif pdf_url:
                outcomes["open-access PDF"] += 1
                resolved[stem] = record.model_copy(
                    update={"input_format": "pdf", "input_url": pdf_url}
                )
            else:
                outcomes["no full text"] += 1
                no_full_text.add(record.input_url)
    logging.info(
        "Resolved %d unconverted Semantic Scholar rows: %s", len(stems), dict(outcomes)
    )
    return resolved, no_full_text


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
