from collections import defaultdict
from collections.abc import Iterable
import re
import unicodedata

from papers_pipeline.models import Paper, SourceRecord

SOURCE_RANK: dict[str, int] = {
    "arxiv": 0,
    "crossref": 1,
    "semantic_scholar": 2,
    "dblp": 3,
    "huggingface": 4,
    "papers_with_code": 5,
    "biorxiv": 6,
    "biorxiv_crossref": 6,
}
_DEFAULT_SOURCE_RANK = max(SOURCE_RANK.values(), default=0) + 1
# An old-style arXiv ID stored without its archive. Modern IDs contain a dot,
# so this never matches one.
STRIPPED_ARXIV_ID = re.compile(r"\d{7}(?:v\d+)?")
_OLD_STYLE_ARXIV_ID = re.compile(r"[a-z-]+(?:\.[a-z-]+)?/(?P<number>\d{7})(?:v\d+)?")


def clean(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()


def normalize(record: SourceRecord) -> Paper:
    doi = _canonical_doi(record.doi)
    arxiv_id = _canonical_arxiv_id(record.arxiv_id)
    identifier = _identifier(
        source=record.source,
        source_id=record.source_id,
        doi=doi,
        arxiv_id=arxiv_id,
    )
    return Paper(
        identifier=identifier,
        title=clean(record.title),
        abstract=clean(record.abstract),
        authors=tuple(clean(author) for author in record.authors),
        published=record.published,
        url=record.url,
        source=record.source,
        input_format=record.input_format,
        input_url=record.input_url,
        categories=tuple(
            sorted({category for category in map(clean, record.categories) if category})
        ),
        doi=doi,
        arxiv_id=arxiv_id,
    )


def deduplicate(
    papers: Iterable[Paper], *, inventory: Iterable[Paper] = ()
) -> list[Paper]:
    """Fold reports into rows, one row per identifier.

    A row's identity and content belong to it: file names and failure history
    are keyed by its identifier, so rows are never renamed or merged. A report
    of the same paper may only add what ``_enriched`` allows. A report whose
    arXiv ID differs from its match's is another paper and gets its own row.
    Reports run best-ranked first, so a new paper's row is its best report.

    Args:
        papers: Freshly fetched papers.
        inventory: Papers already stored in the inventory.

    Returns:
        The inventory rows and new papers, in output order.
    """
    rows = sorted(inventory, key=_preference_key)
    by_identifier: dict[str, int] = {}
    owners: dict[str, int] = {}
    bibliographic: defaultdict[str, set[int]] = defaultdict(set)
    stripped_rows: defaultdict[str, list[int]] = defaultdict(list)

    def remember(row_id: int, paper: Paper) -> None:
        by_identifier.setdefault(paper.identifier, row_id)
        for key in _identity_keys(paper):
            owners.setdefault(key, row_id)
        bibliographic[_bibliographic_key(paper)].add(row_id)

    for row_id, row in enumerate(rows):
        remember(row_id, row)
        if row.arxiv_id and STRIPPED_ARXIV_ID.fullmatch(row.arxiv_id):
            stripped_rows[row.arxiv_id[:7]].append(row_id)

    for report in sorted(papers, key=_preference_key):
        row_id = by_identifier.get(report.identifier)
        if row_id is None and report.arxiv_id:
            row_id = owners.get(_arxiv_key(report.arxiv_id))
        repaired = False
        if row_id is None:
            row_id = _claim_stripped_row(report, rows, stripped_rows)
            repaired = row_id is not None
        if row_id is None:
            matches = {
                match
                for match in bibliographic.get(_bibliographic_key(report), ())
                if _can_merge_bibliographically(
                    _strong_keys(report), _strong_keys(rows[match])
                )
            }
            if report.doi and f"doi:{report.doi}" in owners:
                matches.add(owners[f"doi:{report.doi}"])
            if report.arxiv_id:
                # This arXiv ID has no row, so a match holding any arXiv ID is
                # a different paper that shares a DOI or a title.
                matches = {
                    match
                    for match in matches
                    if not any(
                        key.startswith("arxiv:") for key in _identity_keys(rows[match])
                    )
                }
            row_id = min(matches, default=None)
        if row_id is None:
            rows.append(report)
            remember(len(rows) - 1, report)
            continue
        rows[row_id] = _enriched(rows[row_id], report, row_id, owners, repaired)
        remember(row_id, rows[row_id])
        remember(row_id, report)

    return sorted(rows, key=_output_key)


def _enriched(
    row: Paper, report: Paper, row_id: int, owners: dict[str, int], repaired: bool
) -> Paper:
    """Return the row with the only updates a report may make.

    The report adds categories, an empty abstract or author list, and a missing
    DOI or arXiv ID that no other row owns. An arXiv report that supplies the
    row's arXiv ID also supplies its location, because the row's old input may
    be a publisher link. A repaired old-style ID replaces the stored one and its
    broken location.

    Args:
        row: The stored row.
        report: A report of the same paper.
        row_id: The row's position.
        owners: Row position by identity key.
        repaired: Whether the report repairs the row's stripped arXiv ID.

    Returns:
        The updated row.
    """
    update: dict[str, object] = {}
    if not {*report.categories} <= {*row.categories}:
        update["categories"] = tuple(sorted({*row.categories, *report.categories}))
    if not row.abstract and report.abstract:
        update["abstract"] = report.abstract
    if not row.authors and report.authors:
        update["authors"] = report.authors
    if not row.doi and report.doi and owners.get(f"doi:{report.doi}", row_id) == row_id:
        update["doi"] = report.doi
    learns_arxiv_id = (
        not row.arxiv_id
        and report.arxiv_id is not None
        and owners.get(_arxiv_key(report.arxiv_id), row_id) == row_id
    )
    if repaired or learns_arxiv_id:
        update["arxiv_id"] = report.arxiv_id
    if repaired or (learns_arxiv_id and report.source == "arxiv"):
        update["url"] = report.url
        update["input_format"] = report.input_format
        update["input_url"] = report.input_url
    return row.model_copy(update=update)


def _claim_stripped_row(
    report: Paper, rows: list[Paper], stripped_rows: dict[str, list[int]]
) -> int | None:
    """Return the row holding this report's old-style ID without its archive.

    Archives reuse numbers (cs/9812006, hep-th/9812006), so the number alone
    is not enough: the row must also have the report's exact submission time,
    or its title and date. A claimed row leaves ``stripped_rows``.

    Args:
        report: The report.
        rows: The rows.
        stripped_rows: Row positions by seven-digit number.

    Returns:
        The row position, or None when no stripped row is this paper.
    """
    arxiv_id = report.arxiv_id
    old_style = _OLD_STYLE_ARXIV_ID.fullmatch(arxiv_id) if arxiv_id else None
    if old_style is None:
        return None
    candidates = stripped_rows.get(old_style["number"], [])
    for row_id in candidates:
        stored = rows[row_id]
        if stored.published == report.published or _bibliographic_key(
            stored
        ) == _bibliographic_key(report):
            candidates.remove(row_id)
            return row_id
    return None


def _identifier(
    *, source: str, source_id: str, doi: str | None, arxiv_id: str | None
) -> str:
    if arxiv_id:
        return f"arxiv:{arxiv_id.casefold()}"
    if doi:
        return f"doi:{doi.casefold()}"
    return f"{source}:{clean(source_id)}"


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = clean(value)
    return normalized or None


def _canonical_doi(value: str | None) -> str | None:
    cleaned = _clean_optional(value)
    if cleaned is None:
        return None
    return cleaned.casefold()


def _canonical_arxiv_id(value: str | None) -> str | None:
    cleaned = _clean_optional(value)
    if cleaned is None:
        return None
    return re.sub(r"(?i)^arxiv:\s*", "", cleaned).casefold()


def _strong_keys(paper: Paper) -> set[str]:
    keys: set[str] = set()
    arxiv_id = _canonical_arxiv_id(paper.arxiv_id)
    doi = _canonical_doi(paper.doi)
    if arxiv_id:
        # Versions of one arXiv paper are the same paper; deduplicate keeps the
        # inventory's identifier for it, so file names are stable.
        keys.add(_arxiv_key(arxiv_id))
    if doi:
        keys.add(f"doi:{doi}")
    return keys


def _arxiv_key(arxiv_id: str) -> str:
    return f"arxiv:{re.sub(r'v\d+$', '', arxiv_id)}"


def _identity_keys(paper: Paper) -> set[str]:
    """Return the strong keys plus the key of the paper's own identifier.

    A stored identifier can name an ID the row no longer carries, and a paper
    with no strong ID is known only by its identifier.
    """
    identifier = paper.identifier
    if identifier.startswith("arxiv:"):
        identifier = _arxiv_key(identifier.removeprefix("arxiv:"))
    return _strong_keys(paper) | {identifier}


def _bibliographic_key(paper: Paper) -> str:
    return (
        f"bibliographic:{clean(paper.title).casefold()}|"
        f"{paper.published.date().isoformat()}"
    )


def _can_merge_bibliographically(
    paper_strong_keys: set[str], cluster_strong_keys: set[str]
) -> bool:
    return (
        not paper_strong_keys
        or not cluster_strong_keys
        or bool(paper_strong_keys & cluster_strong_keys)
    )


def _preference_key(paper: Paper) -> tuple[int, int, str, str, str]:
    return (
        _strong_priority(paper),
        _source_rank(paper.source),
        paper.identifier.casefold(),
        paper.url,
        _bibliographic_key(paper),
    )


def _output_key(paper: Paper) -> tuple[str, str]:
    return (paper.identifier.casefold(), paper.source)


def _strong_priority(paper: Paper) -> int:
    if paper.arxiv_id:
        return 0
    if paper.doi:
        return 1
    return 2


def _source_rank(source: str) -> int:
    return SOURCE_RANK.get(source, _DEFAULT_SOURCE_RANK)
