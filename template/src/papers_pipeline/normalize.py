from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, replace
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


@dataclass
class _Cluster:
    paper: Paper
    strong_keys: set[str]
    identity_keys: set[str]
    bibliographic_keys: set[str]


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
    """Merge reports of one paper, keeping every inventory row's identifier.

    File names and failure history are keyed by identifier, so a row in the
    inventory is never renamed and never merged into another row. Reports that
    match a row enrich it, filling only strong IDs the row lacks and no other
    row holds; the rest become new papers.

    Args:
        papers: Freshly fetched papers.
        inventory: Papers already stored in the inventory.

    Returns:
        The inventory rows and new papers, in output order.
    """
    rows = [
        _Cluster(
            paper=paper,
            strong_keys=_strong_keys(paper),
            identity_keys=_identity_keys(paper),
            bibliographic_keys={_bibliographic_key(paper)},
        )
        for paper in sorted(inventory, key=_preference_key)
    ]
    strong_index: dict[str, int] = {}
    bibliographic_index: defaultdict[str, list[int]] = defaultdict(list)
    for row_id, row in enumerate(rows):
        for identity_key in row.identity_keys:
            strong_index.setdefault(identity_key, row_id)
        for bibliographic_key in row.bibliographic_keys:
            bibliographic_index[bibliographic_key].append(row_id)

    new_papers: list[Paper] = []
    for report in sorted(
        _cluster_reports(papers), key=lambda cluster: _preference_key(cluster.paper)
    ):
        matches = {
            strong_index[key] for key in report.identity_keys if key in strong_index
        } | {
            row_id
            for key in report.bibliographic_keys
            for row_id in bibliographic_index.get(key, ())
            if _can_merge_bibliographically(
                report.strong_keys, rows[row_id].strong_keys
            )
        }
        if not matches:
            new_papers.append(report.paper)
            continue
        row_id = min(matches)
        row = rows[row_id]
        doi = row.paper.doi
        if not doi and strong_index.get(f"doi:{report.paper.doi}", row_id) == row_id:
            doi = report.paper.doi
        arxiv_id = row.paper.arxiv_id
        if (
            not arxiv_id
            and report.paper.arxiv_id
            and strong_index.get(_arxiv_key(report.paper.arxiv_id), row_id) == row_id
        ):
            arxiv_id = report.paper.arxiv_id
        enriched = _merge_clusters(row, report).paper.model_copy(
            update={
                "identifier": row.paper.identifier,
                "doi": doi,
                "arxiv_id": arxiv_id,
            }
        )
        strong_keys = _strong_keys(enriched)
        rows[row_id] = replace(
            row,
            paper=enriched,
            strong_keys=strong_keys,
            identity_keys=row.identity_keys | strong_keys,
        )
        for strong_key in strong_keys:
            strong_index.setdefault(strong_key, row_id)

    return sorted([*(row.paper for row in rows), *new_papers], key=_output_key)


def _cluster_reports(papers: Iterable[Paper]) -> list[_Cluster]:
    clusters: dict[int, _Cluster] = {}
    strong_index: dict[str, int] = {}
    bibliographic_index: dict[str, int] = {}
    next_cluster_id = 0

    for paper in sorted(papers, key=_processing_key):
        paper_strong_keys = _strong_keys(paper)
        paper_identity_keys = _identity_keys(paper)
        paper_bibliographic_key = _bibliographic_key(paper)
        candidate_ids = {
            strong_index[key] for key in paper_identity_keys if key in strong_index
        }
        bibliographic_candidate = bibliographic_index.get(paper_bibliographic_key)
        if bibliographic_candidate is not None:
            cluster = clusters[bibliographic_candidate]
            if _can_merge_bibliographically(paper_strong_keys, cluster.strong_keys):
                candidate_ids.add(bibliographic_candidate)

        if candidate_ids:
            cluster_id = min(candidate_ids)
            merged_cluster = _Cluster(
                paper=paper,
                strong_keys=set(),
                identity_keys=set(),
                bibliographic_keys=set(),
            )
            for candidate_id in sorted(candidate_ids):
                candidate_cluster = clusters.pop(candidate_id)
                merged_cluster = _merge_clusters(merged_cluster, candidate_cluster)
            merged_cluster = _merge_clusters(
                merged_cluster,
                _Cluster(
                    paper=paper,
                    strong_keys=paper_strong_keys,
                    identity_keys=paper_identity_keys,
                    bibliographic_keys={paper_bibliographic_key},
                ),
            )
            clusters[cluster_id] = merged_cluster
        else:
            cluster_id = next_cluster_id
            next_cluster_id += 1
            clusters[cluster_id] = _Cluster(
                paper=paper,
                strong_keys=paper_strong_keys,
                identity_keys=paper_identity_keys,
                bibliographic_keys={paper_bibliographic_key},
            )

        _reindex(strong_index, bibliographic_index, clusters)

    return list(clusters.values())


def _merge_clusters(left: _Cluster, right: _Cluster) -> _Cluster:
    preferred = min((left.paper, right.paper), key=_preference_key)
    other = right.paper if preferred == left.paper else left.paper
    categories = tuple(sorted({*left.paper.categories, *right.paper.categories}))
    doi = _merge_identifier_value(preferred.doi, other.doi)
    arxiv_id = _merge_identifier_value(preferred.arxiv_id, other.arxiv_id)
    return _Cluster(
        paper=preferred.model_copy(
            update={
                "identifier": _merged_identifier(preferred, doi=doi, arxiv_id=arxiv_id),
                "categories": categories,
                "doi": doi,
                "arxiv_id": arxiv_id,
            }
        ),
        strong_keys=left.strong_keys | right.strong_keys,
        identity_keys=left.identity_keys | right.identity_keys,
        bibliographic_keys=left.bibliographic_keys | right.bibliographic_keys,
    )


def _reindex(
    strong_index: dict[str, int],
    bibliographic_index: dict[str, int],
    clusters: dict[int, _Cluster],
) -> None:
    strong_index.clear()
    bibliographic_index.clear()
    for cluster_id, cluster in clusters.items():
        for identity_key in cluster.identity_keys:
            strong_index[identity_key] = cluster_id
        for bibliographic_key in cluster.bibliographic_keys:
            bibliographic_index[bibliographic_key] = cluster_id


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


def _merge_identifier_value(preferred: str | None, other: str | None) -> str | None:
    return preferred or other


def _merged_identifier(
    preferred: Paper, *, doi: str | None, arxiv_id: str | None
) -> str:
    if arxiv_id:
        return f"arxiv:{arxiv_id}"
    if doi:
        return f"doi:{doi}"
    return preferred.identifier


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


def _processing_key(paper: Paper) -> tuple[int, int, str, str]:
    return (
        _strong_priority(paper),
        _source_rank(paper.source),
        paper.identifier.casefold(),
        _bibliographic_key(paper),
    )


def _preference_key(paper: Paper) -> tuple[int, int, str, str]:
    return (
        _strong_priority(paper),
        _source_rank(paper.source),
        paper.identifier.casefold(),
        paper.url,
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
