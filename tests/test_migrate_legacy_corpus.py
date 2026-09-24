import json
import subprocess
from pathlib import Path

import httpx
import pytest

from migrate_legacy_corpus import NO_FULL_TEXT, S2_BATCH_URL, migrate
from papers_pipeline.batching import expected_markdown, infer_backlog
from papers_pipeline.front_matter import with_front_matter
from papers_pipeline.inventory import read_inventory
from papers_pipeline.state import load_state

LEGACY_CSV = """\
arxiv_id,title,authors,submitted,categories,url,abstract
2401.00001,Converted Paper,"A. Author, B. Author",2024-01-02,cs.SD eess.AS,https://arxiv.org/abs/2401.00001,First.
2401.00002,Pending Paper,C. Author,2024-01-03,cs.SD,https://arxiv.org/abs/2401.00002,Second.
s2:abc123,Scholar Paper,D. Author,2023-05-06,,https://www.semanticscholar.org/paper/abc123,Third.
dblp:journals/corr/abs-2401-00001,Converted Paper,"A. Author, B. Author",2024-01-02,,https://doi.org/10.48550/arXiv.2401.00001,First.
biorxiv:2025.01.01.000001,Bio Paper,E. Author,2025-01-01,,https://doi.org/10.1101/2025.01.01.000001,Fourth.
ss:onarxiv,Pending Paper (Journal Version),C. Author,2024-06-01,,https://doi.org/10.1000/onarxiv,Second.
ss:openaccess,Open Paper,F. Author,2024-02-03,,https://doi.org/10.1000/openaccess,Sixth.
ss:closed,Closed Paper,G. Author,2024-03-04,,https://doi.org/10.1000/closed,Seventh.
ss:direct,Direct Paper,H. Author,2024-04-05,,https://example.test/direct.PDF?download=1,Eighth.
dblp:conf/clef/Author24,Workshop Paper,I. Author,2024-09-01,,https://ceur-ws.org/Vol-1/paper_1.pdf,Ninth.
dblp:conf/x/Author24,Landing Paper,J. Author,2024-10-01,,https://example.test/landing,Tenth.
"""
# Graph API batch responses for the unconverted Semantic Scholar rows.
S2_PAPERS = {
    "onarxiv": {
        "paperId": "onarxiv",
        "externalIds": {"ArXiv": "2401.00002", "DOI": "10.1000/onarxiv"},
        "openAccessPdf": {"url": "https://example.test/onarxiv.pdf"},
    },
    "openaccess": {
        "paperId": "openaccess",
        "externalIds": {"DOI": "10.1000/openaccess"},
        "openAccessPdf": {"url": "https://example.test/openaccess.pdf"},
    },
    "closed": {
        "paperId": "closed",
        "externalIds": {"DOI": "10.1000/closed"},
        "openAccessPdf": {"url": "", "status": "CLOSED", "license": None},
    },
}


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


def semantic_scholar(request: httpx.Request) -> httpx.Response:
    assert str(request.url).split("?")[0] == S2_BATCH_URL
    assert request.url.params["fields"] == "openAccessPdf,externalIds"
    ids = json.loads(request.content)["ids"]
    return httpx.Response(200, json=[S2_PAPERS[paper_id] for paper_id in ids])


@pytest.fixture
def client() -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(semantic_scholar))


@pytest.fixture
def legacy_repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "user.email", "test@example.test")
    (tmp_path / "papers.csv").write_text(LEGACY_CSV, encoding="utf-8")
    papers = tmp_path / "papers"
    (papers / "2024").mkdir(parents=True)
    (papers / "2023").mkdir()
    (papers / "README.md").write_text("# Index\n", encoding="utf-8")
    (papers / "2024" / "README.md").write_text("# 2024\n", encoding="utf-8")
    (papers / "2024" / "2401.00001.md").write_text(
        "Cites [scholar](../2023/s2:abc123.md), [pending](2401.00002.md)"
        " and [same year](./2401.00002.md).\n",
        encoding="utf-8",
    )
    (papers / "2024" / "dblp:journals_corr_abs-2401-00001.md").write_text(
        "Duplicate conversion.\n", encoding="utf-8"
    )
    (papers / "2023" / "s2:abc123.md").write_text("Scholar.\n", encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "legacy corpus")
    return tmp_path


def test_migration_produces_generated_template_corpus(
    legacy_repo: Path, client: httpx.Client
) -> None:
    migrate(legacy_repo, client)

    papers = read_inventory(legacy_repo / "papers.csv")
    by_identifier = {paper.identifier: paper for paper in papers}
    # The arXiv DOI row merges into its arXiv paper, and so does the Semantic
    # Scholar row whose arXiv ID only the Graph API knows.
    assert sorted(by_identifier) == [
        "arxiv:2401.00001",
        "arxiv:2401.00002",
        "dblp:conf/clef/Author24",
        "dblp:conf/x/Author24",
        "doi:10.1000/closed",
        "doi:10.1000/openaccess",
        "doi:10.1101/2025.01.01.000001",
        "semantic_scholar:abc123",
        "semantic_scholar:direct",
    ]
    converted = by_identifier["arxiv:2401.00001"]
    assert converted.authors == ("A. Author", "B. Author")
    assert converted.categories == ("cs.SD", "eess.AS")
    assert converted.input_url == "https://arxiv.org/pdf/2401.00001"

    # Every pending paper converts from full text, never a landing page.
    assert {
        identifier: (paper.input_format, paper.input_url)
        for identifier, paper in by_identifier.items()
        if identifier != "semantic_scholar:abc123"
    } == {
        "arxiv:2401.00001": ("pdf", "https://arxiv.org/pdf/2401.00001"),
        "arxiv:2401.00002": ("pdf", "https://arxiv.org/pdf/2401.00002"),
        # Legacy URLs that are already PDFs convert as PDFs, without a lookup.
        "dblp:conf/clef/Author24": ("pdf", "https://ceur-ws.org/Vol-1/paper_1.pdf"),
        "dblp:conf/x/Author24": ("html", "https://example.test/landing"),
        "doi:10.1000/closed": ("html", "https://doi.org/10.1000/closed"),
        "doi:10.1000/openaccess": ("pdf", "https://example.test/openaccess.pdf"),
        "doi:10.1101/2025.01.01.000001": (
            "pdf",
            "https://www.biorxiv.org/content/10.1101/2025.01.01.000001.full.pdf",
        ),
        "semantic_scholar:direct": (
            "pdf",
            "https://example.test/direct.PDF?download=1",
        ),
    }

    backlog = infer_backlog(papers, legacy_repo)
    assert {paper.identifier for paper in backlog.generated} == {
        "arxiv:2401.00001",
        "semantic_scholar:abc123",
    }
    # The paper without open-access full text is blocked, not left pending.
    assert [paper.identifier for paper in backlog.blocked] == ["doi:10.1000/closed"]
    assert {paper.identifier for paper in backlog.pending} == {
        "arxiv:2401.00002",
        "dblp:conf/clef/Author24",
        "dblp:conf/x/Author24",
        "doi:10.1000/openaccess",
        "doi:10.1101/2025.01.01.000001",
        "semantic_scholar:direct",
    }
    fixme = expected_markdown(legacy_repo, backlog.blocked[0]).with_suffix(".fixme.txt")
    assert fixme.read_text(encoding="utf-8").splitlines()[:2] == [
        "paper: doi:10.1000/closed",
        f"latest_error: {NO_FULL_TEXT}",
    ]
    assert sorted(path.name for path in (legacy_repo / "papers").iterdir()) == sorted(
        [
            *(
                expected_markdown(legacy_repo, paper).name
                for paper in backlog.generated
            ),
            fixme.name,
        ]
    )

    text = expected_markdown(legacy_repo, converted).read_text(encoding="utf-8")
    scholar = expected_markdown(legacy_repo, by_identifier["semantic_scholar:abc123"])
    pending = expected_markdown(legacy_repo, by_identifier["arxiv:2401.00002"])
    assert text == with_front_matter(
        converted,
        f"Cites [scholar]({scholar.name}), [pending]({pending.name})"
        f" and [same year]({pending.name}).\n",
    )
    assert load_state(legacy_repo / ".papers-state.yml").continuations == {}
    # Moves are staged as renames so history follows each paper.
    assert "R100\tpapers/2023/s2:abc123.md" in git(
        legacy_repo, "diff", "--cached", "-M", "--name-status"
    )


def test_migration_raises_on_semantic_scholar_rate_limit(legacy_repo: Path) -> None:
    client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(429))
    )

    with pytest.raises(RuntimeError, match="Semantic Scholar rate limit"):
        migrate(legacy_repo, client)


def test_migration_rejects_unknown_legacy_prefix(
    legacy_repo: Path, client: httpx.Client
) -> None:
    (legacy_repo / "papers.csv").write_text(
        LEGACY_CSV
        + "pwc:x,Unknown,F. Author,2024-01-01,,https://example.test,Fifth.\n",
        encoding="utf-8",
    )
    git(legacy_repo, "commit", "-qam", "unknown source")

    with pytest.raises(ValueError, match="unknown legacy ID prefix: pwc:x"):
        migrate(legacy_repo, client)
