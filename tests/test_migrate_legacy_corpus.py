import subprocess
from pathlib import Path

import pytest

from migrate_legacy_corpus import main
from papers_pipeline.batching import expected_markdown, infer_backlog
from papers_pipeline.inventory import read_inventory
from papers_pipeline.state import load_state

LEGACY_CSV = """\
arxiv_id,title,authors,submitted,categories,url,abstract
2401.00001,Converted Paper,"A. Author, B. Author",2024-01-02,cs.SD eess.AS,https://arxiv.org/abs/2401.00001,First.
2401.00002,Pending Paper,C. Author,2024-01-03,cs.SD,https://arxiv.org/abs/2401.00002,Second.
s2:abc123,Scholar Paper,D. Author,2023-05-06,,https://www.semanticscholar.org/paper/abc123,Third.
dblp:journals/corr/abs-2401-00001,Converted Paper,"A. Author, B. Author",2024-01-02,,https://doi.org/10.48550/arXiv.2401.00001,First.
biorxiv:2025.01.01.000001,Bio Paper,E. Author,2025-01-01,,https://doi.org/10.1101/2025.01.01.000001,Fourth.
"""


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def legacy_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
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
        "Cites [scholar](../2023/s2:abc123.md) and [pending](2401.00002.md).\n",
        encoding="utf-8",
    )
    (papers / "2024" / "dblp:journals_corr_abs-2401-00001.md").write_text(
        "Duplicate conversion.\n", encoding="utf-8"
    )
    (papers / "2023" / "s2:abc123.md").write_text("Scholar.\n", encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "legacy corpus")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_migration_produces_generated_template_corpus(legacy_repo: Path) -> None:
    main()

    papers = read_inventory(legacy_repo / "papers.csv")
    by_identifier = {paper.identifier: paper for paper in papers}
    # The arXiv DOI row merges into its arXiv paper.
    assert sorted(by_identifier) == [
        "arxiv:2401.00001",
        "arxiv:2401.00002",
        "doi:10.1101/2025.01.01.000001",
        "semantic_scholar:abc123",
    ]
    converted = by_identifier["arxiv:2401.00001"]
    assert converted.authors == ("A. Author", "B. Author")
    assert converted.categories == ("cs.SD", "eess.AS")
    assert converted.input_url == "https://arxiv.org/pdf/2401.00001"

    backlog = infer_backlog(papers, legacy_repo)
    assert {paper.identifier for paper in backlog.generated} == {
        "arxiv:2401.00001",
        "semantic_scholar:abc123",
    }
    assert sorted(path.name for path in (legacy_repo / "papers").iterdir()) == sorted(
        expected_markdown(legacy_repo, paper).name for paper in backlog.generated
    )

    text = expected_markdown(legacy_repo, converted).read_text(encoding="utf-8")
    scholar = expected_markdown(legacy_repo, by_identifier["semantic_scholar:abc123"])
    pending = expected_markdown(legacy_repo, by_identifier["arxiv:2401.00002"])
    assert text == (f"Cites [scholar]({scholar.name}) and [pending]({pending.name}).\n")
    assert load_state(legacy_repo / ".papers-state.yml").continuations == {}
    # Moves are staged as renames so history follows each paper.
    assert "R100\tpapers/2023/s2:abc123.md" in git(
        legacy_repo, "diff", "--cached", "-M", "--name-status"
    )


def test_migration_rejects_unknown_legacy_prefix(legacy_repo: Path) -> None:
    (legacy_repo / "papers.csv").write_text(
        LEGACY_CSV
        + "pwc:x,Unknown,F. Author,2024-01-01,,https://example.test,Fifth.\n",
        encoding="utf-8",
    )
    git(legacy_repo, "commit", "-qam", "unknown source")

    with pytest.raises(ValueError, match="unknown legacy ID prefix: pwc:x"):
        main()
