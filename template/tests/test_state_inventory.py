from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import yaml

from papers_pipeline.inventory import read_inventory, write_inventory
from papers_pipeline.models import FailureAttempt, Paper, PipelineState
from papers_pipeline.state import load_state, save_state


def paper(identifier: str = "arxiv:2401.00001") -> Paper:
    return Paper(
        identifier=identifier,
        title="A Paper",
        abstract="An abstract",
        authors=("A. Author",),
        published=datetime(2024, 1, 2, tzinfo=timezone.utc),
        url="https://example.test/paper",
        source="arxiv",
        input_format="pdf",
        input_url="https://example.test/paper.pdf",
        categories=("cs.CL",),
    )


def test_inventory_round_trip_is_sorted(tmp_path: Path) -> None:
    path = tmp_path / "papers.csv"
    write_inventory(path, [paper("ss:2"), paper("arxiv:1")])

    assert [item.identifier for item in read_inventory(path)] == ["arxiv:1", "ss:2"]


def test_inventory_handles_empty_csv(tmp_path: Path) -> None:
    path = tmp_path / "papers.csv"
    path.write_text(
        "identifier,title,abstract,authors,published,url,source,input_format,input_url,categories,doi,arxiv_id\n"
    )

    assert read_inventory(path) == []


def test_state_round_trip_is_atomic_and_only_stores_state_fields(
    tmp_path: Path,
) -> None:
    path = tmp_path / ".papers-state.yml"
    state = PipelineState(
        cursors={"arxiv": "cursor-2"},
        failures={
            "arxiv:1": [
                FailureAttempt(
                    occurred_at=datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc),
                    error="converter exited 1",
                )
            ]
        },
    )

    original_replace = os.replace
    observed_temp_paths: list[Path] = []

    def spy_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        temp_path = Path(src)
        observed_temp_paths.append(temp_path)
        assert temp_path.exists()
        assert yaml.safe_load(temp_path.read_text()) == state.model_dump(mode="json")
        original_replace(src, dst)

    try:
        os.replace = spy_replace  # type: ignore[assignment]
        save_state(path, state)
    finally:
        os.replace = original_replace  # type: ignore[assignment]

    assert observed_temp_paths == [path.with_suffix(".yml.tmp")]
    assert load_state(path) == state
    assert yaml.safe_load(path.read_text()) == state.model_dump(mode="json")
