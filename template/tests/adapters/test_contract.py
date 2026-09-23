from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from papers_pipeline.adapters.base import (
    FetchPage,
    FetchWindow,
    collect_records,
)
from papers_pipeline.errors import InfrastructureError, PaperError
from papers_pipeline.models import SourceRecord


@dataclass(frozen=True)
class _ParserItem:
    value: str


def test_collect_records_keeps_paper_errors_and_records() -> None:
    source_record = SourceRecord(
        source="arxiv",
        source_id="2401.00001",
        title="Title",
        abstract="Abstract",
        authors=("Author",),
        published=datetime(2024, 1, 2, tzinfo=timezone.utc),
        url="https://example.test/paper",
        input_format="pdf",
        input_url="https://example.test/paper.pdf",
    )

    def parser(item: _ParserItem) -> SourceRecord:
        if item.value == "bad":
            raise PaperError("bad paper")
        if item.value == "boom":
            raise InfrastructureError("boom")
        return source_record

    records, errors = collect_records(
        [_ParserItem("ok"), _ParserItem("bad")],
        parser,
    )

    assert records == (source_record,)
    assert errors == ("bad paper",)


def test_collect_records_propagates_infrastructure_errors() -> None:
    def parser(_: _ParserItem) -> SourceRecord:
        raise InfrastructureError("boom")

    with pytest.raises(InfrastructureError, match="boom"):
        collect_records([_ParserItem("boom")], parser)


def test_fetch_page_is_immutable() -> None:
    page = FetchPage(
        records=(),
        next_cursor=None,
        capped=False,
        permanent_errors=(),
    )

    with pytest.raises(Exception):
        page.records = ()  # type: ignore[misc]


def test_fetch_window_is_immutable() -> None:
    window = FetchWindow(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )

    with pytest.raises(Exception):
        window.start = datetime(2024, 1, 2, tzinfo=timezone.utc)  # type: ignore[misc]


def test_fetch_window_rejects_naive_datetimes() -> None:
    with pytest.raises(
        ValidationError, match="FetchWindow.start and FetchWindow.end must be timezone-aware"
    ):
        FetchWindow(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        )


def test_fetch_window_rejects_reversed_interval() -> None:
    with pytest.raises(
        ValidationError, match="FetchWindow.start must be before or equal to FetchWindow.end"
    ):
        FetchWindow(
            start=datetime(2024, 1, 8, tzinfo=timezone.utc),
            end=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
