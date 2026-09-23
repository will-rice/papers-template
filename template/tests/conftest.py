from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Callable
from pathlib import Path
from typing import Literal, TypedDict

import httpx
import pytest

from papers_pipeline.config import TopicConfig
from papers_pipeline.models import Paper, SourceRecord

FIXTURES = Path(__file__).with_name("fixtures")


class RecordedRoute(TypedDict, total=False):
    fixture: str
    status_code: int
    headers: dict[str, str]
    mode: Literal["text", "bytes"]
    encoding: str


@pytest.fixture
def source_record() -> SourceRecord:
    return SourceRecord(
        source="semantic_scholar",
        source_id="semantic-1",
        title=" Neural   Speech\nRecognition ",
        abstract=" A system for speech understanding. ",
        authors=(" Ada  Lovelace ", " Grace\nHopper "),
        published=datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc),
        url="https://example.test/papers/semantic-1",
        input_format="pdf",
        input_url="https://example.test/papers/semantic-1.pdf",
        categories=("cs.CL", "cs.AI", "cs.CL"),
        doi=None,
        arxiv_id=None,
    )


@pytest.fixture
def paper() -> Paper:
    return Paper(
        identifier="semantic_scholar:semantic-1",
        title="Neural Speech Recognition",
        abstract="A system for speech understanding.",
        authors=("Ada Lovelace", "Grace Hopper"),
        published=datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc),
        url="https://example.test/papers/semantic-1",
        source="semantic_scholar",
        input_format="pdf",
        input_url="https://example.test/papers/semantic-1.pdf",
        categories=("cs.AI", "cs.CL"),
        doi=None,
        arxiv_id=None,
    )


@pytest.fixture
def topic_config() -> TopicConfig:
    return TopicConfig(
        include_any=["speech"],
        include_all=[],
        exclude_any=[],
        categories=[],
        plugin=None,
    )


@pytest.fixture
def fixture_transport() -> Callable[[dict[str, RecordedRoute]], httpx.MockTransport]:
    def build(routes: dict[str, RecordedRoute]) -> httpx.MockTransport:
        recorded_routes = {
            url: _load_recorded_route(spec) for url, spec in routes.items()
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if url not in recorded_routes:
                raise AssertionError(f"unexpected recorded fixture URL: {request.method} {url}")
            status_code, headers, body, mode = recorded_routes[url]
            if mode == "text":
                return httpx.Response(status_code, headers=headers, text=body)
            return httpx.Response(status_code, headers=headers, content=body)

        return httpx.MockTransport(handler)

    return build


def _load_recorded_route(
    spec: RecordedRoute,
) -> tuple[int, dict[str, str], str | bytes, Literal["text", "bytes"]]:
    fixture_path = (FIXTURES / spec["fixture"]).resolve()
    fixtures_root = FIXTURES.resolve()
    try:
        fixture_path.relative_to(fixtures_root)
    except ValueError as error:
        raise ValueError(f"fixture path escapes test fixtures: {spec['fixture']}") from error
    if not fixture_path.is_file():
        raise FileNotFoundError(f"missing recorded fixture: {spec['fixture']}")

    status_code = spec.get("status_code", 200)
    headers = dict(spec.get("headers", {}))
    mode = spec.get("mode", "text")
    if mode == "text":
        return (
            status_code,
            headers,
            fixture_path.read_text(encoding=spec.get("encoding", "utf-8")),
            mode,
        )
    return status_code, headers, fixture_path.read_bytes(), mode
