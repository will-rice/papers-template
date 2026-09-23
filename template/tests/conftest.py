from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Callable
from pathlib import Path
from typing import Literal, TypedDict, cast

import httpx
import pytest

from papers_pipeline.config import TopicConfig
from papers_pipeline.config import AdapterConfig, FetchConfig
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.models import Paper, SourceRecord

FIXTURES = Path(__file__).with_name("fixtures")


class RecordedRoute(TypedDict, total=False):
    fixture: str
    status_code: int
    headers: dict[str, str]
    mode: Literal["text", "bytes"]
    encoding: str


FixtureTransport = Callable[[dict[str, RecordedRoute]], httpx.MockTransport]


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
def fixture_transport() -> FixtureTransport:
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
                return httpx.Response(status_code, headers=headers, text=cast(str, body))
            return httpx.Response(status_code, headers=headers, content=body)

        return httpx.MockTransport(handler)

    return build


@pytest.fixture
def fetch_config() -> FetchConfig:
    return FetchConfig(
        request_timeout_seconds=5,
        retries=0,
        backoff_seconds=0,
        total_deadline_seconds=300,
    )


def _request_client(
    *,
    config: FetchConfig,
    transport: httpx.AsyncBaseTransport,
) -> RequestClient:
    return RequestClient(
        config=config,
        deadline=Deadline.start(config.total_deadline_seconds),
        transport=transport,
    )


def _arxiv_route(*, start: int, page_size: int, search_query: str) -> str:
    return (
        "https://export.arxiv.org/api/query"
        f"?search_query={search_query}&start={start}&max_results={page_size}&sortBy=submittedDate"
    )


def _huggingface_route(*, page: int, page_size: int, date: str) -> str:
    return (
        "https://huggingface.co/api/daily_papers"
        f"?date={date}&p={page}&limit={page_size}"
    )


@pytest.fixture
def arxiv_config() -> AdapterConfig:
    return AdapterConfig(
        name="arxiv",
        lookback_days=7,
        page_size=2,
        max_pages=10,
        max_results=10,
        filters={"search_query": "cat:cs.CL"},
    )


@pytest.fixture
def arxiv_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _arxiv_route(
                start=0,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page.xml",
            },
            _arxiv_route(
                start=2,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-2.xml",
            },
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def arxiv_malformed_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _arxiv_route(
                start=0,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-malformed-record.xml",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def arxiv_invalid_xml_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _arxiv_route(
                start=0,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-invalid.xml",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def arxiv_out_of_window_only_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _arxiv_route(
                start=0,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-out-of-window-only.xml",
            },
            _arxiv_route(
                start=1,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-2.xml",
            },
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def arxiv_malformed_only_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _arxiv_route(
                start=0,
                page_size=arxiv_config.page_size,
                search_query="cat%3Acs.CL",
            ): {
                "fixture": "adapters/arxiv/page-malformed-only.xml",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def huggingface_config() -> AdapterConfig:
    return AdapterConfig(
        name="huggingface",
        lookback_days=7,
        page_size=2,
        max_pages=10,
        max_results=10,
        filters={},
    )


@pytest.fixture
def huggingface_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _huggingface_route(
                page=0,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page.json",
            },
            _huggingface_route(
                page=1,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-2.json",
            },
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def huggingface_malformed_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _huggingface_route(
                page=0,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-malformed-record.json",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def huggingface_invalid_json_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _huggingface_route(
                page=0,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-invalid.json",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def huggingface_out_of_window_only_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _huggingface_route(
                page=0,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-out-of-window-only.json",
            },
            _huggingface_route(
                page=1,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-2.json",
            },
        }
    )
    return _request_client(config=fetch_config, transport=transport)


@pytest.fixture
def huggingface_malformed_only_client(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> RequestClient:
    transport = fixture_transport(
        {
            _huggingface_route(
                page=0,
                page_size=huggingface_config.page_size,
                date="2024-01-08",
            ): {
                "fixture": "adapters/huggingface/page-malformed-only.json",
            }
        }
    )
    return _request_client(config=fetch_config, transport=transport)


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
