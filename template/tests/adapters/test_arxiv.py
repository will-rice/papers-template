import base64
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from papers_pipeline.adapters.arxiv import ArxivAdapter
from papers_pipeline.adapters.base import FetchWindow
from papers_pipeline.config import (
    AdapterConfig,
    ConcurrencyConfig,
    ConversionConfig,
    FetchConfig,
    PipelineConfig,
    RepositoryConfig,
    TopicConfig,
)
from papers_pipeline.errors import InfrastructureError, SourceUnavailableError
from papers_pipeline.fetch import fetch_all
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.models import PipelineState, SourceContinuation, SourceRecord
from papers_pipeline.normalize import deduplicate, normalize

from .contract import assert_adapter_contract


@pytest.mark.asyncio
async def test_arxiv_contract(
    arxiv_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    await assert_adapter_contract(ArxivAdapter(), arxiv_client, arxiv_config)


@pytest.mark.asyncio
async def test_arxiv_continuation_cursor_is_deterministic_and_opaque(
    arxiv_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    adapter = ArxivAdapter()
    first_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_client,
        config=arxiv_config,
    )

    page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=first_page.next_cursor,
        client=arxiv_client,
        config=arxiv_config,
    )

    assert first_page.next_cursor
    assert first_page.next_cursor != "1"
    assert tuple(record.source_id for record in page.records) == ("2401.00002",)
    assert page.next_cursor
    assert page.next_cursor != first_page.next_cursor
    assert page.capped is False
    assert page.permanent_errors == ()


@pytest.mark.asyncio
async def test_arxiv_leaves_the_page_budget_to_the_fetch_loop(
    arxiv_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    adapter = ArxivAdapter()
    first_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_client,
        config=arxiv_config,
    )

    page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=first_page.next_cursor,
        client=arxiv_client,
        config=arxiv_config.model_copy(update={"max_pages": 2, "max_results": 10}),
    )

    assert page.next_cursor
    assert page.capped is False


@pytest.mark.asyncio
async def test_arxiv_max_results_sets_capped_without_dropping_cursor(
    arxiv_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    page = await ArxivAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_client,
        config=arxiv_config.model_copy(update={"max_pages": 10, "max_results": 1}),
    )

    assert tuple(record.source_id for record in page.records) == ("2401.00001",)
    assert page.next_cursor
    assert page.capped is True


@pytest.mark.asyncio
async def test_arxiv_record_fields_normalize_correctly(
    arxiv_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    page = await ArxivAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_client,
        config=arxiv_config,
    )

    record = page.records[0]
    paper = normalize(record)

    assert record.source == "arxiv"
    assert record.source_id == "2401.00001"
    assert record.arxiv_id == "2401.00001"
    assert record.authors == ("A. Author",)
    assert record.categories == ("cs.CL",)
    assert record.url == "https://arxiv.org/abs/2401.00001"
    assert record.input_url == "https://arxiv.org/pdf/2401.00001"
    assert paper.identifier == "arxiv:2401.00001"


@pytest.mark.asyncio
async def test_arxiv_keeps_valid_records_and_surfaces_malformed_entries(
    arxiv_malformed_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    page = await ArxivAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_malformed_client,
        config=arxiv_config,
    )

    assert tuple(record.source_id for record in page.records) == ("2401.00001",)
    assert page.permanent_errors == (
        "arxiv record missing published timestamp: 2401.99999",
    )


@pytest.mark.asyncio
async def test_arxiv_all_out_of_window_page_preserves_progress_for_continuation(
    arxiv_out_of_window_only_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    adapter = ArxivAdapter()
    first_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_out_of_window_only_client,
        config=arxiv_config,
    )

    second_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=first_page.next_cursor,
        client=arxiv_out_of_window_only_client,
        config=arxiv_config,
    )

    assert first_page.records == ()
    assert first_page.next_cursor
    assert first_page.capped is False
    assert first_page.permanent_errors == ()
    assert tuple(record.source_id for record in second_page.records) == ("2401.00002",)


@pytest.mark.asyncio
async def test_arxiv_malformed_only_page_counts_toward_max_results(
    arxiv_malformed_only_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    page = await ArxivAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=arxiv_malformed_only_client,
        config=arxiv_config.model_copy(update={"max_pages": 10, "max_results": 1}),
    )

    assert page.records == ()
    assert page.next_cursor
    assert page.capped is True
    assert page.permanent_errors == (
        "arxiv record missing published timestamp: 2401.99998",
    )


@pytest.mark.asyncio
async def test_arxiv_invalid_atom_page_is_infrastructure_failure(
    arxiv_invalid_xml_client: RequestClient, arxiv_config: AdapterConfig
) -> None:
    with pytest.raises(InfrastructureError, match="invalid XML"):
        await ArxivAdapter().fetch(
            window=FetchWindow(
                start=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end=datetime(2024, 1, 8, tzinfo=timezone.utc),
            ),
            cursor=None,
            client=arxiv_invalid_xml_client,
            config=arxiv_config,
        )


@pytest.mark.asyncio
async def test_arxiv_remote_query_is_bound_to_window_and_stable_across_resume(
    fetch_config: FetchConfig,
    arxiv_config: AdapterConfig,
) -> None:
    requests: list[httpx.Request] = []
    fixtures = Path(__file__).parents[1] / "fixtures" / "adapters" / "arxiv"

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        start = request.url.params["start"]
        fixture = "page.xml" if start == "0" else "page-2.xml"
        return httpx.Response(200, text=(fixtures / fixture).read_text())

    client = RequestClient(
        fetch_config,
        Deadline.start(fetch_config.total_deadline_seconds),
        transport=httpx.MockTransport(handler),
    )
    adapter = ArxivAdapter()
    window = FetchWindow(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )

    first = await adapter.fetch(window, None, client, arxiv_config)
    second = await adapter.fetch(window, first.next_cursor, client, arxiv_config)

    expected_query = "(cat:cs.CL) AND submittedDate:[202401010000 TO 202401080000]"
    assert [request.url.params["search_query"] for request in requests] == [
        expected_query,
        expected_query,
    ]
    assert [request.url.params["sortBy"] for request in requests] == [
        "submittedDate",
        "submittedDate",
    ]
    assert [request.url.params["sortOrder"] for request in requests] == [
        "ascending",
        "ascending",
    ]
    assert [request.url.params["start"] for request in requests] == ["0", "2"]
    assert tuple(record.source_id for record in second.records) == ("2401.00002",)


def _arxiv_feed(total: int, *entries: str) -> str:
    return (
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">'
        f"<opensearch:totalResults>{total}</opensearch:totalResults>"
        f"{''.join(entries)}</feed>"
    )


ARXIV_ENTRY = (
    "<entry><id>http://arxiv.org/abs/2401.00009v1</id>"
    "<published>2024-01-02T00:00:00Z</published><title>Paper</title>"
    "<summary>Abstract</summary><author><name>A. Author</name></author></entry>"
)
ARXIV_WINDOW = FetchWindow(
    start=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end=datetime(2024, 1, 8, tzinfo=timezone.utc),
)


@pytest.mark.asyncio
async def test_arxiv_retries_an_empty_page_before_total_results(
    arxiv_config: AdapterConfig,
) -> None:
    bodies = [_arxiv_feed(1), _arxiv_feed(1, ARXIV_ENTRY)]
    transport = httpx.MockTransport(lambda _: httpx.Response(200, text=bodies.pop(0)))
    config = FetchConfig(
        request_timeout_seconds=5,
        retries=1,
        backoff_seconds=0,
        total_deadline_seconds=300,
    )
    async with RequestClient(
        config, Deadline.start(300), transport=transport
    ) as client:
        page = await ArxivAdapter().fetch(ARXIV_WINDOW, None, client, arxiv_config)

    assert tuple(record.source_id for record in page.records) == ("2401.00009v1",)
    assert page.next_cursor is not None


@pytest.mark.asyncio
async def test_arxiv_empty_page_before_total_results_is_not_complete(
    arxiv_config: AdapterConfig,
) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, text=_arxiv_feed(5)))
    config = FetchConfig(
        request_timeout_seconds=5,
        retries=1,
        backoff_seconds=0,
        total_deadline_seconds=300,
    )
    async with RequestClient(
        config, Deadline.start(300), transport=transport
    ) as client:
        with pytest.raises(SourceUnavailableError, match="retries exhausted"):
            await ArxivAdapter().fetch(ARXIV_WINDOW, None, client, arxiv_config)


@pytest.mark.asyncio
@pytest.mark.parametrize("arxiv_id", ["cs/9812006v1", "cmp-lg/9608012v1"])
async def test_arxiv_keeps_the_archive_of_an_old_style_id(
    arxiv_config: AdapterConfig, arxiv_id: str
) -> None:
    entry = ARXIV_ENTRY.replace("2401.00009v1", arxiv_id)
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, text=_arxiv_feed(1, entry))
    )
    config = FetchConfig(
        request_timeout_seconds=5,
        retries=1,
        backoff_seconds=0,
        total_deadline_seconds=300,
    )
    async with RequestClient(
        config, Deadline.start(300), transport=transport
    ) as client:
        page = await ArxivAdapter().fetch(ARXIV_WINDOW, None, client, arxiv_config)

    [record] = page.records
    assert record.arxiv_id == arxiv_id
    assert record.url == f"https://arxiv.org/abs/{arxiv_id}"
    assert record.input_url == f"https://arxiv.org/pdf/{arxiv_id}"


@pytest.mark.asyncio
async def test_arxiv_empty_page_at_total_results_completes(
    arxiv_config: AdapterConfig,
) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(200, text=_arxiv_feed(0)))
    config = FetchConfig(
        request_timeout_seconds=5,
        retries=1,
        backoff_seconds=0,
        total_deadline_seconds=300,
    )
    async with RequestClient(
        config, Deadline.start(300), transport=transport
    ) as client:
        page = await ArxivAdapter().fetch(ARXIV_WINDOW, None, client, arxiv_config)

    assert page.next_cursor is None


LATE_NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
LATE_QUERY = 'all:"speech enhancement"'


def _late_entry(arxiv_id: str, published: str, updated: str) -> str:
    return (
        f"<entry><id>http://arxiv.org/abs/{arxiv_id}</id>"
        f"<published>{published}</published><updated>{updated}</updated>"
        f"<title>Paper {arxiv_id}</title><summary>Abstract</summary>"
        "<author><name>A. Author</name></author></entry>"
    )


NEW_IN_WINDOW = _late_entry(
    "2610.20000v1", "2026-10-20T00:00:00Z", "2026-10-20T00:00:00Z"
)
REVISED_IN_WINDOW = _late_entry(
    "2609.02376v2", "2026-09-02T09:48:26Z", "2026-10-30T19:59:43Z"
)
LATE_IN_WINDOW = _late_entry(
    "2610.12476v2", "2026-08-14T09:03:34Z", "2026-10-16T18:55:01Z"
)
UPDATED_BEFORE_WINDOW = _late_entry(
    "2608.00001v2", "2026-08-01T00:00:00Z", "2026-09-15T00:00:00Z"
)
LATE_FEEDS = {
    ("submittedDate", "ascending", "0"): _arxiv_feed(1, NEW_IN_WINDOW),
    ("submittedDate", "ascending", "1"): _arxiv_feed(1),
    ("lastUpdatedDate", "descending", "0"): _arxiv_feed(
        4, REVISED_IN_WINDOW, NEW_IN_WINDOW
    ),
    ("lastUpdatedDate", "descending", "2"): _arxiv_feed(
        4, LATE_IN_WINDOW, UPDATED_BEFORE_WINDOW
    ),
}


def _late_config(**limits: int) -> PipelineConfig:
    return PipelineConfig(
        repository=RepositoryConfig(
            name="Example Papers", slug="example-papers", description="Example topic"
        ),
        adapters=[
            AdapterConfig(
                name="arxiv",
                lookback_days=30,
                page_size=2,
                max_pages=limits.get("max_pages", 10),
                max_results=10,
                filters={"search_query": LATE_QUERY},
            )
        ],
        topic=TopicConfig(),
        fetch=FetchConfig(
            request_timeout_seconds=5,
            retries=0,
            backoff_seconds=0,
            total_deadline_seconds=300,
        ),
        conversion=ConversionConfig(
            max_batches_per_run=1,
            max_papers=5,
            max_cost=10,
            html_cost=1,
            latex_cost=1,
            pdf_cost=1,
        ),
        concurrency=ConcurrencyConfig(html=1, latex=1, pdf=1),
    )


def _late_factory(
    config: PipelineConfig, requests: list[httpx.Request]
) -> Callable[[Deadline], RequestClient]:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = request.url.params
        key = (params["sortBy"], params["sortOrder"], params["start"])
        return httpx.Response(200, text=LATE_FEEDS[key])

    return lambda deadline: RequestClient(
        config.fetch, deadline, transport=httpx.MockTransport(handler)
    )


@pytest.mark.asyncio
async def test_arxiv_forward_fetch_finds_papers_updated_inside_the_window() -> None:
    config = _late_config()
    requests: list[httpx.Request] = []

    result = await fetch_all(
        config,
        PipelineState(),
        {"arxiv": ArxivAdapter()},
        _late_factory(config, requests),
        LATE_NOW,
    )

    assert {record.source_id for record in result.records} == {
        "2610.20000v1",
        "2609.02376v2",
        "2610.12476v2",
    }
    assert result.state.continuations == {}
    assert result.state.fetched_through == {"arxiv": LATE_NOW}
    sweeps = [r for r in requests if r.url.params["sortBy"] == "lastUpdatedDate"]
    assert [r.url.params["start"] for r in sweeps] == ["0", "2"]
    assert {r.url.params["search_query"] for r in sweeps} == {LATE_QUERY}


@pytest.mark.asyncio
async def test_arxiv_update_sweep_resumes_from_a_saved_cursor() -> None:
    config = _late_config(max_pages=3)
    requests: list[httpx.Request] = []
    factory = _late_factory(config, requests)
    adapters = {"arxiv": ArxivAdapter()}

    first = await fetch_all(config, PipelineState(), adapters, factory, LATE_NOW)
    second = await fetch_all(config, first.state, adapters, factory, LATE_NOW)

    assert "arxiv" in first.state.continuations
    assert "2610.12476v2" not in {record.source_id for record in first.records}
    assert "2610.12476v2" in {record.source_id for record in second.records}
    assert second.state.continuations == {}


@pytest.mark.asyncio
async def test_arxiv_backfill_never_walks_the_update_sort() -> None:
    config = _late_config()
    dated = config.adapters[0].model_copy(
        update={"backfill_start": date(2026, 9, 1), "backfill_days": 365}
    )
    config = config.model_copy(update={"adapters": [dated]})
    requests: list[httpx.Request] = []
    state = PipelineState(fetched_through={"arxiv": LATE_NOW})

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = request.url.params
        key = (params["sortBy"], params["sortOrder"], params["start"])
        if "submittedDate:[202609010000" in params["search_query"]:
            return httpx.Response(200, text=_arxiv_feed(0))
        return httpx.Response(200, text=LATE_FEEDS[key])

    result = await fetch_all(
        config,
        state,
        {"arxiv": ArxivAdapter()},
        lambda deadline: RequestClient(
            config.fetch, deadline, transport=httpx.MockTransport(handler)
        ),
        LATE_NOW,
    )

    history = [
        r
        for r in requests
        if "submittedDate:[202609010000" in r.url.params["search_query"]
    ]
    assert [r.url.params["sortBy"] for r in history] == ["submittedDate"]
    assert result.state.backfill["arxiv"].covered_from == datetime(
        2026, 9, 1, tzinfo=timezone.utc
    )


@pytest.mark.asyncio
async def test_arxiv_new_version_keeps_the_stored_identifier() -> None:
    config = _late_config()
    stored = normalize(
        SourceRecord(
            source="arxiv",
            source_id="2609.02376v1",
            arxiv_id="2609.02376v1",
            title="Paper 2609.02376v2",
            abstract="Abstract",
            authors=("A. Author",),
            published=datetime(2026, 9, 2, 9, 48, 26, tzinfo=timezone.utc),
            url="https://arxiv.org/abs/2609.02376v1",
            input_format="pdf",
            input_url="https://arxiv.org/pdf/2609.02376v1",
        )
    )

    result = await fetch_all(
        config,
        PipelineState(),
        {"arxiv": ArxivAdapter()},
        _late_factory(config, []),
        LATE_NOW,
    )
    rows = deduplicate(map(normalize, result.records), inventory=[stored])

    assert [row for row in rows if row.identifier == stored.identifier] == [stored]
    assert not any(row.identifier == "arxiv:2609.02376v2" for row in rows)


@pytest.mark.asyncio
async def test_arxiv_cursor_saved_before_the_sweep_resumes_and_reaches_it() -> None:
    config = _late_config()
    requests: list[httpx.Request] = []
    saved = base64.urlsafe_b64encode(b'{"consumed":1,"page":1,"start":1}').decode()
    state = PipelineState(
        continuations={
            "arxiv": SourceContinuation(
                cursor=saved.rstrip("="),
                window_start=LATE_NOW - timedelta(days=30),
                window_end=LATE_NOW,
            )
        }
    )

    result = await fetch_all(
        config,
        state,
        {"arxiv": ArxivAdapter()},
        _late_factory(config, requests),
        LATE_NOW,
    )

    assert [
        (request.url.params["sortBy"], request.url.params["start"])
        for request in requests
    ] == [
        ("submittedDate", "1"),
        ("lastUpdatedDate", "0"),
        ("lastUpdatedDate", "2"),
    ]
    assert result.state.continuations == {}
