from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from pydantic import TypeAdapter

from papers_pipeline.adapters import build_adapters
from papers_pipeline.adapters.arxiv import ArxivAdapter
from papers_pipeline.adapters.base import Adapter, FetchPage, FetchWindow
from papers_pipeline.adapters.dblp import DblpAdapter
from papers_pipeline.adapters.semantic_scholar import SemanticScholarAdapter
from papers_pipeline.config import (
    AdapterConfig,
    AdapterName,
    ConcurrencyConfig,
    ConversionConfig,
    FetchConfig,
    PipelineConfig,
    RepositoryConfig,
    TopicConfig,
)
from papers_pipeline.errors import InfrastructureError, SourceUnavailableError
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.models import (
    BackfillProgress,
    PipelineState,
    SourceContinuation,
    SourceRecord,
)
from papers_pipeline.fetch import fetch_all
from papers_pipeline.state import load_state, save_state

NOW = datetime(2026, 9, 23, 18, 29, 6, tzinfo=timezone.utc)


def source_record(
    source: str,
    source_id: str,
    *,
    published: datetime = NOW - timedelta(days=1),
) -> SourceRecord:
    return SourceRecord(
        source=source,
        source_id=source_id,
        title=f"{source_id} title",
        abstract=f"{source_id} abstract",
        authors=("A. Author",),
        published=published,
        url=f"https://example.test/{source}/{source_id}",
        input_format="pdf",
        input_url=f"https://example.test/{source}/{source_id}.pdf",
    )


def pipeline_config(*adapters: AdapterConfig) -> PipelineConfig:
    return PipelineConfig(
        repository=RepositoryConfig(
            name="Example Papers",
            slug="example-papers",
            description="Example topic",
        ),
        adapters=list(adapters),
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


class TrackingClient(RequestClient):
    def __init__(
        self,
        *,
        config: FetchConfig,
        deadline: Deadline,
        seed_events: tuple[str, ...] = (),
    ) -> None:
        super().__init__(
            config=config,
            deadline=deadline,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    599,
                    request=request,
                    text=f"unexpected network call: {request.url}",
                )
            ),
        )
        self.events.extend(seed_events)
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True
        await super().aclose()


@dataclass
class RecordingAdapter:
    name: str
    pages: list[FetchPage] = field(default_factory=list)
    failure: InfrastructureError | None = None
    record_sources: frozenset[str] = field(default_factory=lambda: frozenset({"stub"}))
    windows: list[FetchWindow] = field(default_factory=list)
    cursors: list[str | None] = field(default_factory=list)
    clients: list[RequestClient] = field(default_factory=list)
    configs: list[AdapterConfig] = field(default_factory=list)

    async def fetch(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
    ) -> FetchPage:
        self.windows.append(window)
        self.cursors.append(cursor)
        self.clients.append(client)
        self.configs.append(config)
        if self.failure is not None:
            raise self.failure
        if not self.pages:
            raise AssertionError(f"no recorded page left for {self.name}")
        return self.pages.pop(0)


def adapter_config(
    name: AdapterName,
    *,
    lookback_days: int,
    page_size: int = 2,
    max_pages: int = 10,
    max_results: int = 10,
    enabled: bool = True,
) -> AdapterConfig:
    return AdapterConfig(
        name=name,
        enabled=enabled,
        lookback_days=lookback_days,
        page_size=page_size,
        max_pages=max_pages,
        max_results=max_results,
        filters={},
    )


def client_factory(
    config: FetchConfig,
    *,
    seed_events: tuple[str, ...] = (),
) -> tuple[Callable[[Deadline], TrackingClient], list[TrackingClient], list[Deadline]]:
    created: list[TrackingClient] = []
    deadlines: list[Deadline] = []

    def build(deadline: Deadline) -> TrackingClient:
        deadlines.append(deadline)
        client = TrackingClient(
            config=config, deadline=deadline, seed_events=seed_events
        )
        created.append(client)
        return client

    return build, created, deadlines


@pytest.mark.asyncio
async def test_fetch_uses_source_windows_continues_after_zero_output_and_clears_completed_cursor() -> (
    None
):
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    state = PipelineState(
        continuations={
            "arxiv": SourceContinuation(
                cursor="opaque-start",
                window_start=NOW - timedelta(days=7),
                window_end=NOW,
            )
        }
    )
    adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(), next_cursor="opaque-mid", capped=False, permanent_errors=()
            ),
            FetchPage(
                records=(source_record("arxiv", "2401.00001"),),
                next_cursor="opaque-final",
                capped=False,
                permanent_errors=(),
            ),
            FetchPage(
                records=(source_record("arxiv", "2401.00002"),),
                next_cursor=None,
                capped=False,
                permanent_errors=("malformed arxiv record",),
            ),
        ],
    )
    factory, clients, _ = client_factory(
        config.fetch,
        seed_events=("retry 1: HTTP 503 for https://example.test/arxiv",),
    )

    result = await fetch_all(config, state, {"arxiv": adapter}, factory, NOW)

    assert adapter.windows == [
        FetchWindow(start=NOW - timedelta(days=7), end=NOW),
        FetchWindow(start=NOW - timedelta(days=7), end=NOW),
        FetchWindow(start=NOW - timedelta(days=7), end=NOW),
    ]
    assert adapter.cursors == ["opaque-start", "opaque-mid", "opaque-final"]
    assert len({id(client) for client in adapter.clients}) == 1
    assert clients[0].closed is True
    assert tuple(record.source_id for record in result.records) == (
        "2401.00001",
        "2401.00002",
    )
    assert result.state.continuations == {}
    assert result.stats[0].source == "arxiv"
    assert result.stats[0].fetched == 2
    assert result.stats[0].rejected == 1
    assert result.stats[0].capped is False
    assert result.stats[0].complete is True
    assert result.events == (
        "arxiv: retry 1: HTTP 503 for https://example.test/arxiv",
        "arxiv: permanent error: malformed arxiv record",
        "arxiv: fetch complete; cursor cleared",
    )


@pytest.mark.asyncio
async def test_fetch_persists_cursor_and_marks_result_cap_without_completing() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7, max_results=1))
    adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(source_record("arxiv", "2401.00001"),),
                next_cursor="opaque-next",
                capped=False,
                permanent_errors=(),
            ),
        ],
    )
    factory, clients, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    assert clients[0].closed is True
    assert result.state.continuations == {
        "arxiv": SourceContinuation(
            cursor="opaque-next",
            window_start=NOW - timedelta(days=7),
            window_end=NOW,
        )
    }
    assert result.stats[0].fetched == 1
    assert result.stats[0].rejected == 0
    assert result.stats[0].capped is True
    assert result.stats[0].complete is False
    assert result.events == ("arxiv: cap reached; continuation persisted",)


@pytest.mark.asyncio
async def test_fetch_raises_when_adapter_returns_more_records_than_remaining_budget() -> (
    None
):
    config = pipeline_config(
        adapter_config("arxiv", lookback_days=7, max_results=3, page_size=2)
    )
    adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(
                    source_record("arxiv", "2401.00001"),
                    source_record("arxiv", "2401.00002"),
                ),
                next_cursor="opaque-mid",
                capped=False,
                permanent_errors=(),
            ),
            FetchPage(
                records=(
                    source_record("arxiv", "2401.00003"),
                    source_record("arxiv", "2401.00004"),
                ),
                next_cursor="opaque-skip",
                capped=False,
                permanent_errors=(),
            ),
        ],
    )
    factory, clients, _ = client_factory(config.fetch)

    with pytest.raises(
        InfrastructureError,
        match="arxiv returned 2 records with only 1 results remaining",
    ):
        await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    assert adapter.cursors == [None, "opaque-mid"]
    assert len(adapter.pages) == 0
    assert clients[0].closed is True


@pytest.mark.asyncio
async def test_fetch_passes_remaining_budget_to_adapter_and_preserves_page_two_continuation() -> (
    None
):
    config = pipeline_config(
        adapter_config("arxiv", lookback_days=7, max_results=3, page_size=2)
    )
    adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(
                    source_record("arxiv", "2401.00001"),
                    source_record("arxiv", "2401.00002"),
                ),
                next_cursor="opaque-mid",
                capped=False,
                permanent_errors=(),
            ),
            FetchPage(
                records=(source_record("arxiv", "2401.00003"),),
                next_cursor="opaque-tail",
                capped=True,
                permanent_errors=(),
            ),
        ],
    )
    factory, clients, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    assert [(cfg.page_size, cfg.max_results) for cfg in adapter.configs] == [
        (2, 3),
        (1, 1),
    ]
    assert tuple(record.source_id for record in result.records) == (
        "2401.00001",
        "2401.00002",
        "2401.00003",
    )
    assert len({record.source_id for record in result.records}) == 3
    assert result.state.continuations["arxiv"].cursor == "opaque-tail"
    assert result.stats[0].fetched == 3
    assert result.stats[0].capped is True
    assert result.stats[0].complete is False
    assert result.events == ("arxiv: cap reached; continuation persisted",)
    assert clients[0].closed is True


@pytest.mark.asyncio
async def test_fetch_enforces_page_cap_and_uses_configured_adapter_order_under_one_deadline() -> (
    None
):
    config = pipeline_config(
        adapter_config("dblp", lookback_days=3, max_pages=1),
        adapter_config("arxiv", lookback_days=7),
    )
    dblp = RecordingAdapter(
        "dblp",
        pages=[
            FetchPage(
                records=(source_record("dblp", "dblp-1"),),
                next_cursor="dblp-cursor",
                capped=False,
                permanent_errors=(),
            ),
        ],
    )
    arxiv = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(source_record("arxiv", "2401.00003"),),
                next_cursor=None,
                capped=False,
                permanent_errors=(),
            ),
        ],
    )
    factory, clients, deadlines = client_factory(config.fetch)

    result = await fetch_all(
        config,
        PipelineState(),
        {"arxiv": arxiv, "dblp": dblp},
        factory,
        NOW,
    )

    assert dblp.windows == [FetchWindow(start=NOW - timedelta(days=3), end=NOW)]
    assert arxiv.windows == [FetchWindow(start=NOW - timedelta(days=7), end=NOW)]
    assert tuple(record.source for record in result.records) == ("dblp", "arxiv")
    assert tuple(stat.source for stat in result.stats) == ("dblp", "arxiv")
    assert result.state.continuations["dblp"].cursor == "dblp-cursor"
    assert result.stats[0].capped is True
    assert result.stats[0].complete is False
    assert result.stats[1].capped is False
    assert result.stats[1].complete is True
    assert len(clients) == 2
    assert clients[0].closed is True
    assert clients[1].closed is True
    assert deadlines[0] is deadlines[1]
    assert result.events == (
        "dblp: cap reached; continuation persisted",
        "arxiv: fetch complete",
    )


@pytest.mark.asyncio
async def test_unavailable_source_is_skipped_and_keeps_its_progress() -> None:
    config = pipeline_config(
        adapter_config("arxiv", lookback_days=7),
        adapter_config("dblp", lookback_days=3),
    )
    continuation = SourceContinuation(
        cursor="resume", window_start=NOW - timedelta(days=7), window_end=NOW
    )
    failing = RecordingAdapter(
        "arxiv",
        failure=SourceUnavailableError(
            "request retries exhausted: https://example.test/arxiv"
        ),
    )
    healthy = RecordingAdapter(
        "dblp",
        pages=[
            FetchPage(
                records=(source_record("dblp", "dblp-1"),),
                next_cursor=None,
                capped=False,
                permanent_errors=(),
            )
        ],
    )
    factory, clients, _ = client_factory(config.fetch)

    result = await fetch_all(
        config,
        PipelineState(continuations={"arxiv": continuation}),
        {"arxiv": failing, "dblp": healthy},
        factory,
        NOW,
    )

    assert all(client.closed for client in clients)
    assert [record.source_id for record in result.records] == ["dblp-1"]
    assert result.state.continuations == {"arxiv": continuation}
    assert (
        "arxiv: source unavailable this run: request retries exhausted: "
        "https://example.test/arxiv"
    ) in result.events
    assert [(item.source, item.fetched) for item in result.stats] == [
        ("arxiv", 0),
        ("dblp", 1),
    ]


@pytest.mark.asyncio
async def test_later_run_reuses_persisted_window_until_source_completes() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7, max_pages=1))
    first_adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(source_record("arxiv", "first"),),
                next_cursor="resume",
                capped=False,
                permanent_errors=(),
            )
        ],
    )
    factory, _, _ = client_factory(config.fetch)
    first = await fetch_all(
        config, PipelineState(), {"arxiv": first_adapter}, factory, NOW
    )

    later = NOW + timedelta(days=2)
    second_adapter = RecordingAdapter(
        "arxiv",
        pages=[
            FetchPage(
                records=(source_record("arxiv", "second"),),
                next_cursor=None,
                capped=False,
                permanent_errors=(),
            )
        ],
    )
    resumed = await fetch_all(
        config, first.state, {"arxiv": second_adapter}, factory, later
    )

    assert second_adapter.windows == [
        FetchWindow(start=NOW - timedelta(days=7), end=NOW)
    ]
    assert second_adapter.cursors == ["resume"]
    assert resumed.state.continuations == {}


def page(*records: SourceRecord, next_cursor: str | None = None) -> FetchPage:
    return FetchPage(
        records=records, next_cursor=next_cursor, capped=False, permanent_errors=()
    )


def backfill_config(
    start: date, *, backfill_days: int = 30, max_pages: int = 10
) -> PipelineConfig:
    return pipeline_config(
        adapter_config("arxiv", lookback_days=7, max_pages=max_pages).model_copy(
            update={"backfill_start": start, "backfill_days": backfill_days}
        )
    )


@pytest.mark.asyncio
async def test_backfill_walks_chunks_until_the_run_budget_is_spent() -> None:
    config = backfill_config(date(2000, 1, 1), max_pages=2)
    old = source_record("arxiv", "old", published=NOW - timedelta(days=20))
    adapter = RecordingAdapter("arxiv", pages=[page(), page(old), page()])
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    lookback_start = NOW - timedelta(days=7)
    assert adapter.windows == [
        FetchWindow(start=lookback_start, end=NOW),
        FetchWindow(start=lookback_start - timedelta(days=30), end=lookback_start),
        FetchWindow(
            start=lookback_start - timedelta(days=60),
            end=lookback_start - timedelta(days=30),
        ),
    ]
    assert result.records == (old,)
    assert result.stats[0].fetched == 1
    covered_from = lookback_start - timedelta(days=60)
    assert result.state.backfill == {
        "arxiv": BackfillProgress(covered_from=covered_from)
    }
    assert result.events[-1] == f"arxiv: backfilled to {covered_from.date()}"


@pytest.mark.asyncio
async def test_capped_backfill_chunk_resumes_before_stepping_further_back() -> None:
    covered_from = NOW - timedelta(days=100)
    history_window = FetchWindow(
        start=covered_from - timedelta(days=30), end=covered_from
    )
    state = PipelineState(
        backfill={
            "arxiv": BackfillProgress(
                covered_from=covered_from,
                continuation=SourceContinuation(
                    cursor="history-2",
                    window_start=history_window.start,
                    window_end=history_window.end,
                ),
            )
        }
    )
    adapter = RecordingAdapter(
        "arxiv",
        pages=[page(), page(next_cursor="history-3"), page(), page(), page()],
    )
    factory, _, _ = client_factory(backfill_config(date(2000, 1, 1)).fetch)

    first = await fetch_all(
        backfill_config(date(2000, 1, 1), max_pages=1),
        state,
        {"arxiv": adapter},
        factory,
        NOW,
    )
    second = await fetch_all(
        backfill_config(date(2000, 1, 1), max_pages=2),
        first.state,
        {"arxiv": adapter},
        factory,
        NOW,
    )

    # Run 1: lookback, then one page of the resumed chunk (capped again).
    # Run 2: lookback, finish the chunk, then step one chunk further back.
    assert adapter.windows[1] == history_window
    assert adapter.cursors[1] == "history-2"
    assert adapter.windows[3:] == [
        history_window,
        FetchWindow(
            start=history_window.start - timedelta(days=30),
            end=history_window.start,
        ),
    ]
    assert adapter.cursors[3] == "history-3"
    assert first.state.backfill["arxiv"].covered_from == covered_from
    assert first.events[-1] == (
        f"arxiv: backfill cap reached at {covered_from.date()}; continuation persisted"
    )
    assert second.state.backfill == {
        "arxiv": BackfillProgress(
            covered_from=history_window.start - timedelta(days=30)
        )
    }


@pytest.mark.asyncio
async def test_backfill_stops_at_its_start_date() -> None:
    start = (NOW - timedelta(days=20)).date()
    config = backfill_config(start)
    adapter = RecordingAdapter("arxiv", pages=[page(), page(), page()])
    factory, _, _ = client_factory(config.fetch)

    first = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)
    second = await fetch_all(config, first.state, {"arxiv": adapter}, factory, NOW)

    limit = datetime.combine(start, time.min, tzinfo=timezone.utc)
    assert adapter.windows[1] == FetchWindow(start=limit, end=NOW - timedelta(days=7))
    assert first.events[-1] == f"arxiv: backfill complete to {start}"
    assert len(adapter.windows) == 3  # second run fetches only the lookback window
    assert second.state.backfill == {"arxiv": BackfillProgress(covered_from=limit)}


@pytest.mark.parametrize("name", ["dblp", "papers_with_code"])
def test_backfill_requires_a_source_that_queries_date_ranges(
    name: AdapterName,
) -> None:
    with pytest.raises(ValueError, match=f"{name} cannot backfill"):
        adapter_config(name, lookback_days=7).model_validate(
            {
                **adapter_config(name, lookback_days=7).model_dump(),
                "backfill_start": date(2020, 1, 1),
            }
        )


def endless_arxiv_page(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params["start"])
    entries = "".join(
        f"<entry><id>http://arxiv.org/abs/2609.{index:05d}v1</id>"
        "<published>2026-09-22T00:00:00Z</published><title>Paper</title>"
        "<summary>Abstract</summary><author><name>A. Author</name></author></entry>"
        for index in (start, start + 1)
    )
    return httpx.Response(
        200,
        text=(
            '<feed xmlns="http://www.w3.org/2005/Atom" '
            'xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">'
            f"<opensearch:totalResults>1000</opensearch:totalResults>{entries}</feed>"
        ),
    )


def endless_dblp_page(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params["f"])
    hits = [
        {
            "info": {
                "key": f"conf/test/{index}",
                "title": "Paper",
                "year": "2026",
                "ee": f"https://public.example/{index}.pdf",
            }
        }
        for index in (start, start + 1)
    ]
    return httpx.Response(200, json={"result": {"hits": {"hit": hits}}})


def endless_semantic_scholar_page(request: httpx.Request) -> httpx.Response:
    start = int(request.url.params["offset"])
    data = [
        {
            "paperId": f"paper-{index}",
            "title": "Paper",
            "publicationDate": "2026-09-22",
            "openAccessPdf": {"url": f"https://public.example/{index}.pdf"},
        }
        for index in (start, start + 1)
    ]
    return httpx.Response(200, json={"total": 1000, "next": start + 2, "data": data})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "adapter", "respond"),
    [
        ("arxiv", ArxivAdapter(), endless_arxiv_page),
        ("dblp", DblpAdapter(), endless_dblp_page),
        (
            "semantic_scholar",
            SemanticScholarAdapter("key"),
            endless_semantic_scholar_page,
        ),
    ],
    ids=["arxiv", "dblp", "semantic_scholar"],
)
async def test_resumed_fetch_gets_a_full_page_budget_every_run(
    name: AdapterName,
    adapter: Adapter,
    respond: Callable[[httpx.Request], httpx.Response],
) -> None:
    name = TypeAdapter(AdapterName).validate_python(adapter.name)
    config = pipeline_config(
        adapter_config(name, lookback_days=7, max_pages=3, max_results=1000)
    )
    requests_per_run: list[int] = []
    state = PipelineState()
    for _ in range(4):
        requests: list[httpx.URL] = []

        def handle(request: httpx.Request) -> httpx.Response:
            requests.append(request.url)
            return respond(request)

        result = await fetch_all(
            config,
            state,
            {adapter.name: adapter},
            lambda deadline: RequestClient(
                config.fetch, deadline, transport=httpx.MockTransport(handle)
            ),
            NOW,
        )
        state = result.state
        requests_per_run.append(len(requests))

    assert requests_per_run == [3, 3, 3, 3]
    assert adapter.name in state.continuations


@pytest.mark.asyncio
async def test_outage_longer_than_lookback_is_fetched_from_the_last_success() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    last_success = datetime(2026, 1, 1, tzinfo=timezone.utc)
    next_run = datetime(2026, 1, 31, tzinfo=timezone.utc)
    missed = source_record(
        "arxiv", "missed", published=datetime(2026, 1, 6, tzinfo=timezone.utc)
    )
    adapter = RecordingAdapter("arxiv", pages=[page()])
    factory, _, _ = client_factory(config.fetch)
    first = await fetch_all(
        config, PipelineState(), {"arxiv": adapter}, factory, last_success
    )

    adapter.pages.append(page(missed))
    second = await fetch_all(config, first.state, {"arxiv": adapter}, factory, next_run)

    assert adapter.windows[1] == FetchWindow(start=last_success, end=next_run)
    assert adapter.windows[1].start <= missed.published <= adapter.windows[1].end
    assert second.records == (missed,)
    assert first.state.fetched_through == {"arxiv": last_success}
    assert second.state.fetched_through == {"arxiv": next_run}


@pytest.mark.asyncio
async def test_recent_success_keeps_the_lookback_overlap() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    state = PipelineState(fetched_through={"arxiv": NOW - timedelta(days=1)})
    adapter = RecordingAdapter("arxiv", pages=[page()])
    factory, _, _ = client_factory(config.fetch)

    await fetch_all(config, state, {"arxiv": adapter}, factory, NOW)

    assert adapter.windows == [FetchWindow(start=NOW - timedelta(days=7), end=NOW)]


@pytest.mark.asyncio
async def test_catch_up_resumes_and_records_only_the_completed_window_end() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7, max_pages=1))
    last_success = NOW - timedelta(days=60)
    resumed_at = NOW + timedelta(days=30)
    window = FetchWindow(start=last_success, end=NOW)
    adapter = RecordingAdapter("arxiv", pages=[page(next_cursor="resume"), page()])
    factory, _, _ = client_factory(config.fetch)

    capped = await fetch_all(
        config,
        PipelineState(fetched_through={"arxiv": last_success}),
        {"arxiv": adapter},
        factory,
        NOW,
    )
    finished = await fetch_all(
        config, capped.state, {"arxiv": adapter}, factory, resumed_at
    )
    adapter.pages.append(page())
    await fetch_all(config, finished.state, {"arxiv": adapter}, factory, resumed_at)

    assert capped.state.fetched_through == {"arxiv": last_success}
    assert adapter.windows[:2] == [window, window]
    assert adapter.cursors[:2] == [None, "resume"]
    # The papers published while the capped window was resumed are next.
    assert finished.state.fetched_through == {"arxiv": NOW}
    assert adapter.windows[2] == FetchWindow(start=NOW, end=resumed_at)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adapter",
    [
        RecordingAdapter(
            "arxiv", failure=SourceUnavailableError("request retries exhausted")
        ),
    ],
    ids=["unavailable"],
)
async def test_incomplete_forward_window_keeps_the_last_success(
    adapter: RecordingAdapter,
) -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    last_success = NOW - timedelta(days=30)
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(
        config,
        PipelineState(fetched_through={"arxiv": last_success}),
        {"arxiv": adapter},
        factory,
        NOW,
    )

    assert result.state.fetched_through == {"arxiv": last_success}


@pytest.mark.asyncio
async def test_failed_backfill_keeps_the_forward_result() -> None:
    config = backfill_config(date(2000, 1, 1))
    last_success = NOW - timedelta(days=30)
    history = BackfillProgress(covered_from=NOW - timedelta(days=90))

    @dataclass
    class ForwardThenFailing(RecordingAdapter):
        async def fetch(
            self,
            window: FetchWindow,
            cursor: str | None,
            client: RequestClient,
            config: AdapterConfig,
        ) -> FetchPage:
            if self.windows:
                raise SourceUnavailableError("request retries exhausted")
            self.windows.append(window)
            return page(kept)

    kept = source_record("arxiv", "kept")
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(
        config,
        PipelineState(
            fetched_through={"arxiv": last_success}, backfill={"arxiv": history}
        ),
        {"arxiv": ForwardThenFailing("arxiv")},
        factory,
        NOW,
    )

    assert result.records == (kept,)
    assert result.state.fetched_through == {"arxiv": NOW}
    assert result.state.backfill == {"arxiv": history}
    assert result.stats[0].complete is True
    assert (
        "arxiv: backfill unavailable this run: request retries exhausted"
        in result.events
    )


def papers_with_code_walk(request: httpx.Request) -> httpx.Response:
    number = int(request.url.params.get("page", "1"))
    following = (
        f"https://paperswithcode.com/api/v1/papers/?page={number + 1}"
        if number < 5
        else None
    )
    return httpx.Response(200, json={"results": [{}], "next": following})


def crossref_walk(request: httpx.Request) -> httpx.Response:
    cursor = request.url.params["cursor"]
    number = 1 if cursor == "*" else int(cursor)
    message: dict[str, object] = {"items": [{}]}
    if number < 5:
        message["next-cursor"] = str(number + 1)
    return httpx.Response(200, json={"message": message})


def biorxiv_walk(request: httpx.Request) -> httpx.Response:
    offset = int(request.url.path.rsplit("/", 1)[-1])
    return httpx.Response(200, json={"collection": [{}] if offset < 5 else []})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "filters", "respond"),
    [
        ("papers_with_code", {}, papers_with_code_walk),
        ("biorxiv_crossref", {"provider": "crossref"}, crossref_walk),
        ("biorxiv_crossref", {"provider": "biorxiv"}, biorxiv_walk),
    ],
    ids=["papers_with_code", "crossref", "biorxiv"],
)
async def test_resumed_catch_up_completes_and_records_success(
    name: AdapterName,
    filters: dict[str, str],
    respond: Callable[[httpx.Request], httpx.Response],
) -> None:
    config = pipeline_config(
        adapter_config(
            name, lookback_days=7, page_size=1, max_pages=2, max_results=100
        ).model_copy(update={"filters": filters})
    )
    adapter = build_adapters(config, {})[name]
    state = PipelineState()
    requests_per_run: list[int] = []
    for _ in range(3):
        requests: list[httpx.URL] = []

        def handle(request: httpx.Request) -> httpx.Response:
            requests.append(request.url)
            return respond(request)

        result = await fetch_all(
            config,
            state,
            {name: adapter},
            lambda deadline: RequestClient(
                config.fetch, deadline, transport=httpx.MockTransport(handle)
            ),
            NOW,
        )
        state = result.state
        requests_per_run.append(len(requests))

    assert requests_per_run[:2] == [2, 2]
    assert state.continuations == {}
    assert state.fetched_through == {name: NOW}


@dataclass
class DayTableAdapter:
    """Replays a fixed page for each (first day, last day) a query can cover."""

    name: str
    table: dict[tuple[date, date], FetchPage]
    record_sources: frozenset[str] = frozenset({"stub"})
    windows: list[FetchWindow] = field(default_factory=list)

    async def fetch(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
    ) -> FetchPage:
        self.windows.append(window)
        return self.table[(window.start.date(), window.end.date())]


def one_paper_per_day_table(
    source: str, first: date, last: date
) -> dict[tuple[date, date], FetchPage]:
    """Pages of a source too dense to answer any query longer than one day."""
    days = [first + timedelta(days=n) for n in range((last - first).days + 1)]
    over_limit = FetchPage(records=(), next_cursor=None, capped=True)
    table = {(a, b): over_limit for a in days for b in days if a < b}
    for day in days:
        published = datetime.combine(day, time.min, tzinfo=timezone.utc)
        table[(day, day)] = page(
            source_record(source, day.isoformat(), published=published)
        )
    return table


def search_body(total: int, *paper_ids: tuple[str, str]) -> dict[str, object]:
    """A /paper/search body in the documented total/data shape."""
    return {
        "total": total,
        "data": [
            {
                "paperId": paper_id,
                "title": f"Paper {paper_id}",
                "publicationDate": published,
                "openAccessPdf": {"url": "https://example.test/paper.pdf"},
            }
            for paper_id, published in paper_ids
        ],
    }


@pytest.mark.asyncio
async def test_dense_catch_up_walks_bounded_windows_without_losing_a_paper() -> None:
    config = pipeline_config(
        adapter_config("arxiv", lookback_days=7, max_pages=10, max_results=500)
    )
    last_success = datetime(2026, 7, 1, tzinfo=timezone.utc)
    adapter = DayTableAdapter(
        "arxiv",
        one_paper_per_day_table("arxiv", last_success.date(), date(2026, 10, 6)),
    )
    factory, _, _ = client_factory(config.fetch)
    state = PipelineState(fetched_through={"arxiv": last_success})
    seen: set[str] = set()
    starts: list[datetime] = []
    run_at = NOW
    for night in range(12):
        run_at = NOW + timedelta(days=night)
        result = await fetch_all(config, state, {"arxiv": adapter}, factory, run_at)
        seen |= {record.source_id for record in result.records}
        state = result.state
        starts.append(state.fetched_through["arxiv"])

    assert starts == sorted(starts)
    assert starts[1] > last_success
    assert state.fetched_through["arxiv"] >= run_at - timedelta(days=7)
    covered_days = (starts[-1].date() - last_success.date()).days
    assert {
        (last_success.date() + timedelta(days=n)).isoformat()
        for n in range(covered_days)
    } <= seen


@pytest.mark.asyncio
async def test_single_day_over_the_limit_is_reported_and_passed() -> None:
    config = pipeline_config(
        adapter_config(
            "semantic_scholar",
            lookback_days=7,
            page_size=100,
            max_pages=100,
            max_results=10000,
        )
    )
    dense = "2026-09-20"
    responses: dict[tuple[str, str], dict[str, object]] = {
        ("2026-09-16:2026-09-23", "0"): search_body(1507),
        ("2026-09-16:2026-09-19", "0"): search_body(
            4,
            *((f"2026-09-{day}-0", f"2026-09-{day}") for day in (16, 17, 18, 19)),
        ),
        ("2026-09-16:2026-09-19", "4"): search_body(4),
        ("2026-09-20:2026-09-23", "0"): search_body(1503),
        ("2026-09-20:2026-09-21", "0"): search_body(1501),
        **{
            (f"{dense}:{dense}", str(offset)): search_body(
                1500,
                *((f"{dense}-{offset + n}", dense) for n in range(100)),
            )
            for offset in range(0, 1000, 100)
        },
        ("2026-09-21:2026-09-23", "0"): search_body(
            3,
            *((f"2026-09-{day}-0", f"2026-09-{day}") for day in (21, 22, 23)),
        ),
        ("2026-09-21:2026-09-23", "3"): search_body(3),
    }

    def handle(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        return httpx.Response(
            200, json=responses[(params["publicationDateOrYear"], params["offset"])]
        )

    adapter = SemanticScholarAdapter("key")

    result = await fetch_all(
        config,
        PipelineState(),
        {adapter.name: adapter},
        lambda deadline: RequestClient(
            config.fetch, deadline, transport=httpx.MockTransport(handle)
        ),
        NOW,
    )

    assert result.state.fetched_through == {adapter.name: NOW}
    assert result.stats[0].capped is True
    assert result.stats[0].complete is False
    assert any(f"window {dense}..{dense} matches 1500" in e for e in result.events)
    ids = {record.source_id for record in result.records}
    assert "2026-09-21-0" in ids
    assert "2026-09-19-0" in ids


@pytest.mark.asyncio
async def test_dense_backfill_chunk_moves_back_without_a_gap() -> None:
    first_day = (NOW - timedelta(days=41)).date()
    config = pipeline_config(
        adapter_config(
            "arxiv",
            lookback_days=1,
            max_pages=100,
            max_results=10000,
        ).model_copy(update={"backfill_start": first_day, "backfill_days": 40})
    )
    adapter = DayTableAdapter(
        "arxiv", one_paper_per_day_table("arxiv", first_day, NOW.date())
    )
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    ids = {record.source_id for record in result.records}
    assert {(first_day + timedelta(days=n)).isoformat() for n in range(40)} <= ids
    assert result.state.backfill["arxiv"].continuation is None


@pytest.mark.asyncio
async def test_dense_source_reaches_new_days_without_a_standing_lag() -> None:
    config = pipeline_config(
        adapter_config("arxiv", lookback_days=7, max_pages=2, max_results=100)
    )
    nights = [NOW + timedelta(days=night) for night in range(15)]
    adapter = DayTableAdapter(
        "arxiv",
        one_paper_per_day_table(
            "arxiv", (NOW - timedelta(days=7)).date(), nights[-1].date()
        ),
    )
    factory, _, _ = client_factory(config.fetch)
    state = PipelineState()
    first_seen: dict[str, date] = {}
    completed = 0
    for run_at in nights:
        result = await fetch_all(config, state, {"arxiv": adapter}, factory, run_at)
        state = result.state
        completed += result.stats[0].complete
        for record in result.records:
            first_seen.setdefault(record.source_id, run_at.date())

    assert completed > 0
    # The initial lookback takes a few nights; after that a day waits at most one.
    for run_at in nights[4:-1]:
        day = run_at.date()
        assert (first_seen[day.isoformat()] - day).days <= 1


@pytest.mark.asyncio
async def test_stop_inside_the_overlap_resumes_at_the_frontier() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7, max_pages=1))
    recorded = NOW - timedelta(days=2)
    adapter = RecordingAdapter("arxiv", pages=[page(next_cursor="overlap"), page()])
    factory, _, _ = client_factory(config.fetch)

    stopped = await fetch_all(
        config,
        PipelineState(fetched_through={"arxiv": recorded}),
        {"arxiv": adapter},
        factory,
        NOW,
    )
    later = NOW + timedelta(days=1)
    resumed = await fetch_all(config, stopped.state, {"arxiv": adapter}, factory, later)

    assert stopped.state.fetched_through == {"arxiv": recorded}
    assert adapter.windows[1] == FetchWindow(start=recorded, end=later)
    assert adapter.cursors[1] is None
    assert resumed.state.continuations == {}
    assert resumed.state.fetched_through == {"arxiv": later}


@pytest.mark.asyncio
async def test_outage_between_spans_resumes_where_the_walk_stopped() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))

    @dataclass
    class SplitThenFailing(RecordingAdapter):
        async def fetch(
            self,
            window: FetchWindow,
            cursor: str | None,
            client: RequestClient,
            config: AdapterConfig,
        ) -> FetchPage:
            self.windows.append(window)
            if len(self.windows) == 1:
                return FetchPage(records=(), next_cursor=None, capped=True)
            if len(self.windows) == 2:
                return page()
            raise SourceUnavailableError("request retries exhausted")

    adapter = SplitThenFailing("arxiv")
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    stopped_at = adapter.windows[2].start
    assert result.state.fetched_through == {"arxiv": stopped_at}
    assert result.state.continuations == {
        "arxiv": SourceContinuation(
            cursor=None, window_start=stopped_at, window_end=NOW
        )
    }


@pytest.mark.asyncio
async def test_split_span_saved_by_an_earlier_version_walks_on_to_now() -> None:
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    span_end = datetime.combine(
        (NOW - timedelta(days=3)).date(), time.max, tzinfo=timezone.utc
    )
    saved = SourceContinuation(
        cursor="resume", window_start=NOW - timedelta(days=7), window_end=span_end
    )
    adapter = RecordingAdapter("arxiv", pages=[page(), page()])
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(
        config,
        PipelineState(continuations={"arxiv": saved}),
        {"arxiv": adapter},
        factory,
        NOW,
    )

    assert adapter.windows[1] == FetchWindow(
        start=span_end + timedelta(microseconds=1), end=NOW
    )
    assert result.state.continuations == {}
    assert result.state.fetched_through == {"arxiv": NOW}
    assert result.stats[0].complete is True


def test_cursorless_continuation_round_trips_through_the_state_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / ".papers-state.yml"
    state = PipelineState(
        continuations={
            "arxiv": SourceContinuation(
                cursor=None, window_start=NOW - timedelta(days=1), window_end=NOW
            )
        }
    )

    save_state(path, state)

    assert load_state(path).continuations == state.continuations


@pytest.mark.asyncio
async def test_deadline_mid_adapter_keeps_completed_pages_and_resumes() -> None:
    config = pipeline_config(
        adapter_config(
            "semantic_scholar",
            lookback_days=7,
            page_size=1,
            max_pages=10,
            max_results=10,
        )
    )
    published = (NOW - timedelta(days=1)).date().isoformat()
    clock = [0.0]

    def handle(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        # The shared deadline expires while the first page is in flight.
        clock[0] = 1000.0
        body: dict[str, object] = {
            "total": 2,
            "data": [
                {
                    "paperId": f"paper-{offset}",
                    "title": f"Paper {offset}",
                    "publicationDate": published,
                    "openAccessPdf": {"url": f"https://example.test/{offset}.pdf"},
                }
            ]
            if offset < 2
            else [],
        }
        return httpx.Response(200, json=body)

    adapter = SemanticScholarAdapter("key")
    expired = await fetch_all(
        config,
        PipelineState(),
        {adapter.name: adapter},
        lambda _: RequestClient(
            config.fetch,
            Deadline.start(60, clock=lambda: clock[0]),
            transport=httpx.MockTransport(handle),
        ),
        NOW,
    )

    assert [record.source_id for record in expired.records] == ["paper-0"]
    assert adapter.name in expired.state.continuations
    assert expired.state.fetched_through == {}
    assert expired.stats[0].complete is False
    assert any("fetch deadline exceeded" in event for event in expired.events)

    resumed = await fetch_all(
        config,
        expired.state,
        {adapter.name: adapter},
        lambda deadline: RequestClient(
            config.fetch, deadline, transport=httpx.MockTransport(handle)
        ),
        NOW,
    )

    assert [record.source_id for record in resumed.records] == ["paper-1"]
    assert resumed.state.continuations == {}
    assert resumed.state.fetched_through == {adapter.name: NOW}


@pytest.mark.asyncio
async def test_backfill_outage_keeps_completed_history_pages() -> None:
    config = backfill_config(date(2000, 1, 1))
    old = source_record("arxiv", "old", published=NOW - timedelta(days=20))

    @dataclass
    class HistoryThenFailing(RecordingAdapter):
        async def fetch(
            self,
            window: FetchWindow,
            cursor: str | None,
            client: RequestClient,
            config: AdapterConfig,
        ) -> FetchPage:
            self.windows.append(window)
            if len(self.windows) == 1:
                return page()
            if len(self.windows) == 2:
                return page(old, next_cursor="history-2")
            raise SourceUnavailableError("fetch deadline exceeded")

    adapter = HistoryThenFailing("arxiv")
    factory, _, _ = client_factory(config.fetch)

    result = await fetch_all(config, PipelineState(), {"arxiv": adapter}, factory, NOW)

    assert result.records == (old,)
    progress = result.state.backfill["arxiv"]
    assert progress.covered_from == NOW - timedelta(days=7)
    assert progress.continuation == SourceContinuation(
        cursor="history-2",
        window_start=adapter.windows[1].start,
        window_end=adapter.windows[1].end,
    )


@pytest.mark.asyncio
async def test_recent_success_makes_no_state_change_until_it_leaves_the_lookback() -> (
    None
):
    config = pipeline_config(adapter_config("arxiv", lookback_days=7))
    recorded = NOW - timedelta(days=6)
    state = PipelineState(fetched_through={"arxiv": recorded})
    adapter = RecordingAdapter("arxiv", pages=[page(), page()])
    factory, _, _ = client_factory(config.fetch)

    quiet = await fetch_all(config, state, {"arxiv": adapter}, factory, NOW)
    later = NOW + timedelta(days=2)
    moved = await fetch_all(config, quiet.state, {"arxiv": adapter}, factory, later)

    assert quiet.state == state
    assert adapter.windows[1] == FetchWindow(start=recorded, end=later)
    assert moved.state.fetched_through == {"arxiv": later}
