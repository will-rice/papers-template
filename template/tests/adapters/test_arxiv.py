from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from papers_pipeline.adapters.arxiv import ArxivAdapter
from papers_pipeline.adapters.base import FetchWindow
from papers_pipeline.config import AdapterConfig, FetchConfig
from papers_pipeline.errors import InfrastructureError, SourceUnavailableError
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.normalize import normalize

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
