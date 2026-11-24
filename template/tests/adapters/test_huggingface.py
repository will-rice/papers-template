from datetime import datetime, timezone

import httpx
import pytest

from papers_pipeline.adapters.base import FetchWindow
from papers_pipeline.adapters.huggingface import HuggingFaceAdapter
from papers_pipeline.config import AdapterConfig
from papers_pipeline.errors import InfrastructureError
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.config import FetchConfig
from conftest import FixtureTransport
from papers_pipeline.normalize import normalize

from .contract import assert_adapter_contract


@pytest.mark.asyncio
async def test_huggingface_contract(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    await assert_adapter_contract(
        HuggingFaceAdapter(), huggingface_client, huggingface_config
    )


@pytest.mark.asyncio
async def test_huggingface_reads_a_whole_day_and_cursor_names_the_next_day(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    window = FetchWindow(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )
    adapter = HuggingFaceAdapter()

    page = await adapter.fetch(window, None, huggingface_client, huggingface_config)
    repeat = await adapter.fetch(window, None, huggingface_client, huggingface_config)

    assert tuple(record.source_id for record in page.records) == (
        "2401.00001",
        "2401.00003",
        "2401.00002",
    )
    assert page.next_cursor
    assert page.next_cursor != "1"
    assert page.next_cursor == repeat.next_cursor
    assert page.capped is False
    assert page.permanent_errors == ()


@pytest.mark.asyncio
async def test_huggingface_max_results_sets_capped_without_dropping_cursor(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    page = await HuggingFaceAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_client,
        config=huggingface_config.model_copy(
            update={"max_pages": 10, "max_results": 1}
        ),
    )

    assert tuple(record.source_id for record in page.records) == ("2401.00001",)
    assert page.next_cursor
    assert page.capped is True


@pytest.mark.asyncio
async def test_huggingface_record_fields_normalize_correctly(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    page = await HuggingFaceAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_client,
        config=huggingface_config,
    )

    record = page.records[0]
    paper = normalize(record)

    assert record.source == "huggingface"
    assert record.source_id == "2401.00001"
    assert record.arxiv_id == "2401.00001"
    assert record.authors == ("A. Author",)
    assert record.categories == ()
    assert record.url == "https://huggingface.co/papers/2401.00001"
    assert record.input_url == "https://arxiv.org/pdf/2401.00001"
    assert paper.identifier == "arxiv:2401.00001"


@pytest.mark.asyncio
async def test_huggingface_keeps_valid_records_and_surfaces_malformed_entries(
    huggingface_malformed_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    page = await HuggingFaceAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_malformed_client,
        config=huggingface_config,
    )

    assert tuple(record.source_id for record in page.records) == ("2401.00001",)
    assert page.permanent_errors == (
        "huggingface record missing publishedAt: 2401.99999",
    )


@pytest.mark.asyncio
async def test_huggingface_keeps_papers_listed_long_after_publication(
    huggingface_out_of_window_only_client: RequestClient,
    huggingface_config: AdapterConfig,
) -> None:
    one_per_page = huggingface_config.model_copy(update={"page_size": 1})
    page = await HuggingFaceAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_out_of_window_only_client,
        config=one_per_page,
    )

    assert tuple(record.source_id for record in page.records) == (
        "2312.12345",
        "2401.00002",
    )
    assert page.next_cursor
    assert page.capped is False
    assert page.permanent_errors == ()


@pytest.mark.asyncio
async def test_huggingface_malformed_only_page_counts_toward_max_results(
    huggingface_malformed_only_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    page = await HuggingFaceAdapter().fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_malformed_only_client,
        config=huggingface_config.model_copy(
            update={"max_pages": 10, "max_results": 1}
        ),
    )

    assert page.records == ()
    assert page.next_cursor
    assert page.capped is True
    assert page.permanent_errors == (
        "huggingface record missing publishedAt: 2401.99998",
    )


@pytest.mark.asyncio
async def test_huggingface_invalid_json_page_is_infrastructure_failure(
    huggingface_invalid_json_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    with pytest.raises(InfrastructureError, match="invalid JSON"):
        await HuggingFaceAdapter().fetch(
            window=FetchWindow(
                start=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end=datetime(2024, 1, 8, tzinfo=timezone.utc),
            ),
            cursor=None,
            client=huggingface_invalid_json_client,
            config=huggingface_config,
        )


@pytest.mark.asyncio
async def test_huggingface_enumerates_each_utc_date_and_resumes_mid_range(
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> None:
    transport = fixture_transport(
        {
            "https://huggingface.co/api/daily_papers?date=2024-01-07&p=0&limit=2": {
                "fixture": "adapters/huggingface/day-7.json"
            },
            "https://huggingface.co/api/daily_papers?date=2024-01-07&p=1&limit=2": {
                "fixture": "adapters/huggingface/empty.json"
            },
            "https://huggingface.co/api/daily_papers?date=2024-01-06&p=0&limit=2": {
                "fixture": "adapters/huggingface/day-6.json"
            },
            "https://huggingface.co/api/daily_papers?date=2024-01-06&p=1&limit=2": {
                "fixture": "adapters/huggingface/empty.json"
            },
        }
    )
    client = RequestClient(
        fetch_config,
        Deadline.start(fetch_config.total_deadline_seconds),
        transport=transport,
    )
    adapter = HuggingFaceAdapter()
    window = FetchWindow(
        start=datetime(2024, 1, 6, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )

    # The first day requested is the one before the window ends; the API
    # rejects days it has not published yet.
    page = await adapter.fetch(window, None, client, huggingface_config)
    assert tuple(record.source_id for record in page.records) == ("2401.00007",)

    page = await adapter.fetch(window, page.next_cursor, client, huggingface_config)
    assert tuple(record.source_id for record in page.records) == ("2401.00006",)
    assert page.next_cursor is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "saved_cursor",
    [
        # Saved by an unfinished backfill: {"date":"2025-06-11","page":1,...}.
        "eyJkYXRlIjoiMjAyNS0wNi0xMSIsInBhZ2UiOjEsInNvdXJjZV9jb25zdW1lZCI6Mjc3OH0",
        # {"date":"2025-06-11","offset":3,"source_consumed":3}.
        "eyJkYXRlIjoiMjAyNS0wNi0xMSIsIm9mZnNldCI6Mywic291cmNlX2NvbnN1bWVkIjozfQ",
    ],
)
async def test_huggingface_saved_cursor_rereads_its_day(
    saved_cursor: str,
    fixture_transport: FixtureTransport,
    fetch_config: FetchConfig,
    huggingface_config: AdapterConfig,
) -> None:
    transport = fixture_transport(
        {
            "https://huggingface.co/api/daily_papers?date=2025-06-11&p=0&limit=2": {
                "fixture": "adapters/huggingface/page.json"
            },
            "https://huggingface.co/api/daily_papers?date=2025-06-11&p=1&limit=2": {
                "fixture": "adapters/huggingface/empty.json"
            },
        }
    )
    client = RequestClient(
        fetch_config,
        Deadline.start(fetch_config.total_deadline_seconds),
        transport=transport,
    )
    window = FetchWindow(
        start=datetime(2024, 11, 8, tzinfo=timezone.utc),
        end=datetime(2025, 11, 8, tzinfo=timezone.utc),
    )

    page = await HuggingFaceAdapter().fetch(
        window, saved_cursor, client, huggingface_config
    )

    assert tuple(record.source_id for record in page.records) == (
        "2401.00001",
        "2401.00003",
    )


def listed_paper(identifier: str) -> dict[str, object]:
    return {
        "paper": {
            "id": identifier,
            "title": f"Paper {identifier}",
            "summary": "Abstract.",
            "publishedAt": "2024-01-07T00:00:00.000Z",
            "authors": [{"name": "A. Author"}],
        }
    }


def replay_client(
    responses: dict[tuple[str, str, str], list[dict[str, object]]],
    fetch_config: FetchConfig,
) -> RequestClient:
    def replay(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        key = (params["date"], params["p"], params["limit"])
        return httpx.Response(200, json=responses[key], request=request)

    return RequestClient(
        fetch_config,
        Deadline.start(fetch_config.total_deadline_seconds),
        transport=httpx.MockTransport(replay),
    )


@pytest.mark.asyncio
async def test_huggingface_day_reordered_between_runs_loses_no_paper(
    fetch_config: FetchConfig, huggingface_config: AdapterConfig
) -> None:
    first, second, climber = (
        listed_paper("2401.00001"),
        listed_paper("2401.00002"),
        listed_paper("2401.00003"),
    )
    window = FetchWindow(
        start=datetime(2024, 1, 7, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )
    adapter = HuggingFaceAdapter()
    stopped = await adapter.fetch(
        window,
        None,
        replay_client({("2024-01-07", "0", "2"): [first, second]}, fetch_config),
        huggingface_config.model_copy(update={"max_results": 2}),
    )
    # The unread paper has climbed above the two already read.
    reordered = replay_client(
        {
            ("2024-01-07", "0", "2"): [climber, first],
            ("2024-01-07", "1", "2"): [second],
            ("2024-01-07", "2", "2"): [],
        },
        fetch_config,
    )

    seen = [record.source_id for record in stopped.records]
    cursor = stopped.next_cursor
    while cursor is not None:
        page = await adapter.fetch(window, cursor, reordered, huggingface_config)
        seen.extend(record.source_id for record in page.records)
        cursor = page.next_cursor

    assert set(seen) == {"2401.00001", "2401.00002", "2401.00003"}


@pytest.mark.asyncio
async def test_huggingface_day_over_the_budget_is_reported_and_passed(
    fetch_config: FetchConfig, huggingface_config: AdapterConfig
) -> None:
    client = replay_client(
        {
            ("2024-01-07", "0", "2"): [
                listed_paper("2401.00001"),
                listed_paper("2401.00002"),
            ],
            ("2024-01-06", "0", "2"): [listed_paper("2401.00006")],
            ("2024-01-06", "1", "2"): [],
        },
        fetch_config,
    )
    window = FetchWindow(
        start=datetime(2024, 1, 6, tzinfo=timezone.utc),
        end=datetime(2024, 1, 8, tzinfo=timezone.utc),
    )
    adapter = HuggingFaceAdapter()
    budget = huggingface_config.model_copy(update={"max_results": 2})

    stopped = await adapter.fetch(window, None, client, budget)
    retried = await adapter.fetch(window, stopped.next_cursor, client, budget)
    older_day = await adapter.fetch(window, retried.next_cursor, client, budget)

    assert stopped.capped is True
    assert stopped.permanent_errors == ()
    assert retried.capped is False
    assert retried.permanent_errors == (
        "huggingface day 2024-01-07 reached the budget of 2 papers; "
        "any papers past it were not fetched",
    )
    assert tuple(record.source_id for record in older_day.records) == ("2401.00006",)
    assert older_day.next_cursor is None
