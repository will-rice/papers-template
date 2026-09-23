from __future__ import annotations

from datetime import datetime, timezone

import pytest

from papers_pipeline.adapters.base import FetchWindow
from papers_pipeline.adapters.huggingface import HuggingFaceAdapter
from papers_pipeline.config import AdapterConfig
from papers_pipeline.errors import InfrastructureError
from papers_pipeline.http import RequestClient
from papers_pipeline.normalize import normalize

from .contract import assert_adapter_contract


@pytest.mark.asyncio
async def test_huggingface_contract(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    await assert_adapter_contract(HuggingFaceAdapter(), huggingface_client, huggingface_config)


@pytest.mark.asyncio
async def test_huggingface_continuation_cursor_is_deterministic_and_opaque(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    adapter = HuggingFaceAdapter()
    first_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_client,
        config=huggingface_config,
    )

    page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=first_page.next_cursor,
        client=huggingface_client,
        config=huggingface_config,
    )

    assert first_page.next_cursor
    assert first_page.next_cursor != "1"
    assert tuple(record.source_id for record in page.records) == ("2401.00002",)
    assert page.next_cursor
    assert page.next_cursor != first_page.next_cursor
    assert page.capped is False
    assert page.permanent_errors == ()


@pytest.mark.asyncio
async def test_huggingface_max_pages_sets_capped_without_dropping_cursor(
    huggingface_client: RequestClient, huggingface_config: AdapterConfig
) -> None:
    adapter = HuggingFaceAdapter()
    first_page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=None,
        client=huggingface_client,
        config=huggingface_config,
    )

    page = await adapter.fetch(
        window=FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        cursor=first_page.next_cursor,
        client=huggingface_client,
        config=huggingface_config.model_copy(update={"max_pages": 2, "max_results": 10}),
    )

    assert page.next_cursor
    assert page.capped is True


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
        config=huggingface_config.model_copy(update={"max_pages": 10, "max_results": 1}),
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
    assert record.input_url == "https://arxiv.org/pdf/2401.00001.pdf"
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
