"""Cross-source fetch orchestration with shared deadlines and continuation state."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

from papers_pipeline.adapters.base import Adapter, FetchPage, FetchWindow
from papers_pipeline.config import AdapterConfig, PipelineConfig
from papers_pipeline.errors import InfrastructureError
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.models import PipelineState, SourceContinuation, SourceRecord


@dataclass(frozen=True)
class FetchStats:
    source: str
    fetched: int
    rejected: int
    capped: bool
    complete: bool


@dataclass(frozen=True)
class FetchResult:
    records: tuple[SourceRecord, ...]
    state: PipelineState
    stats: tuple[FetchStats, ...]
    events: tuple[str, ...]


def _prefix_events(source: str, events: list[str]) -> tuple[str, ...]:
    return tuple(f"{source}: {event}" for event in events)


def _page_error_events(source: str, page: FetchPage) -> tuple[str, ...]:
    return tuple(
        f"{source}: permanent error: {message}" for message in page.permanent_errors
    )


def _page_config(
    config: PipelineConfig, adapter_index: int, remaining: int
) -> AdapterConfig:
    adapter_config = config.adapters[adapter_index]
    return type(adapter_config).model_validate(
        {
            **adapter_config.model_dump(),
            "page_size": min(adapter_config.page_size, remaining),
            "max_results": remaining,
        }
    )


async def fetch_all(
    config: PipelineConfig,
    state: PipelineState,
    adapters: Mapping[str, Adapter],
    client_factory: Callable[[Deadline], RequestClient],
    now: datetime,
) -> FetchResult:
    deadline = Deadline.start(config.fetch.total_deadline_seconds)
    records: list[SourceRecord] = []
    stats: list[FetchStats] = []
    events: list[str] = []
    continuations = dict(state.continuations)

    for adapter_index, adapter_config in enumerate(config.adapters):
        if not adapter_config.enabled:
            continue

        adapter = adapters[adapter_config.name]
        continuation = continuations.get(adapter.name)
        window = FetchWindow(
            start=(
                continuation.window_start
                if continuation
                else now - timedelta(days=adapter_config.lookback_days)
            ),
            end=continuation.window_end if continuation else now,
        )
        cursor = continuation.cursor if continuation else None
        starting_cursor = cursor
        source_records: list[SourceRecord] = []
        source_errors = 0
        source_error_events: list[str] = []
        capped = False
        complete = False
        pages_fetched = 0

        async with client_factory(deadline) as client:
            while pages_fetched < adapter_config.max_pages:
                remaining = adapter_config.max_results - len(source_records)
                page = await adapter.fetch(
                    window,
                    cursor,
                    client,
                    _page_config(config, adapter_index, remaining),
                )
                pages_fetched += 1
                source_error_events.extend(_page_error_events(adapter.name, page))
                source_errors += len(page.permanent_errors)

                if len(page.records) > remaining:
                    raise InfrastructureError(
                        f"{adapter.name} returned {len(page.records)} records with only "
                        f"{remaining} results remaining"
                    )
                source_records.extend(page.records)

                cursor = page.next_cursor
                if cursor is None:
                    complete = not page.capped
                    capped = page.capped
                    break

                if page.capped:
                    capped = True
                    break

                if len(source_records) >= adapter_config.max_results:
                    capped = True
                    break

                if pages_fetched >= adapter_config.max_pages:
                    capped = True
                    break

            events.extend(_prefix_events(adapter.name, client.events))
        events.extend(source_error_events)

        if cursor is not None:
            continuations[adapter.name] = SourceContinuation(
                cursor=cursor,
                window_start=window.start,
                window_end=window.end,
            )
            if capped:
                events.append(f"{adapter.name}: cap reached; continuation persisted")
        elif complete:
            continuations.pop(adapter.name, None)
            if starting_cursor is not None:
                events.append(f"{adapter.name}: fetch complete; cursor cleared")
            else:
                events.append(f"{adapter.name}: fetch complete")
        elif capped:
            continuations.pop(adapter.name, None)
            events.append(f"{adapter.name}: cap reached")

        records.extend(source_records)
        stats.append(
            FetchStats(
                source=adapter.name,
                fetched=len(source_records),
                rejected=source_errors,
                capped=capped,
                complete=complete,
            )
        )

    return FetchResult(
        records=tuple(records),
        state=state.model_copy(update={"continuations": continuations}),
        stats=tuple(stats),
        events=tuple(events),
    )
