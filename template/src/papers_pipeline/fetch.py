"""Cross-source fetch orchestration with shared deadlines and continuation state."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from papers_pipeline.adapters.base import Adapter, FetchPage, FetchWindow
from papers_pipeline.config import AdapterConfig, PipelineConfig
from papers_pipeline.errors import InfrastructureError, SourceUnavailableError
from papers_pipeline.http import Deadline, RequestClient
from papers_pipeline.models import (
    BackfillProgress,
    PipelineState,
    SourceContinuation,
    SourceRecord,
)


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


@dataclass(frozen=True)
class _BackfillPlan:
    window: FetchWindow
    cursor: str | None
    covered_from: datetime


@dataclass(frozen=True)
class _WindowFetch:
    records: list[SourceRecord]
    cursor: str | None
    capped: bool
    complete: bool
    rejected: int
    error_events: list[str]
    pages: int
    unavailable: SourceUnavailableError | None


@dataclass(frozen=True)
class _ForwardFetch:
    records: list[SourceRecord]
    continuation: SourceContinuation | None
    covered_through: datetime | None
    capped: bool
    complete: bool
    rejected: int
    error_events: list[str]
    unavailable: SourceUnavailableError | None


@dataclass(frozen=True)
class _BackfillFetch:
    records: list[SourceRecord]
    progress: BackfillProgress | None
    capped: bool
    rejected: int
    events: list[str]


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
    backfill = dict(state.backfill)
    fetched_through = dict(state.fetched_through)

    for adapter_index, adapter_config in enumerate(config.adapters):
        if not adapter_config.enabled:
            continue

        adapter = adapters[adapter_config.name]
        continuation = continuations.get(adapter.name)
        lookback_start = now - timedelta(days=adapter_config.lookback_days)
        window = FetchWindow(
            start=(
                continuation.window_start
                if continuation
                else min(
                    lookback_start,
                    fetched_through.get(adapter.name, lookback_start),
                )
            ),
            end=continuation.window_end if continuation else now,
        )
        starting_cursor = continuation.cursor if continuation else None

        async with client_factory(deadline) as client:
            forward = await _fetch_forward(
                adapter, config, adapter_index, window, starting_cursor, client
            )
            history = (
                await _backfill(
                    adapter,
                    config,
                    adapter_index,
                    backfill.get(adapter.name),
                    window.start,
                    client,
                )
                if forward.unavailable is None
                else None
            )
            events.extend(_prefix_events(adapter.name, client.events))
        if forward.unavailable is not None:
            # One source's outage should not stop the others or conversion;
            # the pages it already returned are kept and it resumes next run.
            events.append(
                f"{adapter.name}: source unavailable this run: {forward.unavailable}"
            )
        events.extend(forward.error_events)

        if forward.continuation is not None:
            continuations[adapter.name] = forward.continuation
            if forward.capped:
                events.append(f"{adapter.name}: cap reached; continuation persisted")
        else:
            continuations.pop(adapter.name, None)
            if forward.complete:
                if starting_cursor is not None:
                    events.append(f"{adapter.name}: fetch complete; cursor cleared")
                else:
                    events.append(f"{adapter.name}: fetch complete")
            elif forward.capped:
                events.append(f"{adapter.name}: cap reached")
        if forward.covered_through is not None:
            fetched_through[adapter.name] = forward.covered_through

        source_records = forward.records
        rejected = forward.rejected
        if history is not None:
            if history.progress is not None:
                backfill[adapter.name] = history.progress
            events.extend(history.events)
            source_records = [*source_records, *history.records]
            rejected += history.rejected

        records.extend(source_records)
        stats.append(
            FetchStats(
                source=adapter.name,
                fetched=len(source_records),
                rejected=rejected,
                capped=forward.capped or bool(history and history.capped),
                complete=forward.complete,
            )
        )

    return FetchResult(
        records=tuple(records),
        state=state.model_copy(
            update={
                "continuations": continuations,
                "backfill": backfill,
                "fetched_through": fetched_through,
            }
        ),
        stats=tuple(stats),
        events=tuple(events),
    )


async def _fetch_forward(
    adapter: Adapter,
    config: PipelineConfig,
    adapter_index: int,
    window: FetchWindow,
    cursor: str | None,
    client: RequestClient,
) -> _ForwardFetch:
    """Walk a window oldest first, halving any span too dense to page through.

    Each finished span moves covered_through forward, so a catch-up advances
    even when it needs many runs. Halving spends no page budget, so a run always
    reaches a span it can fetch.
    """
    adapter_config = config.adapters[adapter_index]
    records: list[SourceRecord] = []
    error_events: list[str] = []
    rejected = 0
    pages_left = adapter_config.max_pages
    results_left = adapter_config.max_results
    span = window
    covered_through: datetime | None = None
    truncated = False
    continuation: SourceContinuation | None = None
    unavailable: SourceUnavailableError | None = None
    complete = False
    while True:
        chunk = await _fetch_window(
            adapter,
            config,
            adapter_index,
            span,
            cursor,
            client,
            max_pages=pages_left,
            max_results=results_left,
        )
        records.extend(chunk.records)
        rejected += chunk.rejected
        error_events.extend(chunk.error_events)
        unavailable = chunk.unavailable
        if chunk.cursor is not None:
            continuation = SourceContinuation(
                cursor=chunk.cursor, window_start=span.start, window_end=span.end
            )
            truncated = truncated or chunk.capped
            break
        if unavailable is not None:
            break
        cursor = None
        halves = _split_window(span) if chunk.capped else None
        if halves is not None:
            span = halves[0]
            continue
        # A capped single day cannot be narrowed; the adapter reported it.
        truncated = truncated or chunk.capped
        pages_left -= chunk.pages
        results_left -= len(chunk.records)
        # A split span ends on a day's last instant; the next starts at midnight.
        covered_through = (
            span.end + timedelta(microseconds=1)
            if span.end.time() == time.max
            else span.end
        )
        if span.end >= window.end:
            complete = not truncated
            break
        if pages_left <= 0 or results_left <= 0:
            truncated = True
            break
        span = FetchWindow(start=covered_through, end=window.end)
    return _ForwardFetch(
        records=records,
        continuation=continuation,
        covered_through=covered_through,
        capped=truncated,
        complete=complete,
        rejected=rejected,
        error_events=error_events,
        unavailable=unavailable,
    )


def _split_window(window: FetchWindow) -> tuple[FetchWindow, FetchWindow] | None:
    """Halve a window on a UTC day boundary; a single day cannot be split."""
    days = (window.end.date() - window.start.date()).days
    if days == 0:
        return None
    last_left_day = window.start.date() + timedelta(days=(days - 1) // 2)
    return (
        FetchWindow(
            start=window.start,
            end=datetime.combine(last_left_day, time.max, tzinfo=UTC),
        ),
        FetchWindow(
            start=datetime.combine(
                last_left_day + timedelta(days=1), time.min, tzinfo=UTC
            ),
            end=window.end,
        ),
    )


def _backfill_plan(
    adapter_config: AdapterConfig,
    progress: BackfillProgress | None,
    forward_start: datetime,
) -> _BackfillPlan | None:
    """Pick this run's history window: resume a capped one, or step further back."""
    if adapter_config.backfill_start is None:
        return None
    # History starts where the recurring lookback window ends.
    covered_from = progress.covered_from if progress else forward_start
    if progress and progress.continuation:
        return _BackfillPlan(
            window=FetchWindow(
                start=progress.continuation.window_start,
                end=progress.continuation.window_end,
            ),
            cursor=progress.continuation.cursor,
            covered_from=covered_from,
        )
    limit = datetime.combine(adapter_config.backfill_start, time.min, tzinfo=UTC)
    if covered_from <= limit:
        return None
    return _BackfillPlan(
        window=FetchWindow(
            start=max(
                limit, covered_from - timedelta(days=adapter_config.backfill_days)
            ),
            # A boundary at midnight means that whole day is already covered.
            end=(
                covered_from - timedelta(microseconds=1)
                if covered_from.time() == time.min
                else covered_from
            ),
        ),
        cursor=None,
        covered_from=covered_from,
    )


async def _backfill(
    adapter: Adapter,
    config: PipelineConfig,
    adapter_index: int,
    progress: BackfillProgress | None,
    forward_start: datetime,
    client: RequestClient,
) -> _BackfillFetch | None:
    """Walk history chunk by chunk until this run's page or result budget is spent.

    A chunk that hits the budget keeps its cursor and resumes next run; each
    completed chunk moves the covered boundary back toward backfill_start.
    """
    adapter_config = config.adapters[adapter_index]
    plan = _backfill_plan(adapter_config, progress, forward_start)
    if plan is None:
        return None
    records: list[SourceRecord] = []
    error_events: list[str] = []
    rejected = 0
    pages_left = adapter_config.max_pages
    results_left = adapter_config.max_results
    covered_from = plan.covered_from
    starting_covered_from = covered_from
    continuation: SourceContinuation | None = None
    unavailable: SourceUnavailableError | None = None
    while plan is not None and pages_left > 0 and results_left > 0:
        chunk = await _fetch_window(
            adapter,
            config,
            adapter_index,
            plan.window,
            plan.cursor,
            client,
            max_pages=pages_left,
            max_results=results_left,
        )
        records.extend(chunk.records)
        rejected += chunk.rejected
        error_events.extend(chunk.error_events)
        unavailable = chunk.unavailable
        if chunk.cursor is not None:
            continuation = SourceContinuation(
                cursor=chunk.cursor,
                window_start=plan.window.start,
                window_end=plan.window.end,
            )
            break
        if unavailable is not None:
            break
        halves = _split_window(plan.window) if chunk.capped else None
        if halves is not None:
            # Keep the newer half so the covered boundary stays contiguous.
            plan = _BackfillPlan(
                window=halves[1], cursor=None, covered_from=plan.covered_from
            )
            continue
        pages_left -= chunk.pages
        results_left -= len(chunk.records)
        # A complete chunk, or a capped single day, moves the boundary back.
        covered_from = plan.window.start
        plan = _backfill_plan(
            adapter_config, BackfillProgress(covered_from=covered_from), forward_start
        )

    covered = covered_from.date().isoformat()
    if unavailable is not None:
        summary = f"{adapter.name}: backfill unavailable this run: {unavailable}"
    elif continuation is not None:
        summary = (
            f"{adapter.name}: backfill cap reached at {covered}; continuation persisted"
        )
    elif plan is None:
        summary = f"{adapter.name}: backfill complete to {covered}"
    else:
        summary = f"{adapter.name}: backfilled to {covered}"
    return _BackfillFetch(
        records=records,
        progress=(
            BackfillProgress(covered_from=covered_from, continuation=continuation)
            if continuation is not None or covered_from != starting_covered_from
            else progress
        ),
        capped=continuation is not None and unavailable is None,
        rejected=rejected,
        events=[*error_events, summary],
    )


async def _fetch_window(
    adapter: Adapter,
    config: PipelineConfig,
    adapter_index: int,
    window: FetchWindow,
    cursor: str | None,
    client: RequestClient,
    *,
    max_pages: int,
    max_results: int,
) -> _WindowFetch:
    """Page through one window within the given page and result budget."""
    records: list[SourceRecord] = []
    rejected = 0
    error_events: list[str] = []
    capped = False
    complete = False
    pages_fetched = 0
    unavailable: SourceUnavailableError | None = None
    while pages_fetched < max_pages:
        remaining = max_results - len(records)
        try:
            page = await adapter.fetch(
                window, cursor, client, _page_config(config, adapter_index, remaining)
            )
        except SourceUnavailableError as error:
            # Pages already returned stand; cursor still points at this page.
            unavailable = error
            break
        pages_fetched += 1
        error_events.extend(_page_error_events(adapter.name, page))
        rejected += len(page.permanent_errors)

        if len(page.records) > remaining:
            raise InfrastructureError(
                f"{adapter.name} returned {len(page.records)} records with only "
                f"{remaining} results remaining"
            )
        records.extend(page.records)

        cursor = page.next_cursor
        if cursor is None:
            complete = not page.capped
            capped = page.capped
            break
        if page.capped or len(records) >= max_results or pages_fetched >= max_pages:
            capped = True
            break
    return _WindowFetch(
        records=records,
        cursor=cursor,
        capped=capped,
        complete=complete,
        rejected=rejected,
        error_events=error_events,
        pages=pages_fetched,
        unavailable=unavailable,
    )
