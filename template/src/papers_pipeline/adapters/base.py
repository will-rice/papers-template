from __future__ import annotations

from datetime import datetime
from collections.abc import Callable, Iterable
from typing import Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from papers_pipeline.config import AdapterConfig
from papers_pipeline.errors import PaperError
from papers_pipeline.http import RequestClient
from papers_pipeline.models import SourceRecord

RawRecord = TypeVar("RawRecord")


class FetchWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    start: datetime
    end: datetime


class FetchPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    records: tuple[SourceRecord, ...] = Field(default_factory=tuple)
    next_cursor: str | None
    capped: bool
    permanent_errors: tuple[str, ...] = Field(default_factory=tuple)


@runtime_checkable
class Adapter(Protocol):
    name: str
    record_sources: frozenset[str]
    window_type: type[FetchWindow]

    async def fetch(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
    ) -> FetchPage:
        raise NotImplementedError


def collect_records(
    items: Iterable[RawRecord],
    parser: Callable[[RawRecord], SourceRecord],
) -> tuple[tuple[SourceRecord, ...], tuple[str, ...]]:
    records: list[SourceRecord] = []
    permanent_errors: list[str] = []

    for item in items:
        try:
            records.append(parser(item))
        except PaperError as error:
            permanent_errors.append(str(error))

    return tuple(records), tuple(permanent_errors)
