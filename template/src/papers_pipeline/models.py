from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

InputFormat = Literal["html", "latex", "pdf"]


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceRecord(FrozenModel):
    source: str
    source_id: str
    title: str
    abstract: str
    authors: tuple[str, ...]
    published: datetime
    url: str
    input_format: InputFormat
    input_url: str
    categories: tuple[str, ...] = Field(default_factory=tuple)
    doi: str | None = None
    arxiv_id: str | None = None


class Paper(FrozenModel):
    identifier: str
    title: str
    abstract: str
    authors: tuple[str, ...]
    published: datetime
    url: str
    source: str
    input_format: InputFormat
    input_url: str
    categories: tuple[str, ...] = Field(default_factory=tuple)
    doi: str | None = None
    arxiv_id: str | None = None


class FailureAttempt(FrozenModel):
    occurred_at: datetime
    error: str


class PipelineState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cursors: dict[str, str] = Field(default_factory=dict)
    failures: dict[str, list[FailureAttempt]] = Field(default_factory=dict)
