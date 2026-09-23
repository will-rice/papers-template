"""Configuration schema and preflight validation for the papers pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from papers_pipeline.errors import ConfigError

AdapterName = Literal[
    "arxiv",
    "huggingface",
    "semantic_scholar",
    "dblp",
    "biorxiv_crossref",
    "papers_with_code",
]

_FIELD_MESSAGES: dict[tuple[str, ...], str] = {
    ("concurrency", "pdf"): "concurrency.pdf must equal 1",
    ("conversion", "max_papers"): "conversion.max_papers must be between 1 and 100",
    (
        "fetch",
        "total_deadline_seconds",
    ): "fetch.total_deadline_seconds must be between 60 and 7200",
}


class StrictModel(BaseModel):
    """Base model that rejects unknown fields."""

    model_config = ConfigDict(extra="forbid")


class AdapterConfig(StrictModel):
    """Configuration for one paper source adapter."""

    name: AdapterName
    enabled: bool = True
    secret_env: str | None = None
    lookback_days: int = Field(ge=1, le=365)
    page_size: int = Field(ge=1, le=1000)
    max_pages: int = Field(ge=1, le=100)
    max_results: int = Field(ge=1, le=10000)
    filters: dict[str, str | list[str]] = Field(default_factory=dict)


class TopicConfig(StrictModel):
    """Keyword and plugin selection for paper discovery."""

    include_any: list[str] = Field(default_factory=list)
    include_all: list[str] = Field(default_factory=list)
    exclude_any: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    plugin: str | None = None


class FetchConfig(StrictModel):
    """HTTP retry and deadline limits."""

    request_timeout_seconds: float = Field(ge=1, le=120)
    retries: int = Field(ge=0, le=5)
    backoff_seconds: float = Field(ge=0, le=30)
    total_deadline_seconds: int = Field(ge=60, le=7200)


class ConversionConfig(StrictModel):
    """Conversion budget and batching limits."""

    max_batches_per_run: int = Field(ge=1, le=20)
    max_papers: int = Field(ge=1, le=100)
    max_cost: int = Field(ge=1, le=1000)
    html_cost: int = Field(ge=1, le=100)
    latex_cost: int = Field(ge=1, le=100)
    pdf_cost: int = Field(ge=1, le=1000)


class ConcurrencyConfig(StrictModel):
    """Concurrency caps for each conversion type."""

    html: int = Field(ge=1, le=4)
    latex: int = Field(ge=1, le=4)
    pdf: Literal[1]


class RepositoryConfig(StrictModel):
    """Repository metadata copied into the rendered template."""

    name: str = Field(min_length=1)
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*-papers$")
    description: str = Field(min_length=1)


class PipelineConfig(StrictModel):
    """Top-level rendered configuration."""

    repository: RepositoryConfig
    adapters: list[AdapterConfig]
    topic: TopicConfig
    fetch: FetchConfig
    conversion: ConversionConfig
    concurrency: ConcurrencyConfig

    @model_validator(mode="after")
    def unique_adapters(self) -> "PipelineConfig":
        names = [adapter.name for adapter in self.adapters]
        if len(names) != len(set(names)):
            raise ValueError("adapters must have unique names")
        return self


def _format_validation_error(error: ValidationError) -> str:
    first_issue = error.errors()[0]
    location = tuple(str(part) for part in first_issue["loc"])
    if location in _FIELD_MESSAGES:
        return _FIELD_MESSAGES[location]
    path = ".".join(location) or "config"
    message = first_issue["msg"]
    return f"{path}: {message}"


def _read_config(path: Path) -> Mapping[str, Any]:
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ConfigError("config: expected a mapping at the top level")
    return data


def load_config(path: Path, environ: Mapping[str, str]) -> PipelineConfig:
    """Load and validate a rendered pipeline configuration file."""

    try:
        raw_config = _read_config(path)
        config = PipelineConfig.model_validate(raw_config)
    except OSError as error:
        raise ConfigError(str(error)) from error
    except yaml.YAMLError as error:
        raise ConfigError(str(error)) from error
    except ValidationError as error:
        raise ConfigError(_format_validation_error(error)) from error

    for adapter in config.adapters:
        if adapter.enabled and adapter.secret_env and not environ.get(adapter.secret_env):
            raise ConfigError(f"{adapter.name}: missing secret {adapter.secret_env}")
    return config
