from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from papers_pipeline.cli import app
from papers_pipeline.config import ConfigError, PipelineConfig, load_config

VALID_CONFIG = """\
repository:
  name: Example Papers
  slug: example-papers
  description: Example topic
adapters:
  - name: arxiv
    enabled: true
    secret_env: null
    lookback_days: 7
    page_size: 100
    max_pages: 5
    max_results: 500
    filters: {}
topic:
  include_any: ["Example topic"]
  include_all: []
  exclude_any: []
  categories: []
  plugin: null
fetch:
  request_timeout_seconds: 30
  retries: 3
  backoff_seconds: 1
  total_deadline_seconds: 900
conversion:
  max_batches_per_run: 4
  max_papers: 10
  max_cost: 100
  html_cost: 2
  latex_cost: 4
  pdf_cost: 20
concurrency:
  html: 4
  latex: 2
  pdf: 1
"""


@pytest.fixture
def valid_config() -> Path:
    fixtures_dir = Path("tests/.task-2")
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    config_path = fixtures_dir / f"{uuid4()}.yml"
    config_path.write_text(VALID_CONFIG)
    try:
        yield config_path
    finally:
        config_path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("pdf: 2", "concurrency.pdf must equal 1"),
        ("max_papers: 0", "conversion.max_papers must be between 1 and 100"),
        (
            "total_deadline_seconds: 30",
            "fetch.total_deadline_seconds must be between 60 and 7200",
        ),
    ],
)
def test_unsafe_limits_are_rejected(
    valid_config: Path, replacement: str, message: str
) -> None:
    text = valid_config.read_text().replace("pdf: 1", replacement)
    if replacement.startswith("max_papers"):
        text = text.replace("max_papers: 10", replacement)
    if replacement.startswith("total_deadline"):
        text = text.replace("total_deadline_seconds: 900", replacement)
    valid_config.write_text(text)

    with pytest.raises(ConfigError, match=message):
        load_config(valid_config, {})


def test_enabled_adapter_requires_declared_secret(valid_config: Path) -> None:
    text = valid_config.read_text().replace("secret_env: null", "secret_env: S2_KEY", 1)
    valid_config.write_text(text)

    with pytest.raises(ConfigError, match="missing secret S2_KEY"):
        load_config(valid_config, {})


def test_checked_in_schema_matches_model() -> None:
    expected = json.dumps(
        PipelineConfig.model_json_schema(),
        indent=2,
        sort_keys=True,
    ) + "\n"

    assert Path("papers.schema.json").read_text() == expected


def test_validate_command_accepts_valid_config(valid_config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = app(["validate", "--config", str(valid_config)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == f"valid: {valid_config}\n"
