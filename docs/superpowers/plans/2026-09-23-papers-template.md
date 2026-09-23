# Papers Template Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a complete, independently testable Copier template for standalone topic-focused papers repositories with bounded discovery, conversion, formatting, state, and update automation.

**Architecture:** The repository is a Copier source whose rendered project contains a Python 3.12 `papers_pipeline` package, repository-owned YAML/CSV state, offline fixtures, and pinned GitHub Actions workflows. Source adapters feed canonical records through shared normalization, topic filtering, deduplication, inventory, deterministic batching, conversion, formatting, and reporting boundaries; network clients and subprocess runners are injected so the full default suite stays offline.

**Tech Stack:** Copier 9, Python 3.12, uv, Pydantic 2, PyYAML 6, HTTPX 0.28, pytest 8, pytest-asyncio, Ruff, mypy, pre-commit, Pandoc, Marker/PyTorch, GitHub Actions

## Global Constraints

- Every generated repository is standalone and contains pipeline code, tests, workflows, `papers.yml`, `.papers-state.yml`, and `.copier-answers.yml`.
- Copier owns pipeline code, tests, workflows, and template-supplied support files; it must not own or overwrite `papers/`, `papers.csv`, `.papers-state.yml`, or download, parser, model, and tool caches.
- `papers.yml` is the single repository-level configuration surface and must be schema-validated before any network request or conversion.
- Configuration errors must be concise and actionable for unknown or invalid options, missing required secrets, unsupported option combinations, and unsafe resource limits.
- Topic behavior is declarative by default; the only custom extension is one optional topic-filter plugin that accepts a normalized paper and returns a topic decision.
- Supported adapters are arXiv, Hugging Face Papers, Semantic Scholar, DBLP, bioRxiv/Crossref, and Papers With Code.
- Every adapter accepts validated configuration, an incremental window, pagination limits, and request policy; yields records plus continuation metadata; and distinguishes retryable, permanent record/source, and infrastructure failures.
- Fetching must enforce source-specific incremental lookbacks, pagination caps, per-request timeouts, bounded retries with backoff, and one total deadline across all adapters.
- Reaching a fetch cap must persist deterministic continuation state and must never be reported as complete enumeration.
- Normalization, source precedence, deduplication, topic filtering, inventory output, backlog ordering, and batch selection must be deterministic.
- `papers.csv` is the paper inventory, not a mutable queue; backlog is always inferred from CSV rows versus generated files and `.fixme.txt` markers.
- `.papers-state.yml` contains only adapter continuation data and consecutive per-paper failure counters; it is never a backlog or source of paper truth.
- Every conversion batch is bounded by both maximum paper count and deterministic estimated-cost budget; neither limit may be unlimited.
- HTML and LaTeX conversion have modest, independently bounded parallelism; Marker/PyTorch PDF conversion has exactly one worker for the entire job.
- Paper-specific failures are isolated; the third consecutive scheduled failure creates a colocated `.fixme.txt` and removes transient failure state.
- Infrastructure and resource failures fail explicitly and must never become empty success or per-paper failure.
- Nightly formatting receives only files generated or changed in the run plus changed indexes; whole-corpus formatting exists only as a manual, deterministic, bounded sharded workflow.
- Inventory/state changes are committed before conversion; each consistent conversion batch is committed independently; inconsistent infrastructure-failed state is never committed as success.
- GitHub Actions summaries include source counts, inventory/conversion/fixme counts, stage timings, cap/retry/deadline/continuation events, failure classes, and `.fixme.txt` paths.
- Template updates use an explicit Copier release, validate the generated result, and open a pull request; they never push template updates directly to `main`.
- All GitHub Actions are pinned to immutable commit SHAs, jobs and material steps have explicit timeouts, permissions are least privilege, and overlapping scheduled mutation runs are prevented.
- The default test suite is offline and deterministic, using recorded adapter fixtures and tiny HTML, LaTeX, and PDF fixtures.
- The template smoke test generates a sample repository and runs pre-commit plus the complete offline suite inside that rendered repository.
- Migration of `will-rice/lipsync-papers`, `will-rice/tts-papers`, `will-rice/asr-papers`, and `will-rice/birdclef-papers` is excluded from this plan. Each migration requires a separate follow-on plan and may start only after the template smoke test passes.

---

## File Structure

```text
.
├── copier.yml
├── pyproject.toml
├── uv.lock
├── .pre-commit-config.yaml
├── README.md
├── scripts/
│   └── smoke-test.sh
├── tests/
│   ├── test_copier_smoke.py
│   └── test_template_exclusions.py
└── template/
    ├── README.md.jinja
    ├── pyproject.toml.jinja
    ├── uv.lock
    ├── .gitignore
    ├── .pre-commit-config.yaml
    ├── papers.yml.jinja
    ├── .papers-state.yml
    ├── papers.csv
    ├── topic_plugin.py.jinja
    ├── src/papers_pipeline/
    │   ├── __init__.py
    │   ├── cli.py
    │   ├── config.py
    │   ├── errors.py
    │   ├── models.py
    │   ├── state.py
    │   ├── inventory.py
    │   ├── normalize.py
    │   ├── topics.py
    │   ├── http.py
    │   ├── fetch.py
    │   ├── batching.py
    │   ├── convert.py
    │   ├── formatting.py
    │   ├── indexing.py
    │   ├── summary.py
    │   ├── git.py
    │   ├── pipeline.py
    │   └── adapters/
    │       ├── __init__.py
    │       ├── base.py
    │       ├── arxiv.py
    │       ├── huggingface.py
    │       ├── semantic_scholar.py
    │       ├── dblp.py
    │       ├── biorxiv_crossref.py
    │       └── papers_with_code.py
    ├── tests/
    │   ├── conftest.py
    │   ├── adapters/
    │   │   ├── contract.py
    │   │   ├── test_arxiv.py
    │   │   ├── test_huggingface.py
    │   │   ├── test_semantic_scholar.py
    │   │   ├── test_dblp.py
    │   │   ├── test_biorxiv_crossref.py
    │   │   └── test_papers_with_code.py
    │   ├── fixtures/adapters/{arxiv,huggingface,semantic_scholar,dblp,biorxiv_crossref,papers_with_code}/
    │   ├── fixtures/conversion/sample.html
    │   ├── fixtures/conversion/sample.tex
    │   ├── fixtures/conversion/sample.pdf
    │   ├── test_config.py
    │   ├── test_state_inventory.py
    │   ├── test_normalize_topics.py
    │   ├── test_http.py
    │   ├── test_fetch.py
    │   ├── test_batching.py
    │   ├── test_convert.py
    │   ├── test_formatting.py
    │   ├── test_summary.py
    │   ├── test_workflows.py
    │   └── test_pipeline.py
    └── .github/
        ├── workflows/
        │   ├── ci.yml
        │   ├── nightly.yml
        │   ├── format-corpus.yml
        │   └── template-update.yml
        └── scripts/
            ├── nightly.sh
            └── template-update.sh
```

The source repository tests only Copier rendering and ownership exclusions.
Behavioral tests live in the rendered template because generated repositories
must remain independently testable.

### Task 1: Copier Skeleton and Rendered Package

**Files:**
- Create: `copier.yml`
- Create: `pyproject.toml`
- Create: `.pre-commit-config.yaml`
- Create: `template/pyproject.toml.jinja`
- Create: `template/src/papers_pipeline/__init__.py`
- Create: `template/.gitignore`
- Test: `tests/test_copier_smoke.py`

**Interfaces:**
- Consumes: no application interfaces
- Produces: Copier answers `project_name: str`, `project_slug: str`, `topic_description: str`, `template_version: str`; console command `papers-pipeline = papers_pipeline.cli:app`

- [ ] **Step 1: Write the failing render test**

```python
# tests/test_copier_smoke.py
from pathlib import Path
from copier import run_copy


def test_template_renders_python_package(tmp_path: Path) -> None:
    destination = tmp_path / "sample-papers"
    run_copy(
        ".",
        destination,
        data={
            "project_name": "Sample Papers",
            "project_slug": "sample-papers",
            "topic_description": "sample topic",
            "template_version": "0.1.0",
        },
        defaults=True,
        unsafe=True,
    )
    assert (destination / "pyproject.toml").is_file()
    assert (destination / "src/papers_pipeline/__init__.py").is_file()
    answers = (destination / ".copier-answers.yml").read_text()
    assert "_src_path:" in answers
    assert "template_version: 0.1.0" in answers
```

- [ ] **Step 2: Run the test and verify the missing template fails**

Run: `uv run pytest tests/test_copier_smoke.py::test_template_renders_python_package -v`

Expected: FAIL because `copier.yml` does not exist.

- [ ] **Step 3: Create the Copier metadata and rendered package metadata**

```yaml
# copier.yml
_subdirectory: template
_answers_file: .copier-answers.yml
_skip_if_exists:
  - papers
  - papers.csv
  - .papers-state.yml
  - .cache
  - .pytest_cache
  - .mypy_cache
  - .ruff_cache
project_name:
  type: str
  help: Human-readable repository name
project_slug:
  type: str
  help: GitHub repository slug
  validator: "{% if not (project_slug | regex_search('^[a-z0-9][a-z0-9-]*-papers$')) %}Use a lowercase *-papers slug{% endif %}"
topic_description:
  type: str
  help: One-line topic description
template_version:
  type: str
  default: "0.1.0"
```

```toml
# template/pyproject.toml.jinja
[project]
name = "{{ project_slug }}"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [
  "httpx>=0.28,<0.29",
  "pydantic>=2.11,<3",
  "PyYAML>=6.0,<7",
]

[project.optional-dependencies]
dev = [
  "mypy>=1.17,<2",
  "pre-commit>=4.3,<5",
  "pytest>=8.4,<9",
  "pytest-asyncio>=1.1,<2",
  "ruff>=0.12,<0.13",
]

[project.scripts]
papers-pipeline = "papers_pipeline.cli:app"

[tool.pytest.ini_options]
addopts = "-q"
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
line-length = 88
target-version = "py312"

[tool.mypy]
python_version = "3.12"
strict = true
packages = ["papers_pipeline"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

```python
# template/src/papers_pipeline/__init__.py
"""Bounded paper discovery and conversion pipeline."""

__version__ = "0.1.0"
```

```gitignore
# template/.gitignore
.venv/
.cache/
.pytest_cache/
.mypy_cache/
.ruff_cache/
__pycache__/
```

- [ ] **Step 4: Add source-repository tooling and lock both environments**

```toml
# pyproject.toml
[project]
name = "papers-template"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = ["copier>=9.10,<10", "pytest>=8.4,<9"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

Run: `uv lock && (cd template && uv lock)`

Expected: root `uv.lock` and `template/uv.lock` are created without resolution errors.

- [ ] **Step 5: Add pre-commit configuration**

```yaml
# .pre-commit-config.yaml and template/.pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.12.12
    hooks:
      - id: ruff-check
        args: [--fix]
      - id: ruff-format
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.17.1
    hooks:
      - id: mypy
        additional_dependencies:
          - pydantic==2.11.7
          - types-PyYAML==6.0.12.20250822
```

- [ ] **Step 6: Verify rendering**

Run: `uv run pytest tests/test_copier_smoke.py -v`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add copier.yml pyproject.toml uv.lock .pre-commit-config.yaml template tests
git commit -m "feat: scaffold Copier papers template"
```

### Task 2: Configuration Schema and Preflight Validation

**Files:**
- Create: `template/src/papers_pipeline/errors.py`
- Create: `template/src/papers_pipeline/config.py`
- Create: `template/src/papers_pipeline/cli.py`
- Create: `template/papers.yml.jinja`
- Create: `template/papers.schema.json`
- Test: `template/tests/test_config.py`

**Interfaces:**
- Consumes: Copier answers from Task 1
- Produces: `load_config(path: Path, environ: Mapping[str, str]) -> PipelineConfig`; `ConfigError`; CLI `validate --config PATH`

- [ ] **Step 1: Write table-driven failing validation tests**

```python
# template/tests/test_config.py
import json
from pathlib import Path
import pytest
from papers_pipeline.config import ConfigError, PipelineConfig, load_config


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("pdf: 2", "concurrency.pdf must equal 1"),
        ("max_papers: 0", "conversion.max_papers must be between 1 and 100"),
        ("total_deadline_seconds: 30", "fetch.total_deadline_seconds must be between 60 and 7200"),
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
```

- [ ] **Step 2: Run tests and verify imports fail**

Run: `cd template && uv run pytest tests/test_config.py -v`

Expected: FAIL with `ModuleNotFoundError: papers_pipeline.config`.

- [ ] **Step 3: Implement strict Pydantic configuration**

```python
# template/src/papers_pipeline/errors.py
class PipelineError(RuntimeError):
    """Base class for explicit pipeline failures."""


class ConfigError(PipelineError):
    """Configuration cannot safely run."""


class InfrastructureError(PipelineError):
    """Shared infrastructure cannot safely continue."""


class PaperError(PipelineError):
    """One paper failed without invalidating unrelated work."""
```

```python
# template/src/papers_pipeline/config.py
from pathlib import Path
from typing import Literal, Mapping
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from .errors import ConfigError

AdapterName = Literal["arxiv", "huggingface", "semantic_scholar", "dblp", "biorxiv_crossref", "papers_with_code"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AdapterConfig(StrictModel):
    name: AdapterName
    enabled: bool = True
    secret_env: str | None = None
    lookback_days: int = Field(ge=1, le=365)
    page_size: int = Field(ge=1, le=1000)
    max_pages: int = Field(ge=1, le=100)
    max_results: int = Field(ge=1, le=10000)
    filters: dict[str, str | list[str]] = Field(default_factory=dict)


class TopicConfig(StrictModel):
    include_any: list[str] = Field(default_factory=list)
    include_all: list[str] = Field(default_factory=list)
    exclude_any: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    plugin: str | None = None


class FetchConfig(StrictModel):
    request_timeout_seconds: float = Field(ge=1, le=120)
    retries: int = Field(ge=0, le=5)
    backoff_seconds: float = Field(ge=0, le=30)
    total_deadline_seconds: int = Field(ge=60, le=7200)


class ConversionConfig(StrictModel):
    max_batches_per_run: int = Field(ge=1, le=20)
    max_papers: int = Field(ge=1, le=100)
    max_cost: int = Field(ge=1, le=1000)
    html_cost: int = Field(ge=1, le=100)
    latex_cost: int = Field(ge=1, le=100)
    pdf_cost: int = Field(ge=1, le=1000)


class ConcurrencyConfig(StrictModel):
    html: int = Field(ge=1, le=4)
    latex: int = Field(ge=1, le=4)
    pdf: Literal[1]


class RepositoryConfig(StrictModel):
    name: str = Field(min_length=1)
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*-papers$")
    description: str = Field(min_length=1)


class PipelineConfig(StrictModel):
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


def load_config(path: Path, environ: Mapping[str, str]) -> PipelineConfig:
    try:
        config = PipelineConfig.model_validate(yaml.safe_load(path.read_text()))
    except (OSError, yaml.YAMLError, ValidationError) as error:
        raise ConfigError(str(error)) from error
    for adapter in config.adapters:
        if adapter.enabled and adapter.secret_env and not environ.get(adapter.secret_env):
            raise ConfigError(f"{adapter.name}: missing secret {adapter.secret_env}")
    return config
```

- [ ] **Step 4: Add the rendered default configuration and CLI**

```yaml
# template/papers.yml.jinja
repository:
  name: "{{ project_name }}"
  slug: "{{ project_slug }}"
  description: "{{ topic_description }}"
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
  include_any: ["{{ topic_description }}"]
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
```

```python
# template/src/papers_pipeline/cli.py
from pathlib import Path
import argparse
import os
from .config import load_config


def app() -> None:
    parser = argparse.ArgumentParser(prog="papers-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--config", type=Path, default=Path("papers.yml"))
    args = parser.parse_args()
    if args.command == "validate":
        load_config(args.config, os.environ)
        print(f"valid: {args.config}")
```

- [ ] **Step 5: Generate the checked-in JSON Schema**

Generate the checked-in JSON Schema directly from the validated model:

Run:

```bash
cd template
uv run python -c 'import json; from pathlib import Path; from papers_pipeline.config import PipelineConfig; Path("papers.schema.json").write_text(json.dumps(PipelineConfig.model_json_schema(), indent=2, sort_keys=True) + "\n")'
```

Expected: `papers.schema.json` defines every `papers.yml` field with
`additionalProperties: false` and the numeric limits from `PipelineConfig`.

- [ ] **Step 6: Run validation tests**

Run: `cd template && uv run pytest tests/test_config.py -v`

Expected: PASS with all invalid limits and missing-secret cases rejected.

- [ ] **Step 7: Commit**

```bash
git add template/papers.yml.jinja template/papers.schema.json template/src/papers_pipeline template/tests/test_config.py
git commit -m "feat: validate papers pipeline configuration"
```

### Task 3: Canonical Models, Inventory, and Operational State

**Files:**
- Create: `template/src/papers_pipeline/models.py`
- Create: `template/src/papers_pipeline/state.py`
- Create: `template/src/papers_pipeline/inventory.py`
- Create: `template/.papers-state.yml`
- Create: `template/papers.csv`
- Test: `template/tests/test_state_inventory.py`

**Interfaces:**
- Consumes: `PipelineError`
- Produces: `SourceRecord`, `Paper`, `FailureAttempt`, `PipelineState`; `load_state(Path) -> PipelineState`; `save_state(Path, PipelineState) -> None`; `read_inventory(Path) -> list[Paper]`; `write_inventory(Path, Sequence[Paper]) -> None`

- [ ] **Step 1: Write failing round-trip and backlog-source tests**

```python
# template/tests/test_state_inventory.py
from datetime import datetime, timezone
from pathlib import Path
from papers_pipeline.inventory import read_inventory, write_inventory
from papers_pipeline.models import Paper, PipelineState
from papers_pipeline.state import load_state, save_state


def paper(identifier: str = "arxiv:2401.00001") -> Paper:
    return Paper(
        identifier=identifier,
        title="A Paper",
        abstract="An abstract",
        authors=("A. Author",),
        published=datetime(2024, 1, 2, tzinfo=timezone.utc),
        url="https://example.test/paper",
        source="arxiv",
        input_format="pdf",
        input_url="https://example.test/paper.pdf",
        categories=("cs.CL",),
    )


def test_inventory_round_trip_is_sorted(tmp_path: Path) -> None:
    path = tmp_path / "papers.csv"
    write_inventory(path, [paper("ss:2"), paper("arxiv:1")])
    assert [item.identifier for item in read_inventory(path)] == ["arxiv:1", "ss:2"]


def test_state_contains_no_inventory(tmp_path: Path) -> None:
    path = tmp_path / ".papers-state.yml"
    state = PipelineState(cursors={"arxiv": "cursor-2"}, failures={})
    save_state(path, state)
    assert load_state(path) == state
    assert "papers:" not in path.read_text()
```

- [ ] **Step 2: Run tests and verify model imports fail**

Run: `cd template && uv run pytest tests/test_state_inventory.py -v`

Expected: FAIL with missing model modules.

- [ ] **Step 3: Implement immutable canonical and state models**

```python
# template/src/papers_pipeline/models.py
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field

InputFormat = Literal["html", "latex", "pdf"]


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


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
    categories: tuple[str, ...] = ()
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
    categories: tuple[str, ...] = ()
    doi: str | None = None
    arxiv_id: str | None = None


class FailureAttempt(FrozenModel):
    occurred_at: datetime
    error: str


class PipelineState(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cursors: dict[str, str] = Field(default_factory=dict)
    failures: dict[str, list[FailureAttempt]] = Field(default_factory=dict)
```

- [ ] **Step 4: Implement atomic YAML state and deterministic CSV inventory**

```python
# template/src/papers_pipeline/state.py
from pathlib import Path
import os
import yaml
from .models import PipelineState


def load_state(path: Path) -> PipelineState:
    if not path.exists():
        return PipelineState()
    return PipelineState.model_validate(yaml.safe_load(path.read_text()) or {})


def save_state(path: Path, state: PipelineState) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(yaml.safe_dump(state.model_dump(mode="json"), sort_keys=True))
    os.replace(temporary, path)
```

```python
# template/src/papers_pipeline/inventory.py
import csv
from datetime import datetime
from pathlib import Path
from collections.abc import Sequence
from .models import Paper

FIELDS = (
    "identifier", "title", "abstract", "authors", "published", "url", "source",
    "input_format", "input_url", "categories", "doi", "arxiv_id",
)


def write_inventory(path: Path, papers: Sequence[Paper]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for paper in sorted(papers, key=lambda item: item.identifier):
            row = paper.model_dump(mode="json")
            row["authors"] = "|".join(paper.authors)
            row["categories"] = "|".join(paper.categories)
            writer.writerow(row)


def read_inventory(path: Path) -> list[Paper]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return [
            Paper(
                **{
                    **row,
                    "authors": tuple(filter(None, row["authors"].split("|"))),
                    "categories": tuple(filter(None, row["categories"].split("|"))),
                    "published": datetime.fromisoformat(row["published"]),
                    "doi": row["doi"] or None,
                    "arxiv_id": row["arxiv_id"] or None,
                }
            )
            for row in csv.DictReader(handle)
        ]
```

```yaml
# template/.papers-state.yml
cursors: {}
failures: {}
```

```csv
# template/papers.csv
identifier,title,abstract,authors,published,url,source,input_format,input_url,categories,doi,arxiv_id
```

- [ ] **Step 5: Run state and inventory tests**

Run: `cd template && uv run pytest tests/test_state_inventory.py -v`

Expected: PASS and byte-identical output across repeated writes.

- [ ] **Step 6: Commit**

```bash
git add template/.papers-state.yml template/papers.csv template/src/papers_pipeline template/tests/test_state_inventory.py
git commit -m "feat: add paper inventory and operational state"
```

### Task 4: Normalization, Deduplication, and Topic Gates

**Files:**
- Create: `template/src/papers_pipeline/normalize.py`
- Create: `template/src/papers_pipeline/topics.py`
- Create: `template/topic_plugin.py.jinja`
- Test: `template/tests/test_normalize_topics.py`

**Interfaces:**
- Consumes: `SourceRecord`, `Paper`, `TopicConfig`
- Produces: `normalize(record: SourceRecord) -> Paper`; `deduplicate(papers: Iterable[Paper]) -> list[Paper]`; `TopicDecision(accepted: bool, reason: str)`; `build_topic_gate(config: TopicConfig) -> Callable[[Paper], TopicDecision]`

- [ ] **Step 1: Write failing table-driven normalization and gate tests**

```python
# template/tests/test_normalize_topics.py
import pytest
from papers_pipeline.normalize import deduplicate, normalize
from papers_pipeline.topics import build_topic_gate


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("  Neural   Speech\nRecognition ", "Neural Speech Recognition"),
        ("BirdCLEF: Audio Detection", "BirdCLEF: Audio Detection"),
    ],
)
def test_normalize_collapses_whitespace(source_record, title: str, expected: str) -> None:
    assert normalize(source_record.model_copy(update={"title": title})).title == expected


def test_dedupe_prefers_arxiv_then_doi_then_source(source_record) -> None:
    arxiv = normalize(source_record.model_copy(update={"source": "arxiv", "arxiv_id": "2401.1"}))
    semantic = normalize(source_record.model_copy(update={"source": "semantic_scholar", "arxiv_id": "2401.1"}))
    assert deduplicate([semantic, arxiv]) == [arxiv]


def test_declarative_gate_reports_exclusion(topic_config, paper) -> None:
    gate = build_topic_gate(topic_config.model_copy(update={"exclude_any": ["survey"]}))
    decision = gate(paper.model_copy(update={"abstract": "A survey of speech"}))
    assert decision.accepted is False
    assert decision.reason == "matched excluded term: survey"
```

- [ ] **Step 2: Run tests and verify functions are missing**

Run: `cd template && uv run pytest tests/test_normalize_topics.py -v`

Expected: FAIL on imports from `normalize` and `topics`.

- [ ] **Step 3: Implement deterministic normalization and precedence**

```python
# template/src/papers_pipeline/normalize.py
from collections.abc import Iterable
import re
import unicodedata
from .models import Paper, SourceRecord

SOURCE_RANK = {
    "arxiv": 0, "crossref": 1, "semantic_scholar": 2, "dblp": 3,
    "huggingface": 4, "papers_with_code": 5, "biorxiv": 6,
}


def clean(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()


def normalize(record: SourceRecord) -> Paper:
    identifier = (
        f"arxiv:{record.arxiv_id}" if record.arxiv_id
        else f"doi:{record.doi.lower()}" if record.doi
        else f"{record.source}:{record.source_id}"
    )
    return Paper(
        **record.model_dump(exclude={"source_id"}),
        identifier=identifier,
        title=clean(record.title),
        abstract=clean(record.abstract),
        authors=tuple(clean(author) for author in record.authors),
        categories=tuple(sorted(set(record.categories))),
    )


def _identity(paper: Paper) -> str:
    if paper.arxiv_id:
        return f"arxiv:{paper.arxiv_id.casefold()}"
    if paper.doi:
        return f"doi:{paper.doi.casefold()}"
    bibliographic = f"{clean(paper.title).casefold()}|{paper.published.date().isoformat()}"
    return f"bibliographic:{bibliographic}"


def deduplicate(papers: Iterable[Paper]) -> list[Paper]:
    selected: dict[str, Paper] = {}
    for paper in papers:
        key = _identity(paper)
        current = selected.get(key)
        if current is None or SOURCE_RANK[paper.source] < SOURCE_RANK[current.source]:
            selected[key] = paper
    return sorted(selected.values(), key=lambda item: item.identifier)
```

- [ ] **Step 4: Implement declarative gates and narrow plugin loading**

```python
# template/src/papers_pipeline/topics.py
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from .config import TopicConfig
from .errors import ConfigError
from .models import Paper


@dataclass(frozen=True)
class TopicDecision:
    accepted: bool
    reason: str


def _terms(paper: Paper) -> str:
    return f"{paper.title} {paper.abstract}".casefold()


def build_topic_gate(config: TopicConfig) -> Callable[[Paper], TopicDecision]:
    if config.plugin:
        try:
            module_name, function_name = config.plugin.split(":", 1)
            function = getattr(import_module(module_name), function_name)
        except (ValueError, ImportError, AttributeError) as error:
            raise ConfigError(f"invalid topic plugin: {config.plugin}") from error

        def plugin_gate(paper: Paper) -> TopicDecision:
            decision = function(paper)
            if not isinstance(decision, TopicDecision):
                raise ConfigError("topic plugin must return TopicDecision")
            return decision

        return plugin_gate

    def gate(paper: Paper) -> TopicDecision:
        text = _terms(paper)
        for term in config.exclude_any:
            if term.casefold() in text:
                return TopicDecision(False, f"matched excluded term: {term}")
        if config.include_all and not all(term.casefold() in text for term in config.include_all):
            return TopicDecision(False, "missing required terms")
        if config.include_any and not any(term.casefold() in text for term in config.include_any):
            return TopicDecision(False, "missing included term")
        if config.categories and not set(config.categories).intersection(paper.categories):
            return TopicDecision(False, "missing included category")
        return TopicDecision(True, "accepted")

    return gate
```

```python
# template/topic_plugin.py.jinja
from papers_pipeline.models import Paper
from papers_pipeline.topics import TopicDecision


def accept_topic(paper: Paper) -> TopicDecision:
    return TopicDecision(accepted=True, reason="repository plugin accepted paper")
```

- [ ] **Step 5: Run normalization and topic tests**

Run: `cd template && uv run pytest tests/test_normalize_topics.py -v`

Expected: PASS, including deterministic source precedence and plugin signature checks.

- [ ] **Step 6: Commit**

```bash
git add template/src/papers_pipeline/normalize.py template/src/papers_pipeline/topics.py template/topic_plugin.py.jinja template/tests/test_normalize_topics.py
git commit -m "feat: normalize dedupe and filter papers"
```

### Task 5: Deadline-Aware HTTP and Retry Policy

**Files:**
- Create: `template/src/papers_pipeline/http.py`
- Test: `template/tests/test_http.py`

**Interfaces:**
- Consumes: `FetchConfig`, `InfrastructureError`
- Produces: `Deadline.start(seconds: float, clock: Callable[[], float]) -> Deadline`; `Deadline.remaining() -> float`; `RequestClient.get_text(url: str, params: Mapping[str, str], headers: Mapping[str, str]) -> str`

- [ ] **Step 1: Write failing timeout, retry, and deadline tests**

```python
# template/tests/test_http.py
import httpx
import pytest
from papers_pipeline.errors import InfrastructureError
from papers_pipeline.http import Deadline, RequestClient


@pytest.mark.asyncio
async def test_retry_stops_at_total_deadline(fetch_config, fake_clock) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(503))
    deadline = Deadline.start(2, fake_clock)
    client = RequestClient(fetch_config, deadline, transport=transport, sleep=fake_clock.sleep)
    with pytest.raises(InfrastructureError, match="fetch deadline exceeded"):
        await client.get_text("https://example.test", {}, {})
    assert fake_clock.sleeps == [1.0, 1.0]


@pytest.mark.asyncio
async def test_permanent_401_is_not_retried(fetch_config, fake_clock) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(401))
    client = RequestClient(fetch_config, Deadline.start(30, fake_clock), transport=transport)
    with pytest.raises(InfrastructureError, match="authentication failed"):
        await client.get_text("https://example.test", {}, {})
    assert fake_clock.sleeps == []
```

- [ ] **Step 2: Run tests and verify the client is missing**

Run: `cd template && uv run pytest tests/test_http.py -v`

Expected: FAIL with `ModuleNotFoundError: papers_pipeline.http`.

- [ ] **Step 3: Implement one shared deadline and bounded retries**

```python
# template/src/papers_pipeline/http.py
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import asyncio
import time
import httpx
from .config import FetchConfig
from .errors import InfrastructureError


@dataclass(frozen=True)
class Deadline:
    expires_at: float
    clock: Callable[[], float]

    @classmethod
    def start(cls, seconds: float, clock: Callable[[], float] = time.monotonic) -> "Deadline":
        return cls(clock() + seconds, clock)

    def remaining(self) -> float:
        remaining = self.expires_at - self.clock()
        if remaining <= 0:
            raise InfrastructureError("fetch deadline exceeded")
        return remaining


class RequestClient:
    def __init__(
        self,
        config: FetchConfig,
        deadline: Deadline,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self.deadline = deadline
        self.client = httpx.AsyncClient(transport=transport)
        self.sleep = sleep
        self.events: list[str] = []

    async def get_text(
        self, url: str, params: Mapping[str, str], headers: Mapping[str, str]
    ) -> str:
        for attempt in range(self.config.retries + 1):
            timeout = min(self.config.request_timeout_seconds, self.deadline.remaining())
            try:
                response = await self.client.get(url, params=params, headers=headers, timeout=timeout)
            except (httpx.TimeoutException, httpx.NetworkError) as error:
                if attempt == self.config.retries:
                    raise InfrastructureError(f"request retries exhausted: {url}") from error
                self.events.append(f"retry {attempt + 1}: network failure for {url}")
            else:
                if response.status_code in {401, 403}:
                    raise InfrastructureError(f"authentication failed: {url}")
                if response.status_code < 500 and response.status_code != 429:
                    response.raise_for_status()
                    return response.text
                if attempt == self.config.retries:
                    raise InfrastructureError(f"request retries exhausted: {url}")
                self.events.append(
                    f"retry {attempt + 1}: HTTP {response.status_code} for {url}"
                )
            delay = self.config.backoff_seconds * (2 ** attempt)
            if delay >= self.deadline.remaining():
                raise InfrastructureError("fetch deadline exceeded")
            await self.sleep(delay)
        raise AssertionError("retry loop exhausted without result")
```

- [ ] **Step 4: Run HTTP policy tests**

Run: `cd template && uv run pytest tests/test_http.py -v`

Expected: PASS with exact retry counts and no sleep after permanent authentication errors.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/http.py template/tests/test_http.py
git commit -m "feat: bound fetch retries and deadlines"
```

### Task 6: Adapter Contract and Recorded Fixture Harness

**Files:**
- Create: `template/src/papers_pipeline/adapters/base.py`
- Create: `template/src/papers_pipeline/adapters/__init__.py`
- Create: `template/tests/adapters/contract.py`
- Create: `template/tests/conftest.py`

**Interfaces:**
- Consumes: `SourceRecord`, `RequestClient`
- Produces: `FetchWindow(start: datetime, end: datetime)`; `FetchPage(records: tuple[SourceRecord, ...], next_cursor: str | None, capped: bool, permanent_errors: tuple[str, ...])`; `Adapter.fetch(window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage`

- [ ] **Step 1: Write the reusable adapter contract**

```python
# template/tests/adapters/contract.py
from datetime import datetime, timezone
from papers_pipeline.adapters.base import Adapter

WINDOW_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2024, 1, 8, tzinfo=timezone.utc)


async def assert_adapter_contract(adapter: Adapter, client, config) -> None:
    page = await adapter.fetch(
        window=adapter.window_type(WINDOW_START, WINDOW_END),
        cursor=None,
        client=client,
        config=config,
    )
    assert page.records
    assert all(record.source in adapter.record_sources for record in page.records)
    assert all(WINDOW_START <= record.published <= WINDOW_END for record in page.records)
    assert page.next_cursor is None or page.next_cursor
```

- [ ] **Step 2: Run the contract module and verify base types are missing**

Run: `cd template && uv run pytest tests/adapters -v`

Expected: FAIL with missing `papers_pipeline.adapters.base`.

- [ ] **Step 3: Define the exact adapter protocol**

```python
# template/src/papers_pipeline/adapters/base.py
from datetime import datetime
from collections.abc import Callable, Iterable
from typing import Protocol, TypeVar
from pydantic import BaseModel, ConfigDict
from ..config import AdapterConfig
from ..errors import PaperError
from ..http import RequestClient
from ..models import SourceRecord

RawRecord = TypeVar("RawRecord")


class FetchWindow(BaseModel):
    model_config = ConfigDict(frozen=True)
    start: datetime
    end: datetime


class FetchPage(BaseModel):
    model_config = ConfigDict(frozen=True)
    records: tuple[SourceRecord, ...]
    next_cursor: str | None
    capped: bool
    permanent_errors: tuple[str, ...] = ()


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
    errors: list[str] = []
    for item in items:
        try:
            records.append(parser(item))
        except PaperError as error:
            errors.append(str(error))
    return tuple(records), tuple(errors)
```

- [ ] **Step 4: Add shared fixture helpers**

```python
# template/tests/conftest.py
from pathlib import Path
import httpx
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixture_transport():
    def build(routes: dict[str, tuple[int, str]]) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            status, body = routes[str(request.url)]
            return httpx.Response(status, text=body)
        return httpx.MockTransport(handler)
    return build
```

- [ ] **Step 5: Type-check the contract**

Run: `cd template && uv run mypy src tests/adapters/contract.py`

Expected: PASS with no incompatible adapter signatures.

- [ ] **Step 6: Commit**

```bash
git add template/src/papers_pipeline/adapters template/tests/adapters template/tests/conftest.py
git commit -m "test: define source adapter contract"
```

### Task 7: arXiv and Hugging Face Papers Adapters

**Files:**
- Create: `template/src/papers_pipeline/adapters/arxiv.py`
- Create: `template/src/papers_pipeline/adapters/huggingface.py`
- Create: `template/tests/fixtures/adapters/arxiv/page.xml`
- Create: `template/tests/fixtures/adapters/huggingface/page.json`
- Test: `template/tests/adapters/test_arxiv.py`
- Test: `template/tests/adapters/test_huggingface.py`

**Interfaces:**
- Consumes: `Adapter`, `FetchWindow`, `FetchPage`, `RequestClient`
- Produces: `ArxivAdapter`, `HuggingFaceAdapter`, both satisfying `Adapter`

- [ ] **Step 1: Add recorded-fixture contract tests**

```python
# template/tests/adapters/test_arxiv.py
import pytest
from papers_pipeline.adapters.arxiv import ArxivAdapter
from .contract import assert_adapter_contract


@pytest.mark.asyncio
async def test_arxiv_contract(arxiv_client, arxiv_config) -> None:
    await assert_adapter_contract(ArxivAdapter(), arxiv_client, arxiv_config)
```

```python
# template/tests/adapters/test_huggingface.py
import pytest
from papers_pipeline.adapters.huggingface import HuggingFaceAdapter
from .contract import assert_adapter_contract


@pytest.mark.asyncio
async def test_huggingface_contract(huggingface_client, huggingface_config) -> None:
    await assert_adapter_contract(HuggingFaceAdapter(), huggingface_client, huggingface_config)
```

Use these exact first-page fixture shapes:

```xml
<!-- template/tests/fixtures/adapters/arxiv/page.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/2401.00001</id>
    <published>2024-01-02T00:00:00Z</published>
    <title>Fixture Speech Paper</title>
    <summary>Recorded offline abstract.</summary>
    <author><name>A. Author</name></author>
    <category term="cs.CL"/>
  </entry>
  <entry>
    <id>https://arxiv.org/abs/2312.00001</id>
    <published>2023-12-01T00:00:00Z</published>
    <title>Out of Window</title>
    <summary>Excluded by date.</summary>
    <author><name>B. Author</name></author>
    <category term="cs.CL"/>
  </entry>
</feed>
```

```json
[
  {
    "paper": {
      "id": "2401.00001",
      "title": "Fixture Speech Paper",
      "summary": "Recorded offline abstract.",
      "authors": [{"name": "A. Author"}],
      "publishedAt": "2024-01-02T00:00:00Z"
    }
  },
  {
    "paper": {
      "id": "2312.00001",
      "title": "Out of Window",
      "summary": "Excluded by date.",
      "authors": [{"name": "B. Author"}],
      "publishedAt": "2023-12-01T00:00:00Z"
    }
  }
]
```

```xml
<!-- template/tests/fixtures/adapters/arxiv/page-2.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://arxiv.org/abs/2401.00002</id>
    <published>2024-01-03T00:00:00Z</published>
    <title>Second Fixture Speech Paper</title>
    <summary>Recorded continuation page.</summary>
    <author><name>C. Author</name></author>
    <category term="cs.CL"/>
  </entry>
</feed>
```

```json
[
  {
    "paper": {
      "id": "2401.00002",
      "title": "Second Fixture Speech Paper",
      "summary": "Recorded continuation page.",
      "authors": [{"name": "C. Author"}],
      "publishedAt": "2024-01-03T00:00:00Z"
    }
  }
]
```

The tests route page zero and page one to these files and assert that
`max_pages` and `max_results` set `capped=True` while preserving the returned
cursor.

- [ ] **Step 2: Run the two adapter tests and verify imports fail**

Run: `cd template && uv run pytest tests/adapters/test_arxiv.py tests/adapters/test_huggingface.py -v`

Expected: FAIL because both adapter modules are absent.

- [ ] **Step 3: Implement arXiv Atom parsing**

```python
# template/src/papers_pipeline/adapters/arxiv.py
from datetime import datetime
from xml.etree import ElementTree
from .base import FetchPage, FetchWindow, collect_records
from ..config import AdapterConfig
from ..http import RequestClient
from ..models import SourceRecord

ATOM = {"a": "http://www.w3.org/2005/Atom", "x": "http://arxiv.org/schemas/atom"}


class ArxivAdapter:
    name = "arxiv"
    record_sources = frozenset({"arxiv"})
    window_type = FetchWindow

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        start = int(cursor or "0")
        text = await client.get_text(
            "https://export.arxiv.org/api/query",
            {"search_query": "all:*", "start": str(start), "max_results": str(config.page_size), "sortBy": "submittedDate"},
            {},
        )
        root = ElementTree.fromstring(text)
        entries = root.findall("a:entry", ATOM)
        records, errors = collect_records(entries, self._record)
        records = tuple(item for item in records if window.start <= item.published <= window.end)
        consumed = start + len(entries)
        capped = consumed >= config.max_results
        return FetchPage(records=records, next_cursor=str(consumed) if entries else None, capped=capped, permanent_errors=errors)

    def _record(self, entry: ElementTree.Element) -> SourceRecord:
        identifier = entry.findtext("a:id", namespaces=ATOM, default="").rsplit("/", 1)[-1]
        return SourceRecord(
            source=self.name,
            source_id=identifier,
            arxiv_id=identifier,
            title=entry.findtext("a:title", namespaces=ATOM, default=""),
            abstract=entry.findtext("a:summary", namespaces=ATOM, default=""),
            authors=tuple(node.findtext("a:name", namespaces=ATOM, default="") for node in entry.findall("a:author", ATOM)),
            published=datetime.fromisoformat(entry.findtext("a:published", namespaces=ATOM, default="").replace("Z", "+00:00")),
            url=f"https://arxiv.org/abs/{identifier}",
            input_format="pdf",
            input_url=f"https://arxiv.org/pdf/{identifier}",
            categories=tuple(node.attrib["term"] for node in entry.findall("a:category", ATOM)),
        )
```

- [ ] **Step 4: Implement Hugging Face Papers JSON parsing**

```python
# template/src/papers_pipeline/adapters/huggingface.py
from datetime import datetime
import json
from .base import FetchPage, FetchWindow, collect_records
from ..config import AdapterConfig
from ..http import RequestClient
from ..models import SourceRecord


class HuggingFaceAdapter:
    name = "huggingface"
    record_sources = frozenset({"huggingface"})
    window_type = FetchWindow

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        page = int(cursor or "0")
        payload = json.loads(await client.get_text(
            "https://huggingface.co/api/daily_papers",
            {"date": window.end.date().isoformat(), "p": str(page), "limit": str(config.page_size)},
            {},
        ))
        records, errors = collect_records(
            (item["paper"] for item in payload),
            self._record,
        )
        records = tuple(item for item in records if window.start <= item.published <= window.end)
        consumed = (page + 1) * config.page_size
        capped = consumed >= config.max_results
        return FetchPage(records=records, next_cursor=str(page + 1) if payload else None, capped=capped, permanent_errors=errors)

    def _record(self, item: dict[str, object]) -> SourceRecord:
        identifier = str(item["id"])
        return SourceRecord(
            source=self.name,
            source_id=identifier,
            arxiv_id=identifier,
            title=str(item["title"]),
            abstract=str(item.get("summary", "")),
            authors=tuple(str(author["name"]) for author in item.get("authors", [])),
            published=datetime.fromisoformat(str(item["publishedAt"]).replace("Z", "+00:00")),
            url=f"https://huggingface.co/papers/{identifier}",
            input_format="pdf",
            input_url=f"https://arxiv.org/pdf/{identifier}",
        )
```

- [ ] **Step 5: Run contract, cap, malformed-record, and continuation tests**

Run: `cd template && uv run pytest tests/adapters/test_arxiv.py tests/adapters/test_huggingface.py -v`

Expected: PASS; malformed records are reported as permanent record failures without hiding valid fixture records.

- [ ] **Step 6: Commit**

```bash
git add template/src/papers_pipeline/adapters/arxiv.py template/src/papers_pipeline/adapters/huggingface.py template/tests/adapters template/tests/fixtures/adapters
git commit -m "feat: add arXiv and Hugging Face adapters"
```

### Task 8: Semantic Scholar and DBLP Adapters

**Files:**
- Create: `template/src/papers_pipeline/adapters/semantic_scholar.py`
- Create: `template/src/papers_pipeline/adapters/dblp.py`
- Create: `template/tests/fixtures/adapters/semantic_scholar/page.json`
- Create: `template/tests/fixtures/adapters/dblp/page.json`
- Test: `template/tests/adapters/test_semantic_scholar.py`
- Test: `template/tests/adapters/test_dblp.py`

**Interfaces:**
- Consumes: exact Task 6 `Adapter` protocol
- Produces: `SemanticScholarAdapter`, `DblpAdapter`

- [ ] **Step 1: Write fixture-backed contract and credential-header tests**

```python
# template/tests/adapters/test_semantic_scholar.py
@pytest.mark.asyncio
async def test_semantic_scholar_uses_secret_and_offset_contract(client, config) -> None:
    page = await SemanticScholarAdapter(api_key="secret").fetch(WINDOW, None, client, config)
    assert page.records[0].source_id == "abc123"
    assert client.requests[0].headers["x-api-key"] == "secret"
    assert page.next_cursor == "1"
```

```python
# template/tests/adapters/test_dblp.py
@pytest.mark.asyncio
async def test_dblp_uses_stable_hit_offset(client, config) -> None:
    page = await DblpAdapter().fetch(WINDOW, "50", client, config)
    assert page.records[0].source == "dblp"
    assert page.next_cursor == "51"
```

```json
{
  "total": 1,
  "offset": 0,
  "next": 100,
  "data": [
    {
      "paperId": "abc123",
      "title": "Fixture Speech Paper",
      "abstract": "Recorded Semantic Scholar response.",
      "authors": [{"name": "A. Author"}],
      "publicationDate": "2024-01-02",
      "url": "https://www.semanticscholar.org/paper/abc123",
      "externalIds": {"ArXiv": "2401.00001", "DOI": "10.1000/fixture"},
      "openAccessPdf": {"url": "https://example.test/fixture.pdf"}
    }
  ]
}
```

```json
{
  "result": {
    "hits": {
      "@total": "1",
      "hit": [
        {
          "info": {
            "key": "conf/test/Fixture24",
            "title": "Fixture Speech Paper",
            "authors": {"author": [{"text": "A. Author"}]},
            "year": "2024",
            "doi": "10.1000/fixture",
            "ee": "https://example.test/fixture.html",
            "url": "https://dblp.org/rec/conf/test/Fixture24"
          }
        }
      ]
    }
  }
}
```

- [ ] **Step 2: Run tests and verify adapter imports fail**

Run: `cd template && uv run pytest tests/adapters/test_semantic_scholar.py tests/adapters/test_dblp.py -v`

Expected: FAIL on missing modules.

- [ ] **Step 3: Implement both JSON adapters using the shared page model**

```python
# Core implementation required in semantic_scholar.py
class SemanticScholarAdapter:
    name = "semantic_scholar"
    record_sources = frozenset({"semantic_scholar"})
    window_type = FetchWindow

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        offset = int(cursor or "0")
        payload = json.loads(await client.get_text(
            "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
            {"query": str(config.filters.get("query", "*")), "offset": str(offset), "limit": str(config.page_size), "fields": "paperId,title,abstract,authors,publicationDate,url,externalIds,openAccessPdf"},
            {"x-api-key": self.api_key},
        ))
        records, errors = collect_records(payload["data"], parse_semantic_scholar)
        next_offset = offset + len(payload["data"])
        capped = next_offset >= config.max_results
        return FetchPage(records=within_window(records, window), next_cursor=str(next_offset) if payload["data"] else None, capped=capped, permanent_errors=errors)
```

```python
# Core implementation required in dblp.py
class DblpAdapter:
    name = "dblp"
    record_sources = frozenset({"dblp"})
    window_type = FetchWindow

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        offset = int(cursor or "0")
        payload = json.loads(await client.get_text(
            "https://dblp.org/search/publ/api",
            {"q": str(config.filters.get("query", "*")), "f": str(offset), "h": str(config.page_size), "format": "json"},
            {},
        ))
        hits = payload["result"]["hits"]["hit"]
        records, errors = collect_records(
            (hit["info"] for hit in hits),
            parse_dblp,
        )
        next_offset = offset + len(hits)
        capped = next_offset >= config.max_results
        return FetchPage(records=within_window(records, window), next_cursor=str(next_offset) if hits else None, capped=capped, permanent_errors=errors)
```

Add these complete parser helpers beside the fetch methods:

```python
# semantic_scholar.py
from datetime import datetime
from collections.abc import Iterable
from .base import FetchWindow
from ..errors import PaperError
from ..models import SourceRecord


def within_window(
    records: Iterable[SourceRecord], window: FetchWindow
) -> tuple[SourceRecord, ...]:
    return tuple(
        record
        for record in records
        if window.start <= record.published <= window.end
    )


def parse_semantic_scholar(item: dict[str, object]) -> SourceRecord:
    paper_id = str(item.get("paperId") or "")
    title = str(item.get("title") or "")
    published = str(item.get("publicationDate") or "")
    pdf = item.get("openAccessPdf")
    input_url = str(pdf.get("url") or "") if isinstance(pdf, dict) else ""
    if not paper_id or not title or not published or not input_url:
        raise PaperError("Semantic Scholar record lacks id, title, date, or PDF")
    external = item.get("externalIds")
    identifiers = external if isinstance(external, dict) else {}
    authors = item.get("authors")
    author_rows = authors if isinstance(authors, list) else []
    return SourceRecord(
        source="semantic_scholar",
        source_id=paper_id,
        title=title,
        abstract=str(item.get("abstract") or ""),
        authors=tuple(
            str(author.get("name") or "")
            for author in author_rows
            if isinstance(author, dict)
        ),
        published=datetime.fromisoformat(published + "T00:00:00+00:00"),
        url=str(item.get("url") or f"https://www.semanticscholar.org/paper/{paper_id}"),
        input_format="pdf",
        input_url=input_url,
        doi=str(identifiers.get("DOI")) if identifiers.get("DOI") else None,
        arxiv_id=str(identifiers.get("ArXiv")) if identifiers.get("ArXiv") else None,
    )
```

```python
# dblp.py
from datetime import datetime, timezone
from ..errors import PaperError
from ..models import SourceRecord


def parse_dblp(item: dict[str, object]) -> SourceRecord:
    key = str(item.get("key") or "")
    title = str(item.get("title") or "")
    year = str(item.get("year") or "")
    input_url = str(item.get("ee") or item.get("url") or "")
    if not key or not title or not year or not input_url:
        raise PaperError("DBLP record lacks key, title, year, or electronic URL")
    raw_authors = item.get("authors", {}).get("author", [])
    author_rows = raw_authors if isinstance(raw_authors, list) else [raw_authors]
    authors = tuple(
        str(author.get("text") or "") if isinstance(author, dict) else str(author)
        for author in author_rows
    )
    return SourceRecord(
        source="dblp",
        source_id=key,
        title=title,
        abstract="",
        authors=authors,
        published=datetime(int(year), 1, 1, tzinfo=timezone.utc),
        url=str(item.get("url") or input_url),
        input_format="html",
        input_url=input_url,
        doi=str(item.get("doi")) if item.get("doi") else None,
    )
```

In each adapter, catch `PaperError` per item, append the concise record error to
the returned page's permanent-error collection added to `FetchPage`, and
continue parsing valid items. Do not advance the persisted cursor if the page
cannot be parsed far enough to identify its next offset.

- [ ] **Step 4: Run contract and error classification tests**

Run: `cd template && uv run pytest tests/adapters/test_semantic_scholar.py tests/adapters/test_dblp.py -v`

Expected: PASS with credential, pagination, date-window, cap, and malformed-record assertions.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/adapters/semantic_scholar.py template/src/papers_pipeline/adapters/dblp.py template/tests/adapters template/tests/fixtures/adapters
git commit -m "feat: add Semantic Scholar and DBLP adapters"
```

### Task 9: bioRxiv/Crossref and Papers With Code Adapters

**Files:**
- Create: `template/src/papers_pipeline/adapters/biorxiv_crossref.py`
- Create: `template/src/papers_pipeline/adapters/papers_with_code.py`
- Create: `template/tests/fixtures/adapters/biorxiv_crossref/biorxiv.json`
- Create: `template/tests/fixtures/adapters/biorxiv_crossref/crossref.json`
- Create: `template/tests/fixtures/adapters/papers_with_code/page.json`
- Test: `template/tests/adapters/test_biorxiv_crossref.py`
- Test: `template/tests/adapters/test_papers_with_code.py`

**Interfaces:**
- Consumes: exact Task 6 `Adapter` protocol
- Produces: `BiorxivCrossrefAdapter`, `PapersWithCodeAdapter`

- [ ] **Step 1: Write contract tests that cover both bioRxiv and Crossref paths**

```python
# template/tests/adapters/test_biorxiv_crossref.py
@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["biorxiv", "crossref"])
async def test_combined_adapter_contract(provider, client, config) -> None:
    config = config.model_copy(update={"filters": {"provider": provider}})
    page = await BiorxivCrossrefAdapter().fetch(WINDOW, None, client, config)
    assert page.records
    assert page.records[0].source == provider
```

```python
# template/tests/adapters/test_papers_with_code.py
@pytest.mark.asyncio
async def test_papers_with_code_contract(client, config) -> None:
    page = await PapersWithCodeAdapter().fetch(WINDOW, None, client, config)
    assert page.records[0].source == "papers_with_code"
    assert page.next_cursor == "https://paperswithcode.com/api/v1/papers/?page=2"
```

```json
{
  "collection": [
    {
      "doi": "10.1101/2024.01.02.123456",
      "title": "Fixture Bioacoustics Paper",
      "authors": "A. Author; B. Author",
      "date": "2024-01-02",
      "category": "bioinformatics",
      "abstract": "Recorded bioRxiv response."
    }
  ]
}
```

```json
{
  "message": {
    "items": [
      {
        "DOI": "10.1000/fixture",
        "title": ["Fixture Journal Paper"],
        "abstract": "Recorded Crossref response.",
        "author": [{"given": "A.", "family": "Author"}],
        "published": {"date-parts": [[2024, 1, 2]]},
        "URL": "https://doi.org/10.1000/fixture",
        "link": [
          {
            "URL": "https://example.test/fixture.html",
            "content-type": "text/html"
          }
        }
      }
    ],
    "next-cursor": "cursor-2"
  }
}
```

```json
{
  "count": 1,
  "next": "https://paperswithcode.com/api/v1/papers/?page=2",
  "results": [
    {
      "id": "fixture-paper",
      "arxiv_id": "2401.00001",
      "title": "Fixture Machine Learning Paper",
      "abstract": "Recorded Papers With Code response.",
      "authors": ["A. Author"],
      "published": "2024-01-02T00:00:00Z",
      "url_abs": "https://paperswithcode.com/paper/fixture-paper",
      "url_pdf": "https://example.test/fixture.pdf"
    }
  ]
}
```

- [ ] **Step 2: Run tests and verify modules are absent**

Run: `cd template && uv run pytest tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py -v`

Expected: FAIL on missing imports.

- [ ] **Step 3: Implement provider-specific URL and cursor handling**

```python
# Required fetch branch in biorxiv_crossref.py
class BiorxivCrossrefAdapter:
    name = "biorxiv_crossref"
    record_sources = frozenset({"biorxiv", "crossref"})
    window_type = FetchWindow

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        provider = str(config.filters.get("provider", "biorxiv"))
        if provider == "biorxiv":
            offset = int(cursor or "0")
            url = f"https://api.biorxiv.org/details/biorxiv/{window.start.date()}/{window.end.date()}/{offset}"
            payload = json.loads(await client.get_text(url, {}, {}))
            items = payload["collection"]
            records, errors = collect_records(items, parse_biorxiv)
            next_cursor = str(offset + len(items)) if items else None
        elif provider == "crossref":
            cursor_value = cursor or "*"
            payload = json.loads(await client.get_text(
                "https://api.crossref.org/works",
                {"filter": f"from-pub-date:{window.start.date()},until-pub-date:{window.end.date()}", "rows": str(config.page_size), "cursor": cursor_value},
                {"User-Agent": "papers-pipeline/0.1"},
            ))
            items = payload["message"]["items"]
            records, errors = collect_records(items, parse_crossref)
            next_cursor = payload["message"].get("next-cursor")
        else:
            raise ConfigError("biorxiv_crossref.filters.provider must be biorxiv or crossref")
        capped = len(records) >= config.max_results
        return FetchPage(records=records[:config.max_results], next_cursor=next_cursor, capped=capped, permanent_errors=errors)
```

```python
# Required fetch method in papers_with_code.py
class PapersWithCodeAdapter:
    name = "papers_with_code"
    record_sources = frozenset({"papers_with_code"})
    window_type = FetchWindow

    async def fetch(self, window: FetchWindow, cursor: str | None, client: RequestClient, config: AdapterConfig) -> FetchPage:
        url = cursor or "https://paperswithcode.com/api/v1/papers/"
        payload = json.loads(await client.get_text(url, {"items_per_page": str(config.page_size)}, {}))
        records, errors = collect_records(payload["results"], parse_papers_with_code)
        records = tuple(item for item in records if window.start <= item.published <= window.end)
        capped = len(records) >= config.max_results
        return FetchPage(records=records[:config.max_results], next_cursor=payload.get("next"), capped=capped, permanent_errors=errors)
```

Add these typed parsers beside the methods:

```python
# biorxiv_crossref.py
from datetime import datetime, timezone
from ..errors import PaperError
from ..models import SourceRecord


def parse_biorxiv(item: dict[str, object]) -> SourceRecord:
    doi = str(item.get("doi") or "")
    title = str(item.get("title") or "")
    published = str(item.get("date") or "")
    if not doi or not title or not published:
        raise PaperError("bioRxiv record lacks DOI, title, or date")
    return SourceRecord(
        source="biorxiv",
        source_id=doi,
        doi=doi,
        title=title,
        abstract=str(item.get("abstract") or ""),
        authors=tuple(
            author.strip()
            for author in str(item.get("authors") or "").split(";")
            if author.strip()
        ),
        published=datetime.fromisoformat(published + "T00:00:00+00:00"),
        url=f"https://doi.org/{doi}",
        input_format="pdf",
        input_url=f"https://www.biorxiv.org/content/{doi}.full.pdf",
        categories=(str(item.get("category") or ""),),
    )


def parse_crossref(item: dict[str, object]) -> SourceRecord:
    doi = str(item.get("DOI") or "")
    titles = item.get("title")
    title_rows = titles if isinstance(titles, list) else []
    links = item.get("link")
    link_rows = links if isinstance(links, list) else []
    html = next(
        (
            str(link["URL"])
            for link in link_rows
            if isinstance(link, dict) and link.get("content-type") == "text/html"
        ),
        "",
    )
    pdf = next(
        (
            str(link["URL"])
            for link in link_rows
            if isinstance(link, dict) and link.get("content-type") == "application/pdf"
        ),
        "",
    )
    date_parts = item.get("published", {}).get("date-parts", [[]])[0]
    if not doi or not title_rows or not date_parts or not (html or pdf):
        raise PaperError("Crossref record lacks DOI, title, date, or full text")
    year, month, day = [*date_parts, 1, 1][:3]
    authors = item.get("author")
    author_rows = authors if isinstance(authors, list) else []
    return SourceRecord(
        source="crossref",
        source_id=doi,
        doi=doi,
        title=str(title_rows[0]),
        abstract=str(item.get("abstract") or ""),
        authors=tuple(
            " ".join(
                filter(None, [str(author.get("given") or ""), str(author.get("family") or "")])
            )
            for author in author_rows
            if isinstance(author, dict)
        ),
        published=datetime(int(year), int(month), int(day), tzinfo=timezone.utc),
        url=str(item.get("URL") or f"https://doi.org/{doi}"),
        input_format="html" if html else "pdf",
        input_url=html or pdf,
    )
```

```python
# papers_with_code.py
from datetime import datetime
from ..errors import PaperError
from ..models import SourceRecord


def parse_papers_with_code(item: dict[str, object]) -> SourceRecord:
    paper_id = str(item.get("id") or "")
    title = str(item.get("title") or "")
    published = str(item.get("published") or "")
    pdf = str(item.get("url_pdf") or "")
    if not paper_id or not title or not published or not pdf:
        raise PaperError("Papers With Code record lacks id, title, date, or PDF")
    authors = item.get("authors")
    author_rows = authors if isinstance(authors, list) else []
    return SourceRecord(
        source="papers_with_code",
        source_id=paper_id,
        title=title,
        abstract=str(item.get("abstract") or ""),
        authors=tuple(str(author) for author in author_rows),
        published=datetime.fromisoformat(published.replace("Z", "+00:00")),
        url=str(item.get("url_abs") or f"https://paperswithcode.com/paper/{paper_id}"),
        input_format="pdf",
        input_url=pdf,
        arxiv_id=str(item.get("arxiv_id")) if item.get("arxiv_id") else None,
    )
```

Use the same per-record `PaperError` collection defined in Task 8 so valid
records survive malformed neighbors and permanent errors appear in
`FetchPage.permanent_errors`.

Export a complete runtime registry:

```python
# template/src/papers_pipeline/adapters/__init__.py
from collections.abc import Mapping
from .arxiv import ArxivAdapter
from .base import Adapter
from .biorxiv_crossref import BiorxivCrossrefAdapter
from .dblp import DblpAdapter
from .huggingface import HuggingFaceAdapter
from .papers_with_code import PapersWithCodeAdapter
from .semantic_scholar import SemanticScholarAdapter
from ..config import PipelineConfig


def build_adapters(
    config: PipelineConfig,
    environ: Mapping[str, str],
) -> dict[str, Adapter]:
    semantic_config = next(
        (item for item in config.adapters if item.name == "semantic_scholar"),
        None,
    )
    semantic_key = (
        environ[semantic_config.secret_env]
        if semantic_config is not None and semantic_config.secret_env is not None
        else ""
    )
    return {
        "arxiv": ArxivAdapter(),
        "huggingface": HuggingFaceAdapter(),
        "semantic_scholar": SemanticScholarAdapter(semantic_key),
        "dblp": DblpAdapter(),
        "biorxiv_crossref": BiorxivCrossrefAdapter(),
        "papers_with_code": PapersWithCodeAdapter(),
    }
```

- [ ] **Step 4: Run all six adapter families against recorded fixtures**

Run: `cd template && uv run pytest tests/adapters -v`

Expected: PASS with no real network access and contract coverage for all six adapter families.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/adapters template/tests/adapters template/tests/fixtures/adapters
git commit -m "feat: complete paper source adapters"
```

### Task 10: Multi-Adapter Fetch Orchestration and Cursor Persistence

**Files:**
- Create: `template/src/papers_pipeline/fetch.py`
- Test: `template/tests/test_fetch.py`

**Interfaces:**
- Consumes: `PipelineConfig`, `PipelineState`, `Adapter`, `Deadline`, `SourceRecord`
- Produces: `FetchStats`; `FetchResult(records: tuple[SourceRecord, ...], state: PipelineState, stats: tuple[FetchStats, ...], events: tuple[str, ...])`; `fetch_all(config, state, adapters, client_factory, now) -> FetchResult`

- [ ] **Step 1: Write failing source-window, continuation, and cap tests**

```python
# template/tests/test_fetch.py
@pytest.mark.asyncio
async def test_fetch_uses_source_lookback_and_persists_cursor(config, state, recording_adapter) -> None:
    result = await fetch_all(config, state, {"arxiv": recording_adapter}, client_factory, NOW)
    assert recording_adapter.windows == [FetchWindow(start=NOW - timedelta(days=7), end=NOW)]
    assert result.state.cursors["arxiv"] == "next-2"
    assert result.stats[0].capped is True
    assert result.stats[0].complete is False


@pytest.mark.asyncio
async def test_infrastructure_failure_aborts_all_fetches(config, failing_adapter) -> None:
    with pytest.raises(InfrastructureError, match="request retries exhausted"):
        await fetch_all(config, PipelineState(), {"arxiv": failing_adapter}, client_factory, NOW)
```

- [ ] **Step 2: Run tests and verify orchestration is missing**

Run: `cd template && uv run pytest tests/test_fetch.py -v`

Expected: FAIL on import from `papers_pipeline.fetch`.

- [ ] **Step 3: Implement sequential deadline-sharing orchestration**

```python
# template/src/papers_pipeline/fetch.py
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from .adapters.base import Adapter, FetchWindow
from .config import PipelineConfig
from .http import Deadline, RequestClient
from .models import PipelineState, SourceRecord


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
    cursors = dict(state.cursors)
    for adapter_config in config.adapters:
        if not adapter_config.enabled:
            continue
        adapter = adapters[adapter_config.name]
        window = FetchWindow(
            now - timedelta(days=adapter_config.lookback_days), now
        )
        cursor = cursors.get(adapter.name)
        source_records: list[SourceRecord] = []
        permanent_errors: list[str] = []
        pages = 0
        complete = False
        source_capped = False
        client = client_factory(deadline)
        while pages < adapter_config.max_pages:
            page = await adapter.fetch(
                window,
                cursor,
                client,
                adapter_config,
            )
            pages += 1
            remaining = adapter_config.max_results - len(source_records)
            source_records.extend(page.records[:remaining])
            permanent_errors.extend(page.permanent_errors)
            cursor = page.next_cursor
            source_capped = page.capped
            if cursor is None:
                complete = not page.capped
                break
            if len(source_records) >= adapter_config.max_results:
                source_capped = True
                break
        if cursor:
            cursors[adapter.name] = cursor
        elif complete:
            cursors.pop(adapter.name, None)
        records.extend(source_records)
        stats.append(FetchStats(
            adapter.name,
            len(source_records),
            len(permanent_errors),
            source_capped or (pages == adapter_config.max_pages and not complete),
            complete,
        ))
        events.extend(client.events)
    return FetchResult(
        tuple(records),
        state.model_copy(update={"cursors": cursors}),
        tuple(stats),
        tuple(events),
    )
```

- [ ] **Step 4: Run fetch tests**

Run: `cd template && uv run pytest tests/test_fetch.py -v`

Expected: PASS, proving source-specific windows, one shared deadline, deterministic adapter order, cap reporting, and cursor continuation.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/fetch.py template/tests/test_fetch.py
git commit -m "feat: orchestrate incremental source fetching"
```

### Task 11: Backlog Inference and Deterministic Bounded Batching

**Files:**
- Create: `template/src/papers_pipeline/batching.py`
- Test: `template/tests/test_batching.py`

**Interfaces:**
- Consumes: `Paper`, `ConversionConfig`
- Produces: `expected_markdown(root: Path, paper: Paper) -> Path`; `infer_backlog(papers, root) -> Backlog`; `select_batch(pending, config) -> Batch`

- [ ] **Step 1: Write failing backlog, fixme, count, and cost tests**

```python
# template/tests/test_batching.py
def test_backlog_is_csv_minus_outputs_and_fixmes(tmp_path, papers) -> None:
    output = expected_markdown(tmp_path, papers[0])
    output.parent.mkdir(parents=True)
    output.write_text("# complete")
    fixme = expected_markdown(tmp_path, papers[1]).with_suffix(".fixme.txt")
    fixme.write_text("manual repair required")
    result = infer_backlog(papers, tmp_path)
    assert result.generated == (papers[0],)
    assert result.blocked == (papers[1],)
    assert result.pending == tuple(papers[2:])


def test_batch_is_stable_and_bounded_by_count_and_cost(config, papers) -> None:
    batch = select_batch(reversed(papers), config)
    assert [paper.identifier for paper in batch.papers] == ["arxiv:1", "arxiv:2"]
    assert len(batch.papers) <= config.max_papers
    assert batch.estimated_cost <= config.max_cost
```

- [ ] **Step 2: Run tests and verify batching is missing**

Run: `cd template && uv run pytest tests/test_batching.py -v`

Expected: FAIL on missing module.

- [ ] **Step 3: Implement derived backlog and prefix selection**

```python
# template/src/papers_pipeline/batching.py
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
import re
from .config import ConversionConfig
from .models import Paper


@dataclass(frozen=True)
class Backlog:
    generated: tuple[Paper, ...]
    blocked: tuple[Paper, ...]
    pending: tuple[Paper, ...]


@dataclass(frozen=True)
class Batch:
    papers: tuple[Paper, ...]
    estimated_cost: int


def expected_markdown(root: Path, paper: Paper) -> Path:
    slug = re.sub(r"[^a-z0-9]+", "-", paper.identifier.casefold()).strip("-")
    return root / "papers" / f"{slug}.md"


def infer_backlog(papers: Iterable[Paper], root: Path) -> Backlog:
    generated: list[Paper] = []
    blocked: list[Paper] = []
    pending: list[Paper] = []
    for paper in sorted(papers, key=lambda item: item.identifier):
        output = expected_markdown(root, paper)
        if output.exists():
            generated.append(paper)
        elif output.with_suffix(".fixme.txt").exists():
            blocked.append(paper)
        else:
            pending.append(paper)
    return Backlog(tuple(generated), tuple(blocked), tuple(pending))


def select_batch(pending: Iterable[Paper], config: ConversionConfig) -> Batch:
    costs = {"html": config.html_cost, "latex": config.latex_cost, "pdf": config.pdf_cost}
    selected: list[Paper] = []
    total = 0
    for paper in sorted(pending, key=lambda item: item.identifier):
        cost = costs[paper.input_format]
        if len(selected) >= config.max_papers or total + cost > config.max_cost:
            break
        selected.append(paper)
        total += cost
    return Batch(tuple(selected), total)
```

- [ ] **Step 4: Run deterministic batching tests**

Run: `cd template && uv run pytest tests/test_batching.py -v`

Expected: PASS for count cap, cost cap, stable order, existing output, and `.fixme.txt` exclusion.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/batching.py template/tests/test_batching.py
git commit -m "feat: derive and bound conversion backlog"
```

### Task 12: Conversion Resource Classes and Failure Promotion

**Files:**
- Create: `template/src/papers_pipeline/convert.py`
- Create: `template/tests/fixtures/conversion/sample.html`
- Create: `template/tests/fixtures/conversion/sample.tex`
- Create: `template/tests/fixtures/conversion/sample.pdf`
- Test: `template/tests/test_convert.py`

**Interfaces:**
- Consumes: `Batch`, `PipelineState`, `ConcurrencyConfig`, `PaperError`, `InfrastructureError`
- Produces: `CommandRunner.run(argv: Sequence[str], timeout: float) -> CompletedProcess[str]`; `convert_batch(batch, root, state, concurrency, runner, now) -> ConversionResult`

- [ ] **Step 1: Write failing fixture, isolation, and exact PDF concurrency tests**

```python
# template/tests/test_convert.py
@pytest.mark.asyncio
async def test_pdf_concurrency_never_exceeds_one(batch_of_pdfs, state, tracking_runner, tmp_path) -> None:
    await convert_batch(batch_of_pdfs, tmp_path, state, CONCURRENCY, tracking_runner, NOW)
    assert tracking_runner.maximum_active["pdf"] == 1


@pytest.mark.asyncio
async def test_paper_failure_does_not_cancel_successful_peer(batch, state, mixed_runner, tmp_path) -> None:
    result = await convert_batch(batch, tmp_path, state, CONCURRENCY, mixed_runner, NOW)
    assert [item.paper.identifier for item in result.succeeded] == ["arxiv:1"]
    assert [item.paper.identifier for item in result.failed] == ["ss:2"]


@pytest.mark.asyncio
async def test_third_failure_writes_fixme_and_clears_counter(failing_batch, state_with_two_failures, runner, tmp_path) -> None:
    result = await convert_batch(failing_batch, tmp_path, state_with_two_failures, CONCURRENCY, runner, NOW)
    marker = expected_markdown(tmp_path, failing_batch.papers[0]).with_suffix(".fixme.txt")
    assert marker.read_text().count("attempt:") == 3
    assert failing_batch.papers[0].identifier not in result.state.failures
```

- [ ] **Step 2: Run tests and verify converter is missing**

Run: `cd template && uv run pytest tests/test_convert.py -v`

Expected: FAIL on import from `papers_pipeline.convert`.

- [ ] **Step 3: Implement injected subprocess conversion and semaphores**

```python
# template/src/papers_pipeline/convert.py
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import asyncio
import subprocess
from .batching import Batch, expected_markdown
from .config import ConcurrencyConfig
from .errors import InfrastructureError, PaperError
from .models import FailureAttempt, Paper, PipelineState


class CommandRunner:
    async def run(self, argv: Sequence[str], timeout: float) -> subprocess.CompletedProcess[str]:
        try:
            return await asyncio.to_thread(
                subprocess.run, argv, text=True, capture_output=True, check=True, timeout=timeout
            )
        except FileNotFoundError as error:
            raise InfrastructureError(f"missing conversion tool: {argv[0]}") from error
        except subprocess.TimeoutExpired as error:
            raise InfrastructureError(f"conversion infrastructure timeout: {argv[0]}") from error
        except subprocess.CalledProcessError as error:
            raise PaperError(error.stderr.strip() or f"{argv[0]} failed") from error


@dataclass(frozen=True)
class PaperConversion:
    paper: Paper
    output: Path | None
    error: str | None


@dataclass(frozen=True)
class ConversionResult:
    succeeded: tuple[PaperConversion, ...]
    failed: tuple[PaperConversion, ...]
    promoted: tuple[Path, ...]
    state: PipelineState


def command_for(paper: Paper, output: Path) -> list[str]:
    if paper.input_format in {"html", "latex"}:
        return ["pandoc", paper.input_url, "--to=gfm", f"--output={output}"]
    return ["marker_single", paper.input_url, "--output_dir", str(output.parent)]
```

Add this complete batch function. The PDF semaphore is constructed with `1`
unconditionally and the runtime assertion independently protects the schema:

```python
async def convert_batch(
    batch: Batch,
    root: Path,
    state: PipelineState,
    concurrency: ConcurrencyConfig,
    runner: CommandRunner,
    now: datetime,
) -> ConversionResult:
    if concurrency.pdf != 1:
        raise InfrastructureError("PDF concurrency must equal 1")
    semaphores = {
        "html": asyncio.Semaphore(concurrency.html),
        "latex": asyncio.Semaphore(concurrency.latex),
        "pdf": asyncio.Semaphore(1),
    }

    async def convert_one(paper: Paper) -> PaperConversion:
        output = expected_markdown(root, paper)
        output.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with semaphores[paper.input_format]:
                await runner.run(command_for(paper, output), timeout=900)
            if not output.exists():
                raise InfrastructureError(
                    f"converter reported success without output: {paper.identifier}"
                )
            return PaperConversion(paper, output, None)
        except PaperError as error:
            return PaperConversion(paper, None, str(error))

    raw_results = await asyncio.gather(
        *(convert_one(paper) for paper in batch.papers),
        return_exceptions=True,
    )
    for raw_result in raw_results:
        if isinstance(raw_result, InfrastructureError):
            raise raw_result
        if isinstance(raw_result, BaseException):
            raise InfrastructureError("unexpected converter task failure") from raw_result

    results = tuple(
        result
        for result in raw_results
        if isinstance(result, PaperConversion)
    )
    failures = {key: list(value) for key, value in state.failures.items()}
    promoted: list[Path] = []
    for result in results:
        identifier = result.paper.identifier
        if result.error is None:
            failures.pop(identifier, None)
            continue
        attempts = [
            *failures.get(identifier, []),
            FailureAttempt(occurred_at=now, error=result.error),
        ][-3:]
        if len(attempts) < 3:
            failures[identifier] = attempts
            continue
        marker = expected_markdown(root, result.paper).with_suffix(".fixme.txt")
        marker.write_text(
            "\n".join(
                [
                    f"paper: {identifier}",
                    f"latest_error: {result.error}",
                    *(
                        f"attempt: {attempt.occurred_at.isoformat()} {attempt.error}"
                        for attempt in attempts
                    ),
                ]
            )
            + "\n"
        )
        promoted.append(marker)
        failures.pop(identifier, None)

    return ConversionResult(
        succeeded=tuple(result for result in results if result.error is None),
        failed=tuple(result for result in results if result.error is not None),
        promoted=tuple(promoted),
        state=state.model_copy(update={"failures": failures}),
    )
```

The marker has these exact fields:

```text
paper: arxiv:2401.00001
latest_error: converter exited 1
attempt: 2026-09-21T02:00:00+00:00 converter exited 1
attempt: 2026-09-22T02:00:00+00:00 converter exited 1
attempt: 2026-09-23T02:00:00+00:00 converter exited 1
```

- [ ] **Step 4: Add tiny valid fixture files**

```html
<!-- template/tests/fixtures/conversion/sample.html -->
<html><body><main><h1>Fixture Paper</h1><p>Offline HTML.</p></main></body></html>
```

```tex
% template/tests/fixtures/conversion/sample.tex
\documentclass{article}
\begin{document}
\section{Fixture Paper}
Offline LaTeX.
\end{document}
```

Generate the PDF fixture with:

Run: `cd template && printf '%s\n' '%PDF-1.4' '1 0 obj<</Type/Catalog>>endobj' 'trailer<</Root 1 0 R>>' '%%EOF' > tests/fixtures/conversion/sample.pdf`

Expected: `file tests/fixtures/conversion/sample.pdf` reports PDF document data.

- [ ] **Step 5: Run conversion tests**

Run: `cd template && uv run pytest tests/test_convert.py -v`

Expected: PASS, proving bounded HTML/LaTeX concurrency, exactly-one PDF concurrency, isolated paper failures, third-failure promotion, and explicit infrastructure failure.

- [ ] **Step 6: Commit**

```bash
git add template/src/papers_pipeline/convert.py template/tests/test_convert.py template/tests/fixtures/conversion
git commit -m "feat: bound and isolate paper conversion"
```

### Task 13: Changed-File Formatting, Indexing, and Manual Sharding

**Files:**
- Create: `template/src/papers_pipeline/formatting.py`
- Create: `template/src/papers_pipeline/indexing.py`
- Test: `template/tests/test_formatting.py`

**Interfaces:**
- Consumes: successful `PaperConversion` outputs
- Produces: `format_changed(paths: Sequence[Path], runner: CommandRunner) -> None`; `shard_paths(paths: Sequence[Path], shard_index: int, shard_count: int) -> tuple[Path, ...]`; `write_index(root: Path, papers: Sequence[Paper]) -> Path`

- [ ] **Step 1: Write failing no-glob and deterministic shard tests**

```python
# template/tests/test_formatting.py
@pytest.mark.asyncio
async def test_format_changed_passes_exact_paths(recording_runner, tmp_path) -> None:
    changed = [tmp_path / "papers/a.md", tmp_path / "README.md"]
    await format_changed(changed, recording_runner)
    assert recording_runner.argv == ["prettier", "--write", str(changed[0]), str(changed[1])]
    assert "**" not in " ".join(recording_runner.argv)


def test_shards_are_stable_and_complete(paths) -> None:
    shards = [shard_paths(reversed(paths), index, 3) for index in range(3)]
    assert sorted(path for shard in shards for path in shard) == sorted(paths)
    assert set(shards[0]).isdisjoint(shards[1])
```

- [ ] **Step 2: Run tests and verify formatting module is missing**

Run: `cd template && uv run pytest tests/test_formatting.py -v`

Expected: FAIL on missing module.

- [ ] **Step 3: Implement exact-path formatting and modulo sharding**

```python
# template/src/papers_pipeline/formatting.py
from collections.abc import Sequence
from pathlib import Path
from .convert import CommandRunner


async def format_changed(paths: Sequence[Path], runner: CommandRunner) -> None:
    selected = sorted({path for path in paths if path.suffix in {".md", ".yml", ".yaml", ".json"}})
    if selected:
        await runner.run(["prettier", "--write", *(str(path) for path in selected)], timeout=300)


def shard_paths(paths: Sequence[Path], shard_index: int, shard_count: int) -> tuple[Path, ...]:
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must be within shard_count")
    ordered = sorted(set(paths))
    return tuple(path for index, path in enumerate(ordered) if index % shard_count == shard_index)
```

```python
# template/src/papers_pipeline/indexing.py
from collections.abc import Sequence
from pathlib import Path
from .models import Paper


def write_index(root: Path, papers: Sequence[Paper]) -> Path:
    path = root / "README.md"
    rows = ["# Papers", "", "| Date | Paper | Source |", "|---|---|---|"]
    rows.extend(
        f"| {paper.published.date()} | [{paper.title}]({paper.url}) | {paper.source} |"
        for paper in sorted(papers, key=lambda item: (item.published, item.identifier), reverse=True)
    )
    path.write_text("\n".join(rows) + "\n")
    return path
```

- [ ] **Step 4: Run formatting and index tests**

Run: `cd template && uv run pytest tests/test_formatting.py -v`

Expected: PASS and no nightly code path constructs a corpus glob.

- [ ] **Step 5: Commit**

```bash
git add template/src/papers_pipeline/formatting.py template/src/papers_pipeline/indexing.py template/tests/test_formatting.py
git commit -m "feat: format changed files in bounded shards"
```

### Task 14: Job Summaries, Git Commits, and End-to-End Pipeline

**Files:**
- Create: `template/src/papers_pipeline/summary.py`
- Create: `template/src/papers_pipeline/git.py`
- Create: `template/src/papers_pipeline/pipeline.py`
- Modify: `template/src/papers_pipeline/cli.py`
- Test: `template/tests/test_summary.py`
- Test: `template/tests/test_pipeline.py`

**Interfaces:**
- Consumes: all Task 2-13 interfaces
- Produces: `RunSummary.to_markdown() -> str`; `GitRepository.commit(paths: Sequence[Path], message: str) -> str | None`; `run_nightly(paths: PipelinePaths, dependencies: Dependencies) -> RunSummary`; CLI commands `nightly`, `format-corpus`

- [ ] **Step 1: Write failing summary and transaction-order tests**

```python
# template/tests/test_pipeline.py
@pytest.mark.asyncio
async def test_inventory_commit_precedes_batch_commit(dependencies, paths) -> None:
    summary = await run_nightly(paths, dependencies)
    assert dependencies.git.messages == [
        "chore: update paper inventory",
        "chore: convert paper batch 1",
    ]
    assert summary.inventory == 2
    assert summary.succeeded == 1
    assert summary.failed == 1


@pytest.mark.asyncio
async def test_infrastructure_failure_creates_no_batch_commit(dependencies, paths) -> None:
    dependencies.runner.fail_with(InfrastructureError("disk exhausted"))
    with pytest.raises(InfrastructureError, match="disk exhausted"):
        await run_nightly(paths, dependencies)
    assert "chore: convert paper batch 1" not in dependencies.git.messages
```

```python
# template/tests/test_summary.py
def test_summary_contains_required_counts_timings_and_events(summary) -> None:
    markdown = summary.to_markdown()
    for heading in ["Sources", "Inventory and conversion", "Timings", "Continuation and failures"]:
        assert f"## {heading}" in markdown
    assert "promoted_to_fixme" in markdown
    assert "deadline" in markdown
```

- [ ] **Step 2: Run tests and verify pipeline modules are missing**

Run: `cd template && uv run pytest tests/test_summary.py tests/test_pipeline.py -v`

Expected: FAIL on missing modules.

- [ ] **Step 3: Implement typed summary rendering**

```python
# template/src/papers_pipeline/summary.py
from dataclasses import dataclass, field
@dataclass(frozen=True)
class SourceCounts:
    source: str
    fetched: int
    accepted: int
    deduplicated: int
    rejected: int
    capped: bool
    complete: bool


@dataclass
class RunSummary:
    sources: tuple[SourceCounts, ...] = ()
    inventory: int = 0
    generated: int = 0
    pending: int = 0
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    promoted_to_fixme: int = 0
    timings: dict[str, float] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    fixme_paths: list[str] = field(default_factory=list)

    def to_markdown(self) -> str:
        source_rows = "\n".join(
            f"| {item.source} | {item.fetched} | {item.accepted} | {item.deduplicated} | {item.rejected} | {item.capped} | {item.complete} |"
            for item in self.sources
        )
        timing_rows = "\n".join(f"| {name} | {seconds:.3f} |" for name, seconds in sorted(self.timings.items()))
        return "\n".join([
            "# Papers pipeline summary",
            "## Sources", "| Source | Fetched | Accepted | Deduplicated | Rejected | Capped | Complete |", "|---|---:|---:|---:|---:|---|---|", source_rows,
            "## Inventory and conversion",
            f"- inventory: {self.inventory}\n- generated: {self.generated}\n- pending: {self.pending}\n- attempted: {self.attempted}\n- succeeded: {self.succeeded}\n- failed: {self.failed}\n- promoted_to_fixme: {self.promoted_to_fixme}",
            "## Timings", "| Stage | Seconds |", "|---|---:|", timing_rows,
            "## Continuation and failures", *(f"- {event}" for event in self.events), *(f"- fixme: {path}" for path in self.fixme_paths),
        ]) + "\n"
```

- [ ] **Step 4: Implement scoped Git commits**

```python
# template/src/papers_pipeline/git.py
from collections.abc import Sequence
from pathlib import Path
import subprocess


class GitRepository:
    def __init__(self, root: Path) -> None:
        self.root = root

    def commit(self, paths: Sequence[Path], message: str) -> str | None:
        relative = [str(path.relative_to(self.root)) for path in sorted(set(paths)) if path.exists()]
        if not relative:
            return None
        subprocess.run(["git", "add", "--", *relative], cwd=self.root, check=True)
        changed = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=self.root)
        if changed.returncode == 0:
            return None
        subprocess.run(["git", "commit", "-m", message], cwd=self.root, check=True)
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
```

- [ ] **Step 5: Implement pipeline order and CLI wiring**

`run_nightly` must perform this exact order:

```python
config = load_config(paths.config, dependencies.environ)
state = load_state(paths.state)
fetched = await fetch_all(config, state, dependencies.adapters, dependencies.client_factory, dependencies.now())
state = fetched.state
normalized = [normalize(record) for record in fetched.records]
accepted = [paper for paper in normalized if build_topic_gate(config.topic)(paper).accepted]
inventory = deduplicate([*read_inventory(paths.inventory), *accepted])
write_inventory(paths.inventory, inventory)
save_state(paths.state, state)
dependencies.git.commit([paths.inventory, paths.state], "chore: update paper inventory")
backlog = infer_backlog(inventory, paths.root)
batch_number = 0
attempted: set[str] = set()
while backlog.pending and batch_number < config.conversion.max_batches_per_run:
    eligible = tuple(
        paper for paper in backlog.pending if paper.identifier not in attempted
    )
    batch = select_batch(eligible, config.conversion)
    if not batch.papers:
        break
    batch_number += 1
    attempted.update(paper.identifier for paper in batch.papers)
    converted = await convert_batch(batch, paths.root, state, config.concurrency, dependencies.runner, dependencies.now())
    state = converted.state
    index = write_index(paths.root, inventory)
    changed = [item.output for item in converted.succeeded if item.output is not None]
    await format_changed([*changed, index], dependencies.runner)
    save_state(paths.state, state)
    dependencies.git.commit(
        [*changed, *converted.promoted, index, paths.state],
        f"chore: convert paper batch {batch_number}",
    )
    backlog = infer_backlog(inventory, paths.root)
```

Define the collaborating types exactly:

```python
# pipeline.py
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from .adapters.base import Adapter
from .convert import CommandRunner
from .git import GitRepository
from .http import Deadline, RequestClient


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    config: Path
    state: Path
    inventory: Path
    summary: Path | None


@dataclass(frozen=True)
class Dependencies:
    environ: Mapping[str, str]
    adapters: Mapping[str, Adapter]
    client_factory: Callable[[Deadline], RequestClient]
    runner: CommandRunner
    git: GitRepository
    now: Callable[[], datetime]
    monotonic: Callable[[], float]
```

Wrap each stage with `dependencies.monotonic()` and store elapsed seconds in
`RunSummary.timings`. Construct source events with this exact logic:

```python
source_counts: list[SourceCounts] = []
for item in fetched.stats:
    adapter = dependencies.adapters[item.source]
    source_records = [
        record
        for record in fetched.records
        if record.source in adapter.record_sources
    ]
    source_normalized = [normalize(record) for record in source_records]
    source_accepted = [
        paper
        for paper in source_normalized
        if build_topic_gate(config.topic)(paper).accepted
    ]
    selected_new = deduplicate(accepted)
    retained_keys = {
        (paper.identifier, paper.source)
        for paper in selected_new
    }
    retained = sum(
        (paper.identifier, paper.source) in retained_keys
        for paper in source_accepted
    )
    source_counts.append(
        SourceCounts(
            source=item.source,
            fetched=item.fetched,
            accepted=len(source_accepted),
            deduplicated=len(source_accepted) - retained,
            rejected=item.rejected + len(source_normalized) - len(source_accepted),
            capped=item.capped,
            complete=item.complete,
        )
    )
    if item.capped:
        summary.events.append(f"{item.source}: cap reached; continuation persisted")
    if item.rejected:
        summary.events.append(f"{item.source}: {item.rejected} permanent record failures")
summary.sources = tuple(source_counts)
summary.events.extend(fetched.events)
summary.inventory = len(inventory)
summary.generated = len(backlog.generated)
summary.pending = len(backlog.pending)
summary.attempted += len(batch.papers)
summary.succeeded += len(converted.succeeded)
summary.failed += len(converted.failed)
summary.promoted_to_fixme += len(converted.promoted)
summary.fixme_paths.extend(str(path) for path in converted.promoted)
```

Add one append-only summary writer:

```python
def write_actions_summary(path: Path | None, summary: RunSummary) -> None:
    if path is not None:
        with path.open("a") as handle:
            handle.write(summary.to_markdown())
```

Add this context manager and run the previously shown stage body inside it so
infrastructure failures are summarized and re-raised:

```python
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def report_infrastructure_failure(
    paths: PipelinePaths,
    summary: RunSummary,
) -> Iterator[None]:
    try:
        yield
    except InfrastructureError as error:
        summary.events.append(f"infrastructure failure: {error}")
        write_actions_summary(paths.summary, summary)
        raise
```

After successful stages, call `write_actions_summary(paths.summary, summary)`
once and return `summary`.

Extend `cli.py` with exact argument definitions:

```python
nightly = subparsers.add_parser("nightly")
nightly.add_argument("--config", type=Path, default=Path("papers.yml"))
format_corpus = subparsers.add_parser("format-corpus")
format_corpus.add_argument("--shard-index", type=int, required=True)
format_corpus.add_argument("--shard-count", type=int, required=True)
```

Use this exact CLI construction after parsing arguments:

```python
import asyncio
from datetime import datetime, timezone
import time
from .adapters import build_adapters
from .convert import CommandRunner
from .formatting import format_changed, shard_paths
from .git import GitRepository
from .http import RequestClient
from .pipeline import Dependencies, PipelinePaths, run_nightly


if args.command == "nightly":
    config = load_config(args.config, os.environ)
    root = args.config.resolve().parent
    summary_path = (
        Path(os.environ["GITHUB_STEP_SUMMARY"])
        if "GITHUB_STEP_SUMMARY" in os.environ
        else None
    )
    dependencies = Dependencies(
        environ=os.environ,
        adapters=build_adapters(config, os.environ),
        client_factory=lambda deadline: RequestClient(config.fetch, deadline),
        runner=CommandRunner(),
        git=GitRepository(root),
        now=lambda: datetime.now(timezone.utc),
        monotonic=time.monotonic,
    )
    asyncio.run(
        run_nightly(
            PipelinePaths(
                root=root,
                config=args.config,
                state=root / ".papers-state.yml",
                inventory=root / "papers.csv",
                summary=summary_path,
            ),
            dependencies,
        )
    )
elif args.command == "format-corpus":
    root = Path.cwd()
    selected = shard_paths(
        sorted((root / "papers").glob("*.md")),
        args.shard_index,
        args.shard_count,
    )
    asyncio.run(format_changed(selected, CommandRunner()))
```

- [ ] **Step 6: Run pipeline and summary tests**

Run: `cd template && uv run pytest tests/test_summary.py tests/test_pipeline.py -v`

Expected: PASS with inventory commit first, one commit per consistent batch, continuation after partial success, no batch commit after infrastructure failure, and all summary fields present.

- [ ] **Step 7: Commit**

```bash
git add template/src/papers_pipeline template/tests/test_summary.py template/tests/test_pipeline.py
git commit -m "feat: orchestrate transactional nightly pipeline"
```

### Task 15: CI, Nightly, and Manual Maintenance Workflows

**Files:**
- Create: `template/.github/workflows/ci.yml`
- Create: `template/.github/workflows/nightly.yml`
- Create: `template/.github/workflows/format-corpus.yml`
- Create: `template/.github/scripts/nightly.sh`
- Test: `template/tests/test_workflows.py`

**Interfaces:**
- Consumes: CLI commands from Task 14
- Produces: pull-request CI; scheduled/manual nightly mutation workflow; manual sharded formatting workflow

- [ ] **Step 1: Write failing workflow policy tests**

```python
# template/tests/test_workflows.py
from pathlib import Path
import yaml


def workflow(name: str) -> dict:
    return yaml.safe_load((Path(".github/workflows") / name).read_text())


def test_actions_are_sha_pinned_and_jobs_have_timeouts() -> None:
    for path in Path(".github/workflows").glob("*.yml"):
        data = yaml.safe_load(path.read_text())
        for job in data["jobs"].values():
            assert "timeout-minutes" in job
            for step in job["steps"]:
                if "uses" in step:
                    assert len(step["uses"].rsplit("@", 1)[1]) == 40


def test_nightly_has_single_mutation_concurrency_and_least_privilege() -> None:
    data = workflow("nightly.yml")
    assert data["concurrency"]["cancel-in-progress"] is False
    assert data["permissions"] == {"contents": "write"}
```

- [ ] **Step 2: Run tests and verify workflows are absent**

Run: `cd template && uv run pytest tests/test_workflows.py -v`

Expected: FAIL because workflow files do not exist.

- [ ] **Step 3: Add CI and nightly workflows with immutable actions**

```yaml
# template/.github/workflows/ci.yml
name: CI
on:
  pull_request:
  push:
    branches: [main]
permissions:
  contents: read
jobs:
  test:
    timeout-minutes: 20
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065
        with:
          python-version: "3.12"
      - run: pip install uv==0.8.17
      - run: uv sync --locked --extra dev
      - run: uv run pre-commit run --all-files
      - run: uv run pytest
```

```yaml
# template/.github/workflows/nightly.yml
name: Nightly papers
on:
  schedule:
    - cron: "17 2 * * *"
  workflow_dispatch:
permissions:
  contents: write
concurrency:
  group: nightly-papers
  cancel-in-progress: false
jobs:
  update:
    timeout-minutes: 120
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
        with:
          fetch-depth: 0
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065
        with:
          python-version: "3.12"
      - run: pip install uv==0.8.17
      - run: uv sync --locked --extra dev
      - run: papers-pipeline validate --config papers.yml
      - run: .github/scripts/nightly.sh
        timeout-minutes: 105
```

```bash
# template/.github/scripts/nightly.sh
#!/usr/bin/env bash
set -euo pipefail
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
status=0
uv run papers-pipeline nightly --config papers.yml || status=$?
git push origin HEAD:main
exit "$status"
```

- [ ] **Step 4: Add manual matrix sharding with no scheduled trigger**

```yaml
# template/.github/workflows/format-corpus.yml
name: Format complete corpus
on:
  workflow_dispatch:
    inputs:
      shard_count:
        description: Number of bounded formatting shards
        required: true
        default: "8"
permissions:
  contents: write
  pull-requests: write
concurrency:
  group: format-complete-corpus
  cancel-in-progress: false
jobs:
  plan:
    timeout-minutes: 5
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.matrix.outputs.matrix }}
    steps:
      - id: matrix
        env:
          SHARD_COUNT: ${{ inputs.shard_count }}
        run: |
          python -c 'import json, os; count=int(os.environ["SHARD_COUNT"]); assert 1 <= count <= 32; print("matrix=" + json.dumps({"include": [{"shard": index, "count": count} for index in range(count)]}))' >> "$GITHUB_OUTPUT"
  format:
    needs: plan
    timeout-minutes: 30
    strategy:
      fail-fast: false
      matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065
        with:
          python-version: "3.12"
      - run: pip install uv==0.8.17
      - run: uv sync --locked --extra dev
      - run: uv run papers-pipeline format-corpus --shard-index ${{ matrix.shard }} --shard-count ${{ matrix.count }}
      - run: git diff --binary > "format-${{ matrix.shard }}.patch"
      - uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02
        with:
          name: format-${{ matrix.shard }}
          path: format-${{ matrix.shard }}.patch
  combine:
    needs: format
    timeout-minutes: 15
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
      - uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093
        with:
          pattern: format-*
          path: patches
          merge-multiple: true
      - run: find patches -name '*.patch' -print0 | sort -z | xargs -0 -n1 git apply
      - uses: peter-evans/create-pull-request@22a9089034f40e5a961c8808d113e2c98fb63676
        with:
          branch: maintenance/format-corpus
          delete-branch: true
          title: "style: format complete paper corpus"
          commit-message: "style: format complete paper corpus"
```

- [ ] **Step 5: Run workflow policy tests**

Run: `cd template && uv run pytest tests/test_workflows.py -v`

Expected: PASS; every action is SHA-pinned, every job has a timeout, schedules cannot overlap, and whole-corpus formatting is manual-only.

- [ ] **Step 6: Commit**

```bash
git add template/.github template/tests/test_workflows.py
git commit -m "ci: add bounded papers workflows"
```

### Task 16: Validated Copier Update Pull Requests

**Files:**
- Create: `template/.github/workflows/template-update.yml`
- Create: `template/.github/scripts/template-update.sh`
- Modify: `template/tests/test_workflows.py`

**Interfaces:**
- Consumes: `.copier-answers.yml`, rendered pre-commit and offline tests
- Produces: scheduled/manual PR-only Copier update automation

- [ ] **Step 1: Add failing tests for PR-only template updates**

```python
# append to template/tests/test_workflows.py
def test_template_update_opens_pr_and_never_pushes_main() -> None:
    data = workflow("template-update.yml")
    assert data["permissions"] == {"contents": "write", "pull-requests": "write"}
    commands = "\n".join(
        step.get("run", "") for job in data["jobs"].values() for step in job["steps"]
    )
    assert "copier update" in commands
    assert "--vcs-ref" in commands
    assert "pre-commit run --all-files" in commands
    assert "pytest" in commands
    assert "git push origin main" not in commands
    assert any("peter-evans/create-pull-request@" in step.get("uses", "") for step in data["jobs"]["update"]["steps"])
```

- [ ] **Step 2: Run the test and verify workflow absence**

Run: `cd template && uv run pytest tests/test_workflows.py::test_template_update_opens_pr_and_never_pushes_main -v`

Expected: FAIL because `template-update.yml` does not exist.

- [ ] **Step 3: Add update validation script and workflow**

```bash
# template/.github/scripts/template-update.sh
#!/usr/bin/env bash
set -euo pipefail
template_ref="${TEMPLATE_REF:?TEMPLATE_REF must name an immutable template release}"
uv run copier update --vcs-ref "$template_ref" --defaults --trust
uv sync --locked --extra dev
uv run papers-pipeline validate --config papers.yml
uv run pre-commit run --all-files
uv run pytest
```

```yaml
# template/.github/workflows/template-update.yml
name: Update papers template
on:
  schedule:
    - cron: "43 6 * * 1"
  workflow_dispatch:
permissions:
  contents: write
  pull-requests: write
concurrency:
  group: template-update
  cancel-in-progress: false
jobs:
  update:
    timeout-minutes: 30
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
        with:
          fetch-depth: 0
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065
        with:
          python-version: "3.12"
      - run: pip install uv==0.8.17 copier==9.10.2
      - id: release
        env:
          GH_TOKEN: ${{ github.token }}
          TEMPLATE_REPOSITORY: will-rice/papers-template
        run: echo "ref=$(gh api "repos/$TEMPLATE_REPOSITORY/releases/latest" --jq .tag_name)" >> "$GITHUB_OUTPUT"
      - run: .github/scripts/template-update.sh
        env:
          TEMPLATE_REF: ${{ steps.release.outputs.ref }}
      - uses: peter-evans/create-pull-request@22a9089034f40e5a961c8808d113e2c98fb63676
        with:
          branch: automation/update-papers-template
          delete-branch: true
          title: "chore: update papers template"
          body: "Automated Copier update. CI validates schema, pre-commit, and the offline suite."
          commit-message: "chore: update papers template"
```

- [ ] **Step 4: Run workflow policy tests**

Run: `cd template && uv run pytest tests/test_workflows.py -v`

Expected: PASS and no update path pushes directly to `main`.

- [ ] **Step 5: Commit**

```bash
git add template/.github/workflows/template-update.yml template/.github/scripts/template-update.sh template/tests/test_workflows.py
git commit -m "ci: automate validated Copier update PRs"
```

### Task 17: Template Ownership Exclusions and Update Regression

**Files:**
- Create: `tests/test_template_exclusions.py`
- Modify: `copier.yml`

**Interfaces:**
- Consumes: Copier source and answers
- Produces: regression proof that update preserves `papers/`, `papers.csv`, `.papers-state.yml`, and caches

- [ ] **Step 1: Write an update-preservation test**

```python
# tests/test_template_exclusions.py
from pathlib import Path
import subprocess
from copier import run_copy, run_update


def test_copier_update_preserves_repository_data(tmp_path: Path) -> None:
    destination = tmp_path / "sample-papers"
    answers = {
        "project_name": "Sample Papers",
        "project_slug": "sample-papers",
        "topic_description": "sample topic",
        "template_version": "0.1.0",
    }
    run_copy(".", destination, data=answers, defaults=True, unsafe=True)
    subprocess.run(["git", "init"], cwd=destination, check=True)
    subprocess.run(["git", "config", "user.name", "Template Test"], cwd=destination, check=True)
    subprocess.run(["git", "config", "user.email", "template@example.test"], cwd=destination, check=True)
    subprocess.run(["git", "add", "."], cwd=destination, check=True)
    subprocess.run(["git", "commit", "-m", "render template"], cwd=destination, check=True)
    protected = {
        "papers/manual.md": "manual corpus",
        "papers.csv": "repository inventory",
        ".papers-state.yml": "cursors:\n  arxiv: next\nfailures: {}\n",
        ".cache/model.bin": "cache bytes",
    }
    for relative, content in protected.items():
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    subprocess.run(["git", "add", "."], cwd=destination, check=True)
    subprocess.run(["git", "commit", "-m", "add repository data"], cwd=destination, check=True)
    run_update(destination, defaults=True, unsafe=True)
    assert {name: (destination / name).read_text() for name in protected} == protected
```

- [ ] **Step 2: Run the test and capture any overwritten path**

Run: `uv run pytest tests/test_template_exclusions.py -v`

Expected: PASS. If it fails, the expected failure identifies the exact protected path still owned by Copier.

- [ ] **Step 3: Make exclusions update-safe**

Ensure `copier.yml` has this exact ownership block:

```yaml
_skip_if_exists:
  - papers
  - papers.csv
  - .papers-state.yml
  - .cache
  - .pytest_cache
  - .mypy_cache
  - .ruff_cache
```

`_skip_if_exists` renders the initial empty inventory and state files, then
preserves repository-owned content on updates. Do not add an `_exclude` entry
for these paths because `_exclude` would prevent their initial generation.

- [ ] **Step 4: Re-run ownership and render tests**

Run: `uv run pytest tests/test_template_exclusions.py tests/test_copier_smoke.py -v`

Expected: PASS with byte-identical protected files after update.

- [ ] **Step 5: Commit**

```bash
git add copier.yml tests/test_template_exclusions.py
git commit -m "test: preserve repository-owned paper data"
```

### Task 18: Documentation, Generated Smoke Test, and Final Validation

**Files:**
- Create: `README.md`
- Create: `template/README.md.jinja`
- Create: `scripts/smoke-test.sh`
- Modify: `tests/test_copier_smoke.py`

**Interfaces:**
- Consumes: complete Copier template
- Produces: documented generation, configuration, local operation, failure recovery, template updates, and follow-on migration gate; full generated-repository validation

- [ ] **Step 1: Extend the smoke test to run inside the generated repository**

```python
# append to tests/test_copier_smoke.py
import subprocess


def test_generated_repository_passes_offline_suite(tmp_path: Path) -> None:
    destination = tmp_path / "sample-papers"
    run_copy(
        ".",
        destination,
        data={
            "project_name": "Sample Papers",
            "project_slug": "sample-papers",
            "topic_description": "sample topic",
            "template_version": "0.1.0",
        },
        defaults=True,
        unsafe=True,
    )
    subprocess.run(["git", "init"], cwd=destination, check=True)
    subprocess.run(["git", "add", "."], cwd=destination, check=True)
    subprocess.run(["uv", "sync", "--locked", "--extra", "dev"], cwd=destination, check=True)
    subprocess.run(["uv", "run", "pre-commit", "run", "--all-files"], cwd=destination, check=True)
    subprocess.run(["uv", "run", "pytest"], cwd=destination, check=True)
```

- [ ] **Step 2: Run the smoke test and verify documentation is still absent**

Run: `uv run pytest tests/test_copier_smoke.py::test_generated_repository_passes_offline_suite -v`

Expected: PASS for generated code; repository documentation remains the only incomplete deliverable.

- [ ] **Step 3: Write source and generated documentation**

`README.md` must contain these exact sections:

```markdown
# Papers Template

Copier source for standalone, topic-focused `*-papers` repositories.

## Generate a repository

Run `uv run copier copy . ../example-papers`, answer the prompts, then run
`uv sync --locked --extra dev` in the generated repository.

## Validate the template

Run `uv run pytest`, which renders a sample repository and runs its pre-commit
hooks and complete offline test suite.

## Ownership boundary

Copier updates pipeline code, tests, workflows, and support files. It never
owns `papers/`, `papers.csv`, `.papers-state.yml`, or caches.

## Migration gate

Migrations of lipsync-papers, tts-papers, asr-papers, and birdclef-papers are
separate follow-on projects. Do not begin a migration until the generated
repository smoke test passes on the template release selected for migration.
```

Write this generated-repository guide:

````markdown
# {{ project_name }}

Standalone paper discovery and conversion for {{ topic_description }}.

## Configuration

`papers.yml` defines repository identity, enabled adapters, topic gates, fetch
policy, conversion budgets, and concurrency. Run
`uv run papers-pipeline validate --config papers.yml` before a manual run.

Adapter `lookback_days` is 1-365, `page_size` is 1-1000, `max_pages` is 1-100,
and `max_results` is 1-10000. Semantic Scholar reads the environment variable
named by `secret_env`; adapters without credentials use `secret_env: null`.
Adapter `filters` contains source query strings and, for
`biorxiv_crossref`, `provider: biorxiv` or `provider: crossref`.

Topic gates support `include_any`, `include_all`, `exclude_any`, and
`categories`. Set `plugin: topic_plugin:accept_topic` only when these gates
cannot express the repository rule; the plugin accepts `Paper` and returns
`TopicDecision`.

Fetch request timeouts are 1-120 seconds, retries are 0-5, backoff is 0-30
seconds, and the shared fetch deadline is 60-7200 seconds. Conversion allows
1-20 batches per run, 1-100 papers per batch, and a cost budget of 1-1000.
HTML and LaTeX concurrency is 1-4. PDF concurrency is always exactly 1.

## Run locally

```bash
uv sync --locked --extra dev
uv run papers-pipeline validate --config papers.yml
uv run papers-pipeline nightly --config papers.yml
uv run papers-pipeline format-corpus --shard-index 0 --shard-count 8
```

The nightly command derives backlog from `papers.csv` versus files under
`papers/`. `.papers-state.yml` stores only source cursors and consecutive
failure attempts. Count and cost budgets select deterministic batches.

A paper failure does not stop peers. A third consecutive scheduled failure
creates a colocated `.fixme.txt`; fix the input and remove the marker to retry.
Missing tools, resource exhaustion, deadlines, and other infrastructure errors
fail the run explicitly.

Nightly formatting receives only changed paper files and indexes. Complete
corpus formatting runs only through the manual sharded workflow.

## Automation

The nightly Actions summary reports per-source fetched, accepted,
deduplicated, and rejected counts; inventory, generated, pending, attempted,
succeeded, failed, and fixme counts; timings; continuation, cap, retry, and
deadline events; and fixme paths.

The weekly template workflow runs Copier against an explicit release, validates
the result, and opens a pull request. It never updates `main` directly.
`papers/`, `papers.csv`, `.papers-state.yml`, and caches remain
repository-owned across Copier updates.
````

- [ ] **Step 4: Add a reproducible smoke script**

```bash
# scripts/smoke-test.sh
#!/usr/bin/env bash
set -euo pipefail
uv sync --locked
uv run pytest tests/test_copier_smoke.py tests/test_template_exclusions.py -v
```

- [ ] **Step 5: Run targeted full validation**

Run: `./scripts/smoke-test.sh`

Expected: PASS; a temporary generated repository runs pre-commit and every offline test without network access.

- [ ] **Step 6: Run source checks and inspect generated ownership**

Run: `uv run pre-commit run --all-files && uv run pytest -v && git diff --check`

Expected: all hooks and tests PASS, and `git diff --check` produces no output.

- [ ] **Step 7: Verify specification coverage with explicit assertions**

Run:

```bash
test -f template/.github/workflows/nightly.yml
test -f template/.github/workflows/format-corpus.yml
test -f template/.github/workflows/template-update.yml
test "$(find template/src/papers_pipeline/adapters -name '*.py' | wc -l | tr -d ' ')" -ge 8
grep -R "Semaphore(1)" template/src/papers_pipeline/convert.py
! grep -R 'papers/\*\*/\*.md' template/.github/workflows/nightly.yml template/.github/scripts/nightly.sh
grep -R "create-pull-request@22a9089034f40e5a961c8808d113e2c98fb63676" template/.github/workflows/template-update.yml
```

Expected: every command exits 0; the only whole-corpus enumeration is behind the manual `format-corpus` CLI/workflow.

- [ ] **Step 8: Commit**

```bash
git add README.md template/README.md.jinja scripts/smoke-test.sh tests/test_copier_smoke.py
git commit -m "docs: complete papers template validation guide"
```

## Follow-On Plans

After Task 18's generated smoke test passes on a committed template release,
write four separate migration plans in this order:

1. fixture-repository adoption rehearsal;
2. `will-rice/lipsync-papers`;
3. `will-rice/tts-papers`;
4. `will-rice/asr-papers`;
5. `will-rice/birdclef-papers`.

Each real-repository plan must preserve corpus and Git history, add
`papers.yml`, `.papers-state.yml`, and `.copier-answers.yml`, leave scheduling
disabled, run and inspect the manual workflow, verify continuation, and only
then re-enable the schedule. None of those migrations is part of this
implementation plan.
