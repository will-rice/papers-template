from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from papers_pipeline.batching import Batch, expected_markdown
from papers_pipeline.config import ConcurrencyConfig
from papers_pipeline.errors import InfrastructureError, PaperError
from papers_pipeline.models import FailureAttempt, Paper, PipelineState
from papers_pipeline.convert import CommandRunner, convert_batch

NOW = datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures" / "conversion"
CONCURRENCY = ConcurrencyConfig(html=2, latex=1, pdf=1)


def paper(identifier: str, *, input_format: str) -> Paper:
    source = identifier.split(":", 1)[0]
    fixture_name = {"html": "sample.html", "latex": "sample.tex", "pdf": "sample.pdf"}[
        input_format
    ]
    fixture = FIXTURES / fixture_name
    return Paper(
        identifier=identifier,
        title=f"{identifier} title",
        abstract=f"{identifier} abstract",
        authors=("A. Author",),
        published=datetime(2024, 1, 2, tzinfo=timezone.utc),
        url=f"https://example.test/{identifier}",
        source=source,
        input_format=input_format,  # type: ignore[arg-type]
        input_url=f"{fixture.resolve().as_uri()}?paper={identifier}",
        categories=("cs.CL",),
    )


class TrackingRunner(CommandRunner):
    def __init__(
        self,
        *,
        outputs: Mapping[str, Path],
        behaviors: Mapping[str, str] | None = None,
        delay: float = 0.01,
    ) -> None:
        self.outputs = dict(outputs)
        self.behaviors = dict(behaviors or {})
        self.delay = delay
        self.active = {"html": 0, "latex": 0, "pdf": 0}
        self.maximum_active = {"html": 0, "latex": 0, "pdf": 0}

    async def run(
        self, argv: Sequence[str], timeout: float
    ) -> subprocess.CompletedProcess[str]:
        assert timeout == 900
        kind = _kind_for(argv)
        input_url = argv[1]
        self.active[kind] += 1
        self.maximum_active[kind] = max(self.maximum_active[kind], self.active[kind])
        try:
            await asyncio.sleep(self.delay)
            behavior = self.behaviors.get(input_url, "success")
            if behavior == "paper_error":
                raise PaperError("converter exited 1")
            if behavior == "infra_error":
                raise InfrastructureError("missing conversion tool: pandoc")
            if behavior == "unexpected":
                raise RuntimeError("boom")
            if behavior != "no_output":
                output = self.outputs[input_url]
                output.write_text(
                    f"# converted {Path(urlsplit(input_url).path).suffix}\n",
                    encoding="utf-8",
                )
            return subprocess.CompletedProcess(args=list(argv), returncode=0, stdout="", stderr="")
        finally:
            self.active[kind] -= 1


@pytest.fixture
def state() -> PipelineState:
    return PipelineState()


@pytest.mark.asyncio
async def test_command_runner_returns_completed_process() -> None:
    result = await CommandRunner().run(
        [sys.executable, "-c", "print('ok')"],
        timeout=5,
    )

    assert result.stdout.strip() == "ok"
    assert result.returncode == 0


@pytest.mark.asyncio
async def test_command_runner_maps_missing_tool_to_infrastructure_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_tool(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(subprocess, "run", missing_tool)

    with pytest.raises(InfrastructureError, match="missing conversion tool: pandoc"):
        await CommandRunner().run(["pandoc", "input.html"], timeout=5)


@pytest.mark.asyncio
async def test_command_runner_maps_timeout_to_infrastructure_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timeout(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["pandoc", "input.html"], timeout=5)

    monkeypatch.setattr(subprocess, "run", timeout)

    with pytest.raises(
        InfrastructureError,
        match="conversion infrastructure timeout: pandoc",
    ):
        await CommandRunner().run(["pandoc", "input.html"], timeout=5)


@pytest.mark.asyncio
async def test_command_runner_maps_called_process_error_to_paper_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def bad_paper(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            returncode=1,
            cmd=["pandoc", "input.html"],
            stderr="converter exited 1",
        )

    monkeypatch.setattr(subprocess, "run", bad_paper)

    with pytest.raises(PaperError, match="converter exited 1"):
        await CommandRunner().run(["pandoc", "input.html"], timeout=5)


@pytest.mark.asyncio
async def test_html_and_latex_concurrency_are_bounded_independently(
    tmp_path: Path, state: PipelineState
) -> None:
    batch = Batch(
        papers=(
            paper("arxiv:1", input_format="html"),
            paper("arxiv:2", input_format="html"),
            paper("arxiv:3", input_format="html"),
            paper("ss:4", input_format="latex"),
            paper("ss:5", input_format="latex"),
        ),
        estimated_cost=5,
    )
    runner = TrackingRunner(
        outputs={
            item.input_url: expected_markdown(tmp_path, item)
            for item in batch.papers
        }
    )

    await convert_batch(batch, tmp_path, state, CONCURRENCY, runner, NOW)

    assert runner.maximum_active == {"html": 2, "latex": 1, "pdf": 0}


@pytest.mark.asyncio
async def test_pdf_concurrency_never_exceeds_one(tmp_path: Path, state: PipelineState) -> None:
    batch = Batch(
        papers=(
            paper("arxiv:1", input_format="pdf"),
            paper("arxiv:2", input_format="pdf"),
            paper("arxiv:3", input_format="pdf"),
        ),
        estimated_cost=3,
    )
    runner = TrackingRunner(
        outputs={
            item.input_url: expected_markdown(tmp_path, item)
            for item in batch.papers
        }
    )

    await convert_batch(batch, tmp_path, state, CONCURRENCY, runner, NOW)

    assert runner.maximum_active["pdf"] == 1


@pytest.mark.asyncio
async def test_pdf_runtime_config_must_equal_one(tmp_path: Path, state: PipelineState) -> None:
    batch = Batch(papers=(paper("arxiv:1", input_format="pdf"),), estimated_cost=1)
    runner = TrackingRunner(
        outputs={batch.papers[0].input_url: expected_markdown(tmp_path, batch.papers[0])}
    )

    with pytest.raises(InfrastructureError, match="PDF concurrency must equal 1"):
        await convert_batch(
            batch,
            tmp_path,
            state,
            ConcurrencyConfig.model_construct(html=1, latex=1, pdf=2),
            runner,
            NOW,
        )


@pytest.mark.asyncio
async def test_paper_failure_does_not_cancel_successful_peer(
    tmp_path: Path, state: PipelineState
) -> None:
    batch = Batch(
        papers=(
            paper("arxiv:1", input_format="html"),
            paper("ss:2", input_format="pdf"),
        ),
        estimated_cost=2,
    )
    runner = TrackingRunner(
        outputs={
            item.input_url: expected_markdown(tmp_path, item)
            for item in batch.papers
        },
        behaviors={batch.papers[1].input_url: "paper_error"},
    )

    result = await convert_batch(batch, tmp_path, state, CONCURRENCY, runner, NOW)

    assert [item.paper.identifier for item in result.succeeded] == ["arxiv:1"]
    assert [item.paper.identifier for item in result.failed] == ["ss:2"]
    assert result.promoted == ()


@pytest.mark.asyncio
async def test_failures_accumulate_consecutively_until_success_clears_history(
    tmp_path: Path,
) -> None:
    target = paper("ss:2", input_format="pdf")
    batch = Batch(papers=(target,), estimated_cost=1)
    first = await convert_batch(
        batch,
        tmp_path,
        PipelineState(),
        CONCURRENCY,
        TrackingRunner(
            outputs={target.input_url: expected_markdown(tmp_path, target)},
            behaviors={target.input_url: "paper_error"},
        ),
        NOW,
    )
    second = await convert_batch(
        batch,
        tmp_path,
        first.state,
        CONCURRENCY,
        TrackingRunner(
            outputs={target.input_url: expected_markdown(tmp_path, target)},
            behaviors={target.input_url: "paper_error"},
        ),
        NOW.replace(day=24),
    )
    third = await convert_batch(
        batch,
        tmp_path,
        second.state,
        CONCURRENCY,
        TrackingRunner(outputs={target.input_url: expected_markdown(tmp_path, target)}),
        NOW.replace(day=25),
    )

    assert [attempt.error for attempt in first.state.failures[target.identifier]] == [
        "converter exited 1"
    ]
    assert [attempt.error for attempt in second.state.failures[target.identifier]] == [
        "converter exited 1",
        "converter exited 1",
    ]
    assert third.state.failures == {}


@pytest.mark.asyncio
async def test_third_failure_writes_fixme_atomically_and_clears_counter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = paper("arxiv:2401.00001", input_format="pdf")
    batch = Batch(papers=(target,), estimated_cost=1)
    prior_state = PipelineState(
        failures={
            target.identifier: [
                FailureAttempt(occurred_at=NOW.replace(day=21), error="converter exited 1"),
                FailureAttempt(occurred_at=NOW.replace(day=22), error="converter exited 1"),
            ]
        }
    )
    marker = expected_markdown(tmp_path, target).with_suffix(".fixme.txt")
    observed_temp_paths: list[Path] = []
    original_replace = os.replace

    def spy_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        temp_path = Path(src)
        observed_temp_paths.append(temp_path)
        assert temp_path.exists()
        assert Path(dst) == marker
        assert temp_path.read_text(encoding="utf-8") == (
            "paper: arxiv:2401.00001\n"
            "latest_error: converter exited 1\n"
            "attempt: 2026-09-21T02:00:00+00:00 converter exited 1\n"
            "attempt: 2026-09-22T02:00:00+00:00 converter exited 1\n"
            "attempt: 2026-09-23T02:00:00+00:00 converter exited 1\n"
        )
        original_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy_replace)

    result = await convert_batch(
        batch,
        tmp_path,
        prior_state,
        CONCURRENCY,
        TrackingRunner(
            outputs={target.input_url: expected_markdown(tmp_path, target)},
            behaviors={target.input_url: "paper_error"},
        ),
        NOW,
    )

    assert marker.read_text(encoding="utf-8").count("attempt:") == 3
    assert result.promoted == (marker,)
    assert result.state.failures == {}
    assert observed_temp_paths == [marker.with_name(f"{marker.name}.tmp")]


@pytest.mark.asyncio
async def test_converter_must_materialize_output(tmp_path: Path, state: PipelineState) -> None:
    target = paper("arxiv:1", input_format="html")
    batch = Batch(papers=(target,), estimated_cost=1)

    with pytest.raises(
        InfrastructureError,
        match="converter reported success without output: arxiv:1",
    ):
        await convert_batch(
            batch,
            tmp_path,
            state,
            CONCURRENCY,
            TrackingRunner(
                outputs={target.input_url: expected_markdown(tmp_path, target)},
                behaviors={target.input_url: "no_output"},
            ),
            NOW,
        )


@pytest.mark.asyncio
async def test_infrastructure_failure_is_reraised_without_promoting_or_mutating_state(
    tmp_path: Path,
) -> None:
    flaky = paper("arxiv:1", input_format="html")
    aborted = paper("ss:2", input_format="pdf")
    batch = Batch(papers=(flaky, aborted), estimated_cost=2)
    state = PipelineState(
        failures={
            flaky.identifier: [
                FailureAttempt(occurred_at=NOW.replace(day=21), error="converter exited 1"),
                FailureAttempt(occurred_at=NOW.replace(day=22), error="converter exited 1"),
            ]
        }
    )

    with pytest.raises(InfrastructureError, match="missing conversion tool: pandoc"):
        await convert_batch(
            batch,
            tmp_path,
            state,
            CONCURRENCY,
            TrackingRunner(
                outputs={
                    item.input_url: expected_markdown(tmp_path, item)
                    for item in batch.papers
                },
                behaviors={
                    flaky.input_url: "paper_error",
                    aborted.input_url: "infra_error",
                },
            ),
            NOW,
        )

    assert state.failures[flaky.identifier] == [
        FailureAttempt(occurred_at=NOW.replace(day=21), error="converter exited 1"),
        FailureAttempt(occurred_at=NOW.replace(day=22), error="converter exited 1"),
    ]
    assert expected_markdown(tmp_path, flaky).with_suffix(".fixme.txt").exists() is False


@pytest.mark.asyncio
async def test_unexpected_task_failure_is_wrapped_as_infrastructure_error(
    tmp_path: Path, state: PipelineState
) -> None:
    target = paper("arxiv:1", input_format="html")
    batch = Batch(papers=(target,), estimated_cost=1)

    with pytest.raises(
        InfrastructureError,
        match="unexpected converter task failure",
    ):
        await convert_batch(
            batch,
            tmp_path,
            state,
            CONCURRENCY,
            TrackingRunner(
                outputs={target.input_url: expected_markdown(tmp_path, target)},
                behaviors={target.input_url: "unexpected"},
            ),
            NOW,
        )


def _kind_for(argv: Sequence[str]) -> str:
    if argv[0] == "marker_single":
        return "pdf"
    if Path(urlsplit(argv[1]).path).suffix == ".html":
        return "html"
    return "latex"
