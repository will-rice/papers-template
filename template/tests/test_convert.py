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
import httpx

from papers_pipeline.batching import Batch, expected_markdown, infer_backlog
from papers_pipeline.adapters.arxiv import ArxivAdapter
from papers_pipeline.adapters.base import FetchWindow
from papers_pipeline.config import AdapterConfig, ConcurrencyConfig
from papers_pipeline.convert import (
    CommandRunner,
    DownloadingMaterializer,
    MaterializedInput,
    convert_batch,
    _download_bytes,
)
from papers_pipeline.errors import InfrastructureError, PaperError
from papers_pipeline.http import RequestClient
from papers_pipeline.models import FailureAttempt, Paper, PipelineState

NOW = datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures" / "conversion"
CONCURRENCY = ConcurrencyConfig(html=2, latex=1, pdf=1)


def paper(identifier: str, *, input_format: str) -> Paper:
    source = identifier.split(":", 1)[0]
    extension = {"html": "html", "latex": "tex", "pdf": "pdf"}[input_format]
    return Paper(
        identifier=identifier,
        title=f"{identifier} title",
        abstract=f"{identifier} abstract",
        authors=("A. Author",),
        published=datetime(2024, 1, 2, tzinfo=timezone.utc),
        url=f"https://example.test/{identifier}",
        source=source,
        input_format=input_format,  # type: ignore[arg-type]
        input_url=f"https://example.test/inputs/{identifier.replace(':', '-')}.{extension}",
        categories=("cs.CL",),
    )


def fixture_for(paper: Paper) -> Path:
    name = {"html": "sample.html", "latex": "sample.tex", "pdf": "sample.pdf"}[
        paper.input_format
    ]
    return FIXTURES / name


class FakeMaterializer:
    def __init__(
        self,
        *,
        fixtures: Mapping[str, Path],
        behaviors: Mapping[str, str] | None = None,
    ) -> None:
        self.fixtures = dict(fixtures)
        self.behaviors = dict(behaviors or {})
        self.materialized_urls: dict[Path, str] = {}

    async def materialize(self, paper: Paper, root: Path) -> MaterializedInput:
        behavior = self.behaviors.get(paper.input_url, "success")
        if behavior == "infra_error":
            raise InfrastructureError(
                f"conversion input request failed: {paper.input_url}"
            )
        if behavior == "disk_error":
            raise InfrastructureError(
                f"conversion input cache write failed: {paper.input_url}"
            )
        if behavior == "paper_error":
            raise PaperError(f"conversion input HTTP 404: {paper.input_url}")

        source = self.fixtures[paper.input_url]
        local_path = (
            root
            / "inputs"
            / f"{paper.identifier.replace(':', '-').replace('/', '-')}{source.suffix}"
        )
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(source.read_bytes())
        self.materialized_urls[local_path] = paper.input_url
        return MaterializedInput(local_path=local_path)

    def lookup(self, local_path: Path) -> str:
        return self.materialized_urls[local_path]


class TrackingRunner(CommandRunner):
    def __init__(
        self,
        *,
        materializer: FakeMaterializer,
        behaviors: Mapping[str, str] | None = None,
        delay: float = 0.01,
        delays: Mapping[str, float] | None = None,
    ) -> None:
        self.materializer = materializer
        self.behaviors = dict(behaviors or {})
        self.delay = delay
        self.delays = dict(delays or {})
        self.active = {"html": 0, "latex": 0, "pdf": 0}
        self.maximum_active = {"html": 0, "latex": 0, "pdf": 0}

    async def run(
        self, argv: Sequence[str], timeout: float
    ) -> subprocess.CompletedProcess[str]:
        assert timeout == 900
        kind = _kind_for(argv)
        input_path = Path(argv[1])
        assert input_path.is_absolute()
        assert "://" not in argv[1]
        original_url = self.materializer.lookup(input_path)
        behavior = self.behaviors.get(original_url, "success")
        self.active[kind] += 1
        self.maximum_active[kind] = max(self.maximum_active[kind], self.active[kind])
        try:
            await asyncio.sleep(self.delays.get(original_url, self.delay))
            if behavior == "paper_error":
                raise PaperError("converter exited 1")
            if behavior == "infra_error":
                raise InfrastructureError("missing conversion tool: pandoc")
            if behavior == "unexpected":
                raise RuntimeError("boom")
            if behavior != "no_output":
                _write_converter_output(argv, input_path)
            return subprocess.CompletedProcess(
                args=list(argv), returncode=0, stdout="", stderr=""
            )
        finally:
            self.active[kind] -= 1


@pytest.fixture
def state() -> PipelineState:
    return PipelineState()


@pytest.fixture
def materializer() -> FakeMaterializer:
    papers = (
        paper("arxiv:1", input_format="html"),
        paper("arxiv:2", input_format="html"),
        paper("arxiv:3", input_format="html"),
        paper("ss:2", input_format="pdf"),
        paper("ss:4", input_format="latex"),
        paper("ss:5", input_format="latex"),
        paper("arxiv:2401.00001", input_format="pdf"),
    )
    return FakeMaterializer(
        fixtures={item.input_url: fixture_for(item) for item in papers}
    )


@pytest.mark.asyncio
async def test_command_runner_returns_completed_process() -> None:
    result = await CommandRunner().run(
        [sys.executable, "-c", "print('ok')"],
        timeout=5,
    )

    assert result.stdout.strip() == "ok"
    assert result.returncode == 0


@pytest.mark.asyncio
async def test_command_runner_maps_missing_tool_to_infrastructure_error() -> None:
    with pytest.raises(
        InfrastructureError,
        match="missing conversion tool: __missing_pandoc__",
    ):
        await CommandRunner().run(["__missing_pandoc__", "input.html"], timeout=5)


@pytest.mark.asyncio
async def test_command_runner_timeout_terminates_child_and_preserves_output(
    tmp_path: Path,
) -> None:
    pid_file = tmp_path / "timeout.pid"
    child = _sleeping_child_script(pid_file)

    with pytest.raises(
        InfrastructureError,
        match="conversion infrastructure timeout:",
    ) as exc_info:
        await CommandRunner().run([sys.executable, "-c", child], timeout=0.1)

    pid = int((await _wait_for_file(pid_file)).strip())
    await _assert_process_gone(pid)
    timeout_error = exc_info.value.__cause__
    assert isinstance(timeout_error, subprocess.TimeoutExpired)
    assert isinstance(timeout_error.output, str)
    assert isinstance(timeout_error.stderr, str)
    assert timeout_error.output == "ready\n"
    assert timeout_error.stderr == "waiting\n"


@pytest.mark.asyncio
async def test_command_runner_cancellation_terminates_child(
    tmp_path: Path,
) -> None:
    pid_file = tmp_path / "cancel.pid"
    child = _sleeping_child_script(pid_file)
    task = asyncio.create_task(
        CommandRunner().run([sys.executable, "-c", child], timeout=5)
    )

    pid = int((await _wait_for_file(pid_file)).strip())
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    await _assert_process_gone(pid)


@pytest.mark.asyncio
async def test_command_runner_isolates_recognized_corrupt_document_failure() -> None:
    with pytest.raises(PaperError, match="corrupt input document") as exc_info:
        await CommandRunner().run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "print('partial output'); "
                    "print('corrupt input document', file=sys.stderr); "
                    "raise SystemExit(1)"
                ),
            ],
            timeout=5,
        )

    process_error = exc_info.value.__cause__
    assert isinstance(process_error, subprocess.CalledProcessError)
    assert isinstance(process_error.output, str)
    assert isinstance(process_error.stderr, str)
    assert process_error.output == "partial output\n"
    assert process_error.stderr == "corrupt input document\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [-15, 137, 143])
async def test_command_runner_maps_termination_exits_to_infrastructure_error(
    returncode: int,
) -> None:
    if returncode < 0:
        script = "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"
    else:
        script = f"raise SystemExit({returncode})"

    with pytest.raises(InfrastructureError, match="conversion infrastructure failure"):
        await CommandRunner().run([sys.executable, "-c", script], timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "stream"),
    [
        ("CUDA out of memory", "stdout"),
        ("No space left on device", "stderr"),
        ("failed to initialize model", "stderr"),
        ("error loading model weights", "stderr"),
        ("cache directory is not writable", "stderr"),
        ("PyTorch shared library could not be loaded", "stderr"),
        ("libcudnn.so: cannot open shared object file", "stderr"),
        ("unclassified converter failure", "stderr"),
    ],
)
async def test_command_runner_fails_safe_for_infrastructure_and_unknown_errors(
    message: str,
    stream: str,
) -> None:
    destination = "" if stream == "stdout" else ", file=sys.stderr"
    script = (
        "import sys; "
        f"print({message!r}{destination}); "
        "raise SystemExit(1)"
    )

    with pytest.raises(InfrastructureError, match="conversion infrastructure failure"):
        await CommandRunner().run([sys.executable, "-c", script], timeout=5)


@pytest.mark.asyncio
async def test_downloading_materializer_materializes_remote_input_to_local_file(
    tmp_path: Path,
) -> None:
    target = paper("arxiv:1", input_format="html")

    async def downloader(url: str, timeout: float) -> bytes:
        assert url == target.input_url
        assert timeout == 900
        return b"<html><body>offline</body></html>"

    result = await DownloadingMaterializer(downloader=downloader).materialize(
        target, tmp_path
    )

    assert (
        result.local_path.read_text(encoding="utf-8")
        == "<html><body>offline</body></html>"
    )
    assert result.local_path.is_absolute()
    assert result.local_path.parent == tmp_path / "inputs"
    assert result.cleanup_paths == (result.local_path,)


@pytest.mark.asyncio
async def test_arxiv_adapter_pdf_url_downloads_without_redirect(
    arxiv_client: RequestClient,
    arxiv_config: AdapterConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = await ArxivAdapter().fetch(
        FetchWindow(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime(2024, 1, 8, tzinfo=timezone.utc),
        ),
        None,
        arxiv_client,
        arxiv_config,
    )
    adapter_url = page.records[0].input_url

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == adapter_url:
            return httpx.Response(200, content=b"%PDF fixture")
        if str(request.url) == f"{adapter_url}.pdf":
            return httpx.Response(302, headers={"location": adapter_url})
        raise AssertionError(f"unexpected URL: {request.url}")

    transport = httpx.MockTransport(handler)
    original_client = httpx.AsyncClient
    monkeypatch.setattr(
        "papers_pipeline.convert.httpx.AsyncClient",
        lambda **kwargs: original_client(transport=transport, **kwargs),
    )

    assert await _download_bytes(adapter_url, 1) == b"%PDF fixture"
    with pytest.raises(InfrastructureError, match="redirect HTTP 302"):
        await _download_bytes(f"{adapter_url}.pdf", 1)


@pytest.mark.asyncio
async def test_downloading_materializer_maps_disk_errors_to_infrastructure_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = paper("arxiv:1", input_format="html")

    async def downloader(url: str, timeout: float) -> bytes:
        return b"fixture"

    def broken_write(self: Path, data: bytes, *args: object, **kwargs: object) -> int:
        raise OSError("disk full")

    monkeypatch.setattr(Path, "write_bytes", broken_write)

    with pytest.raises(
        InfrastructureError,
        match=f"conversion input cache write failed: {target.input_url}",
    ):
        await DownloadingMaterializer(downloader=downloader).materialize(
            target, tmp_path
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 404, 410, 422])
async def test_permanent_download_http_errors_are_paper_errors(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    class Client:
        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def get(self, url: str, timeout: float) -> object:
            return type(
                "Response",
                (),
                {
                    "status_code": status_code,
                    "is_error": True,
                    "content": b"",
                },
            )()

    monkeypatch.setattr(
        "papers_pipeline.convert.httpx.AsyncClient", lambda **_: Client()
    )

    with pytest.raises(PaperError, match=f"HTTP {status_code}"):
        await _download_bytes("https://example.test/missing.pdf", 1)


@pytest.mark.asyncio
async def test_invalid_per_paper_input_url_is_a_paper_error(tmp_path: Path) -> None:
    target = paper("arxiv:invalid", input_format="pdf").model_copy(
        update={"input_url": "ftp://example.test/paper.pdf"}
    )

    with pytest.raises(PaperError, match="unsupported conversion input URL"):
        await DownloadingMaterializer().materialize(target, tmp_path)


@pytest.mark.asyncio
async def test_permanent_download_failure_does_not_cancel_batch_peer(
    tmp_path: Path,
) -> None:
    missing = paper("arxiv:missing", input_format="html")
    valid = paper("arxiv:valid", input_format="html")
    batch = Batch(papers=(missing, valid), estimated_cost=2)
    fake_materializer = FakeMaterializer(
        fixtures={valid.input_url: fixture_for(valid)},
        behaviors={missing.input_url: "paper_error"},
    )

    result = await convert_batch(
        batch,
        tmp_path,
        PipelineState(),
        CONCURRENCY,
        TrackingRunner(materializer=fake_materializer),
        fake_materializer,
        NOW,
    )

    assert [item.paper.identifier for item in result.failed] == [missing.identifier]
    assert [item.paper.identifier for item in result.succeeded] == [valid.identifier]
    assert missing.identifier in result.state.failures


@pytest.mark.asyncio
async def test_third_permanent_download_failure_writes_fixme(tmp_path: Path) -> None:
    target = paper("arxiv:missing", input_format="html")
    attempts = [
        FailureAttempt(occurred_at=NOW.replace(day=21), error="HTTP 404"),
        FailureAttempt(occurred_at=NOW.replace(day=22), error="HTTP 404"),
    ]
    fake_materializer = FakeMaterializer(
        fixtures={},
        behaviors={target.input_url: "paper_error"},
    )

    result = await convert_batch(
        Batch(papers=(target,), estimated_cost=1),
        tmp_path,
        PipelineState(failures={target.identifier: attempts}),
        CONCURRENCY,
        TrackingRunner(materializer=fake_materializer),
        fake_materializer,
        NOW,
    )

    marker = expected_markdown(tmp_path, target).with_suffix(".fixme.txt")
    assert marker in result.promoted
    assert target.identifier not in result.state.failures


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
    fake_materializer = FakeMaterializer(
        fixtures={item.input_url: fixture_for(item) for item in batch.papers}
    )
    runner = TrackingRunner(materializer=fake_materializer)

    await convert_batch(
        batch, tmp_path, state, CONCURRENCY, runner, fake_materializer, NOW
    )

    assert runner.maximum_active == {"html": 2, "latex": 1, "pdf": 0}


@pytest.mark.asyncio
async def test_pdf_concurrency_never_exceeds_one(
    tmp_path: Path, state: PipelineState
) -> None:
    batch = Batch(
        papers=(
            paper("arxiv:1", input_format="pdf"),
            paper("arxiv:2", input_format="pdf"),
            paper("arxiv:3", input_format="pdf"),
        ),
        estimated_cost=3,
    )
    fake_materializer = FakeMaterializer(
        fixtures={item.input_url: fixture_for(item) for item in batch.papers}
    )
    runner = TrackingRunner(materializer=fake_materializer)

    await convert_batch(
        batch, tmp_path, state, CONCURRENCY, runner, fake_materializer, NOW
    )

    assert runner.maximum_active["pdf"] == 1


@pytest.mark.asyncio
async def test_pdf_runtime_config_must_equal_one(
    tmp_path: Path, state: PipelineState
) -> None:
    batch = Batch(papers=(paper("arxiv:1", input_format="pdf"),), estimated_cost=1)
    fake_materializer = FakeMaterializer(
        fixtures={batch.papers[0].input_url: fixture_for(batch.papers[0])}
    )
    runner = TrackingRunner(materializer=fake_materializer)

    with pytest.raises(InfrastructureError, match="PDF concurrency must equal 1"):
        await convert_batch(
            batch,
            tmp_path,
            state,
            ConcurrencyConfig.model_construct(html=1, latex=1, pdf=2),
            runner,
            fake_materializer,
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
    fake_materializer = FakeMaterializer(
        fixtures={item.input_url: fixture_for(item) for item in batch.papers}
    )
    runner = TrackingRunner(
        materializer=fake_materializer,
        behaviors={batch.papers[1].input_url: "paper_error"},
    )

    result = await convert_batch(
        batch,
        tmp_path,
        state,
        CONCURRENCY,
        runner,
        fake_materializer,
        NOW,
    )

    assert [item.paper.identifier for item in result.succeeded] == ["arxiv:1"]
    assert [item.paper.identifier for item in result.failed] == ["ss:2"]
    assert result.promoted == ()
    assert expected_markdown(tmp_path, batch.papers[0]).exists()


@pytest.mark.asyncio
async def test_failures_accumulate_consecutively_until_success_clears_history(
    tmp_path: Path,
) -> None:
    target = paper("ss:2", input_format="pdf")
    batch = Batch(papers=(target,), estimated_cost=1)
    fake_materializer = FakeMaterializer(
        fixtures={target.input_url: fixture_for(target)}
    )
    first = await convert_batch(
        batch,
        tmp_path,
        PipelineState(),
        CONCURRENCY,
        TrackingRunner(
            materializer=fake_materializer,
            behaviors={target.input_url: "paper_error"},
        ),
        fake_materializer,
        NOW,
    )
    second = await convert_batch(
        batch,
        tmp_path,
        first.state,
        CONCURRENCY,
        TrackingRunner(
            materializer=fake_materializer,
            behaviors={target.input_url: "paper_error"},
        ),
        fake_materializer,
        NOW.replace(day=24),
    )
    third = await convert_batch(
        batch,
        tmp_path,
        second.state,
        CONCURRENCY,
        TrackingRunner(materializer=fake_materializer),
        fake_materializer,
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
                FailureAttempt(
                    occurred_at=NOW.replace(day=21), error="converter exited 1"
                ),
                FailureAttempt(
                    occurred_at=NOW.replace(day=22), error="converter exited 1"
                ),
            ]
        }
    )
    fake_materializer = FakeMaterializer(
        fixtures={target.input_url: fixture_for(target)}
    )
    marker = expected_markdown(tmp_path, target).with_suffix(".fixme.txt")
    observed_temp_paths: list[Path] = []
    original_replace = Path.replace

    def spy_replace(self: Path, target_path: Path) -> Path:
        observed_temp_paths.append(self)
        assert target_path == marker
        assert self.exists()
        assert self.read_text(encoding="utf-8") == (
            "paper: arxiv:2401.00001\n"
            "latest_error: converter exited 1\n"
            "attempt: 2026-09-21T02:00:00+00:00 converter exited 1\n"
            "attempt: 2026-09-22T02:00:00+00:00 converter exited 1\n"
            "attempt: 2026-09-23T02:00:00+00:00 converter exited 1\n"
        )
        return original_replace(self, target_path)

    monkeypatch.setattr(Path, "replace", spy_replace)

    result = await convert_batch(
        batch,
        tmp_path,
        prior_state,
        CONCURRENCY,
        TrackingRunner(
            materializer=fake_materializer,
            behaviors={target.input_url: "paper_error"},
        ),
        fake_materializer,
        NOW,
    )

    assert marker.read_text(encoding="utf-8").count("attempt:") == 3
    assert result.promoted == (marker,)
    assert result.state.failures == {}
    assert observed_temp_paths == [marker.with_name(f"{marker.name}.tmp")]


@pytest.mark.asyncio
async def test_converter_must_materialize_output(
    tmp_path: Path, state: PipelineState
) -> None:
    target = paper("arxiv:1", input_format="html")
    batch = Batch(papers=(target,), estimated_cost=1)
    fake_materializer = FakeMaterializer(
        fixtures={target.input_url: fixture_for(target)}
    )

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
                materializer=fake_materializer,
                behaviors={target.input_url: "no_output"},
            ),
            fake_materializer,
            NOW,
        )


@pytest.mark.asyncio
async def test_marker_output_is_moved_from_marker_contract_location(
    tmp_path: Path, state: PipelineState
) -> None:
    target = paper("ss:2", input_format="pdf")
    batch = Batch(papers=(target,), estimated_cost=1)
    fake_materializer = FakeMaterializer(
        fixtures={target.input_url: fixture_for(target)}
    )

    result = await convert_batch(
        batch,
        tmp_path,
        state,
        CONCURRENCY,
        TrackingRunner(materializer=fake_materializer),
        fake_materializer,
        NOW,
    )

    output = expected_markdown(tmp_path, target)
    assert result.succeeded[0].output == output
    assert output.read_text(encoding="utf-8") == "# converted .pdf\n"
    assert not any((tmp_path / ".convert-batch").glob("**/*.md"))


@pytest.mark.asyncio
async def test_infrastructure_failure_cleans_successful_outputs_and_leaves_backlog_pending(
    tmp_path: Path,
) -> None:
    successful = paper("arxiv:1", input_format="html")
    aborted = paper("ss:2", input_format="pdf")
    batch = Batch(papers=(successful, aborted), estimated_cost=2)
    state = PipelineState(
        failures={
            successful.identifier: [
                FailureAttempt(
                    occurred_at=NOW.replace(day=21), error="converter exited 1"
                ),
                FailureAttempt(
                    occurred_at=NOW.replace(day=22), error="converter exited 1"
                ),
            ]
        }
    )
    initial_state = state.model_copy(deep=True)
    fake_materializer = FakeMaterializer(
        fixtures={item.input_url: fixture_for(item) for item in batch.papers}
    )
    runner = TrackingRunner(
        materializer=fake_materializer,
        behaviors={aborted.input_url: "infra_error"},
        delays={successful.input_url: 0.0, aborted.input_url: 0.02},
    )

    with pytest.raises(InfrastructureError, match="missing conversion tool: pandoc"):
        await convert_batch(
            batch,
            tmp_path,
            state,
            CONCURRENCY,
            runner,
            fake_materializer,
            NOW,
        )

    assert not expected_markdown(tmp_path, successful).exists()
    assert (
        not expected_markdown(tmp_path, successful).with_suffix(".fixme.txt").exists()
    )
    assert infer_backlog(batch.papers, tmp_path).pending == batch.papers
    assert state == initial_state
    assert not (tmp_path / ".convert-batch").exists()


@pytest.mark.asyncio
async def test_unexpected_task_failure_is_wrapped_as_infrastructure_error(
    tmp_path: Path, state: PipelineState
) -> None:
    target = paper("arxiv:1", input_format="html")
    batch = Batch(papers=(target,), estimated_cost=1)
    fake_materializer = FakeMaterializer(
        fixtures={target.input_url: fixture_for(target)}
    )

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
                materializer=fake_materializer,
                behaviors={target.input_url: "unexpected"},
            ),
            fake_materializer,
            NOW,
        )


def _kind_for(argv: Sequence[str]) -> str:
    if argv[0] == "marker_single":
        return "pdf"
    if Path(urlsplit(argv[1]).path).suffix == ".html":
        return "html"
    return "latex"


def _write_converter_output(argv: Sequence[str], input_path: Path) -> None:
    if argv[0] == "marker_single":
        output_dir = Path(argv[3])
        output = output_dir / input_path.stem / f"{input_path.stem}.md"
    else:
        output = Path(
            next(
                arg.removeprefix("--output=")
                for arg in argv
                if arg.startswith("--output=")
            )
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        f"# converted {input_path.suffix}\n",
        encoding="utf-8",
    )


def _sleeping_child_script(pid_file: Path) -> str:
    return (
        "from pathlib import Path; "
        "import os, sys, time; "
        f"Path({str(pid_file)!r}).write_text(str(os.getpid()), encoding='utf-8'); "
        "print('ready', flush=True); "
        "print('waiting', file=sys.stderr, flush=True); "
        "time.sleep(30)"
    )


async def _wait_for_file(path: Path, *, timeout: float = 5.0) -> str:
    async with asyncio.timeout(timeout):
        while not path.exists():
            await asyncio.sleep(0.01)
    return path.read_text(encoding="utf-8")


async def _assert_process_gone(pid: int, *, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while _is_process_alive(pid):
            await asyncio.sleep(0.01)


def _is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
