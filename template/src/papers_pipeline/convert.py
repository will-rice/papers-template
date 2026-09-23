from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from papers_pipeline.batching import Batch, expected_markdown
from papers_pipeline.config import ConcurrencyConfig
from papers_pipeline.errors import InfrastructureError, PaperError
from papers_pipeline.models import FailureAttempt, Paper, PipelineState


class CommandRunner:
    async def run(
        self, argv: Sequence[str], timeout: float
    ) -> subprocess.CompletedProcess[str]:
        try:
            return await asyncio.to_thread(
                subprocess.run,
                argv,
                text=True,
                capture_output=True,
                check=True,
                timeout=timeout,
            )
        except FileNotFoundError as error:
            raise InfrastructureError(f"missing conversion tool: {argv[0]}") from error
        except subprocess.TimeoutExpired as error:
            raise InfrastructureError(
                f"conversion infrastructure timeout: {argv[0]}"
            ) from error
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
            return PaperConversion(paper=paper, output=output, error=None)
        except PaperError as error:
            return PaperConversion(paper=paper, output=None, error=str(error))

    raw_results = await asyncio.gather(
        *(convert_one(paper) for paper in batch.papers),
        return_exceptions=True,
    )

    for raw_result in raw_results:
        if isinstance(raw_result, InfrastructureError):
            raise raw_result
        if isinstance(raw_result, BaseException):
            raise InfrastructureError("unexpected converter task failure") from raw_result

    results = tuple(result for result in raw_results if isinstance(result, PaperConversion))

    failures = {identifier: list(attempts) for identifier, attempts in state.failures.items()}
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
        _write_fixme(
            marker,
            identifier=identifier,
            latest_error=result.error,
            attempts=attempts,
        )
        promoted.append(marker)
        failures.pop(identifier, None)

    return ConversionResult(
        succeeded=tuple(result for result in results if result.error is None),
        failed=tuple(result for result in results if result.error is not None),
        promoted=tuple(promoted),
        state=state.model_copy(update={"failures": failures}),
    )


def _write_fixme(
    path: Path,
    *,
    identifier: str,
    latest_error: str,
    attempts: list[FailureAttempt],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(
        "\n".join(
            [
                f"paper: {identifier}",
                f"latest_error: {latest_error}",
                *(
                    f"attempt: {attempt.occurred_at.isoformat()} {attempt.error}"
                    for attempt in attempts
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
