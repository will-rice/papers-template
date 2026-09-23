# Task 5 Report: Deadline-Aware HTTP and Retry Policy

## RED
### Tests written first
- Added `template/tests/test_http.py` before any production HTTP client code.
- Covered:
  - expired shared deadline errors;
  - per-request timeout clamped to remaining total deadline;
  - retry stop behavior once the shared deadline is consumed;
  - bounded exponential backoff for retryable network failures;
  - immediate permanent authentication failure with no retry;
  - immediate permanent non-retryable HTTP failure;
  - explicit async client shutdown for later orchestration.

### Failing commands and results
- `cd template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - Result: `ERROR`
  - Failure: `ModuleNotFoundError: No module named 'httpx'`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv pip install httpx pytest-asyncio`
  - Result: installed missing runtime/test dependencies needed by the template HTTP tests.
- `cd template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - Result: `ERROR`
  - Failure: `ModuleNotFoundError: No module named 'papers_pipeline.http'`

These failures confirmed the new tests were exercising missing Task 5 behavior rather than existing code.

## GREEN
### Implementation added
- `template/src/papers_pipeline/http.py`

### Behavior delivered
- Added `Deadline.start()` and `Deadline.remaining()` to enforce one caller-supplied total fetch deadline.
- Added `RequestClient.get_text()` with:
  - per-request timeout bounded by remaining shared deadline;
  - retryable classification for timeout/network failures and HTTP `408/429/500/502/503/504`;
  - bounded exponential backoff that clamps to remaining deadline;
  - immediate permanent authentication failure for `401/403`;
  - explicit `InfrastructureError` messages for deadline exhaustion, permanent HTTP failures, and retry exhaustion.
- Added clean client lifecycle support through `aclose()` plus async context-manager methods for later adapter orchestration.
- Preserved deterministic retry event recording via `client.events`.

### Passing focused command
- `cd template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - First GREEN attempt: `6 passed, 1 failed`
  - Failure cause: the original deadline test expected a third request after consuming the full deadline, but the implementation correctly refused to issue a request once the shared deadline had been fully spent.
- Updated the test expectation to assert no extra request is made after the deadline is exhausted, then reran:
- `cd template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - Result: `7 passed`

## Verification
### Full tests
- `cd template && PYTHONPATH=src uv run --project . pytest -v`
  - Result: `46 passed`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run pytest -v`
  - Result: `2 passed, 2 warnings`
  - Warning detail: existing Copier `DirtyLocalWarning: Dirty template changes included automatically.`

### Lint and type checks
- `cd template && PYTHONPATH=src uv run --project . ruff check src tests`
  - Result: `All checks passed!`
- `cd template && PYTHONPATH=src uv run --project . mypy src`
  - Result: `Success: no issues found in 10 source files`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run ruff check .`
  - Result: `All checks passed!`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run mypy tests`
  - Result: `Success: no issues found in 1 source file`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && git --no-pager diff --check`
  - Result: passed

## Self-review
- Reviewed the Task 5 diff directly for scope and behavior.
- Confirmed the change stays within the shared HTTP boundary only: no adapter code, no spec/plan edits, and no `.superpowers` files included in the code commit.
- Rechecked the retry classification, bounded backoff, deadline enforcement, and async lifecycle surface against the Task 5 brief and existing plan language; no follow-up fixes were needed after verification.

## Concerns
- Root-repository development dependencies still do not declare `httpx` or `pytest-asyncio`; Task 5 validation required installing those packages into the local environment because the rendered template depends on them before later template-generated environments exist.

## Commit
- `fcc753f` — `feat: add deadline-aware http retries`

## Review fix
### What changed
- Expanded `RequestClient.get_text()` to treat HTTPX protocol-layer request failures as retryable alongside timeout/network failures.
- Converted non-retryable request failures, including `httpx.TooManyRedirects`, into immediate explicit `InfrastructureError` values.
- Kept bounded deadline/backoff behavior and async client lifecycle unchanged.

### Focused tests added
- `test_retryable_protocol_failures_back_off_until_exhausted`
- `test_too_many_redirects_fails_immediately`

### Verification after the fix
- `cd template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - `9 passed`
- `cd template && PYTHONPATH=src uv run --project . pytest -v`
  - `48 passed`
- `cd template && PYTHONPATH=src uv run --project . ruff check src tests`
  - `All checks passed!`
- `cd template && PYTHONPATH=src uv run --project . mypy src`
  - `Success: no issues found in 10 source files`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run pytest -v`
  - `2 passed, 2 warnings`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run ruff check .`
  - `All checks passed!`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run mypy tests`
  - `Success: no issues found in 1 source file`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && git --no-pager diff --check`
  - passed

### Notes
- `httpx.RemoteProtocolError` is now retried under the existing bounded policy.
- `httpx.TooManyRedirects` now fails immediately with `InfrastructureError("too many redirects: <url>")`.

## 3xx redirect fix
### What changed
- Added an explicit 3xx guard in `RequestClient.get_text()` so redirect responses never fall through to success text.
- Classified plain redirects as immediate infrastructure failures with `InfrastructureError("redirect HTTP <status>: <url>")`.

### Focused regression test
- Added `test_redirect_302_is_not_treated_as_success`

### Verification after the fix
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle/template && PYTHONPATH=src uv run --project . pytest tests/test_http.py -v`
  - `10 passed in 0.03s`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle/template && PYTHONPATH=src uv run --project . pytest -v`
  - `49 passed in 0.10s`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle/template && PYTHONPATH=src uv run --project . ruff check src tests`
  - `All checks passed!`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle/template && PYTHONPATH=src uv run --project . mypy src`
  - `Success: no issues found in 10 source files`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run pytest -v`
  - `2 passed, 2 warnings`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run ruff check .`
  - `All checks passed!`
- `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && uv run mypy tests`
  - `Success: no issues found in 1 source file`
