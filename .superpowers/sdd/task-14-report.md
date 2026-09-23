# Task 14 Report

## Delivered

- Added typed source/run summaries and append-only GitHub step-summary output.
- Added exact-path Git commits with deletion handling, no-op detection, and
  infrastructure-error classification.
- Added the validated nightly orchestration path: fetch, normalize/topic gate,
  deduplicate, inventory/cursor commit, bounded conversion batches, changed-file
  indexing/formatting, failure-state/fixme persistence, batch commits, and final
  backlog recomputation.
- Added real `nightly` dependency construction and sharded `format-corpus` CLI
  commands. No workflow files were added.

## TDD evidence

1. `cd template && PYTHONPATH=src ../.venv/bin/pytest tests/test_summary.py tests/test_pipeline.py -v`
   initially failed during collection because `papers_pipeline.summary` and
   `papers_pipeline.git` did not exist.
2. Focused implementation run: `11 passed`.
3. CLI tests were separately observed failing because the new command
   collaborators were absent, then passed after wiring.
4. The validation-order test was observed failing with one fetch call before an
   invalid plugin error, then passed after moving plugin validation ahead of
   fetch.

## Verification

- Template suite: `181 passed`.
- Root suite: `2 passed` (only Copier dirty-template warnings).
- Ruff on changed source/tests: `All checks passed!`
- Mypy strict on the changed source surface: `Success: no issues found in 4 source files`.
- `git diff --check`: clean.

Coverage includes transaction ordering, exact scoped/deletion commits, no-op
reruns, attempt-once continuation, infrastructure summaries without batch
commits, exact formatting inputs, CLI sharding/dependency construction, and
validation before network activity. The full conversion suite also exercises
partial completion and cancellation cleanup. Self-review confirmed the initial
inventory/cursor commit occurs before conversion and remains independently
pushable if a later batch fails.

## Findings remediation

- Added a pre-mutation Git preflight over inventory, state, index, generated
  papers, conversion workspace, and input-cache paths. Dirty relevant paths now
  fail with an actionable `InfrastructureError`; unrelated user changes remain
  untouched.
- Added exact byte/existence snapshots around inventory persistence and every
  conversion batch. Inventory/state write or commit failure and conversion,
  index, formatter, state-save, or batch-commit failure restore the complete
  transaction write set and clear only its staged paths without `git reset`.
- Kept the inventory transaction independent, so its successful commit survives
  a later conversion failure and a subsequent clean retry can process the same
  backlog.
- Removed the second synthesized cap/continuation event; the canonical fetch
  event is now emitted once.

### Additional TDD and verification evidence

Recovery regressions were first observed failing for retained inventory/state,
promoted output/index files, missing clean-preflight APIs, unsafe cleanup of
unstaged paths, and duplicate cap events. After implementation:

- Focused pipeline suite: `16 passed`.
- Full template suite: `188 passed`.
- Root suite: `2 passed` with only expected Copier dirty-template warnings.
- Ruff: `All checks passed!`
- Mypy strict: `Success: no issues found in 26 source files`.
- `git diff --check`: clean.

## Commit-point hardening

- `GitRepository.commit` now resolves the old HEAD before committing, validates
  that HEAD advanced afterward, and falls back to `git log` when the primary
  post-commit `rev-parse` fails.
- If both post-commit SHA lookups fail, `CommitCompletedError` records that the
  commit command succeeded. Inventory and batch transactions propagate this
  committed-state error without restoring files or clearing staging, preventing
  reverse-dirty worktrees and duplicate commits on retry.
- Added real-subprocess tests with injected post-commit lookup failures,
  covering fallback SHA recovery and the unresolved committed-state retry path.
- Removed the unrequested tracked Task 14 recovery plan.

### Final verification

- Focused Task 14 tests: `20 passed`.
- Full template suite: `190 passed`.
- Root suite: `2 passed` with only expected Copier dirty-template warnings.
- Ruff: `All checks passed!`
- Mypy strict (with stale third-party suppression warnings disabled):
  `Success: no issues found in 26 source files`.
- `git diff --check`: clean.
