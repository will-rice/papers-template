# Task 15 Report

## Delivered

- Added rendered CI, nightly, and manual corpus-formatting workflows.
- Pinned every action to an immutable 40-character commit SHA and pinned uv to
  `0.8.17`; every environment sync uses `--locked --extra dev`.
- Added least-privilege workflow permissions plus job and material-step
  timeouts.
- Added configuration preflight, non-cancelling mutation concurrency, and an
  executable nightly publishing script.
- Added a manual-only, validated 1-32 shard matrix that creates binary patches,
  downloads and applies them in numeric shard order, and opens a pull request
  against `main` without directly pushing it.
- Did not add a template-update workflow.

## TDD evidence

1. Added `template/tests/test_workflows.py` before workflow implementation.
2. The focused run failed as expected because all three workflow files were
   absent.
3. After implementation, the focused workflow suite passed: `11 passed`.
4. Runtime Git tests prove a failed pipeline still pushes its earlier committed
   batch and returns nonzero, while an inconsistent dirty managed tree is not
   pushed.

The policy suite also covers YAML 1.2-safe handling of the `on` key, exact
triggers and permissions, SHA pins, job/step timeouts, locked setup, validation
ordering, concurrency, nightly corpus-glob exclusion, manual-only formatting,
deterministic sharding, binary patch generation, numeric patch ordering, empty
patch handling, PR-only publication, and executable script mode.

## Verification

- Focused workflow suite: `11 passed`.
- Full template suite: `201 passed`.
- Copier/render suite: `2 passed` with only expected dirty-template warnings;
  it verifies all workflows and the executable script are rendered.
- Ruff: `All checks passed!`.
- Focused strict mypy: `Success: no issues found in 1 source file`.
- Full mypy remains at the pre-existing baseline: 25 errors in 6 unrelated test
  files.
- `bash -n`, YAML parsing, and `git diff --check`: passed.
- `actionlint` was not installed, so the optional check was skipped.

## Self-review

Confirmed the nightly wrapper records the pipeline status, refuses all pushes
when managed files remain dirty, pushes clean commits even after a later
pipeline failure, and then preserves the original nonzero status. Confirmed the
manual combiner skips empty patches and uses GNU version sorting so shard 10
cannot precede shard 2.

## Findings remediation

- The manual workflow now resolves the repository default branch in `plan`,
  records its commit SHA, and checks out that same immutable SHA in every
  formatting shard and in `combine`. The PR base uses the resolved default
  branch, so a dispatch from a feature branch cannot contribute feature-branch
  content.
- Workflow-level permissions are read-only. `plan` and `format` inherit only
  `contents: read`; only `combine` receives `contents: write` and
  `pull-requests: write`.
- The nightly post-run guard now explicitly covers the complete pipeline path
  set: `papers`, `papers.csv`, `.papers-state.yml`, `README.md`,
  `.convert-batch`, and `inputs`. Regression tests keep this list aligned with
  the pipeline guard, exercise dirt under every path, and prove ignored cache
  content does not block a clean push.

## Remediation verification

- TDD red run exposed the missing job permissions, immutable default-branch
  base, and complete nightly guard before implementation.
- Focused workflow suite: `19 passed`.
- Full template suite: `209 passed`.
- Copier/render suite: `2 passed` with the expected dirty-template warnings.
- Ruff: `All checks passed!`.
- Focused strict mypy: `Success: no issues found in 1 source file`.
- Full strict mypy remains at the pre-existing baseline: 25 errors in 6
  unrelated test files.
- `bash -n`, `git diff --check`, and YAML policy parsing: passed.
- `actionlint` was not installed, so the optional check was skipped.
