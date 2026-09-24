# Task 17 Report

## Delivered

- Added a real Copier copy, destination commit, and update regression using a
  local Git template repository with immutable initial and updated refs.
- Verified the initial inventory and state files render with their expected
  bytes.
- Added update collisions for `papers/`, `papers.csv`, `.papers-state.yml`,
  and download, parser, model, and tool caches, then proved repository bytes
  remain identical while a template-owned Python file and Copier metadata
  advance to version `2.0.0`.
- Made `DirtyLocalWarning` an error around both Copier operations, ensuring the
  regression cannot silently fall back to a dirty local template.
- Confirmed the existing `copier.yml` ownership block already exactly uses
  `_skip_if_exists` for all required paths and has no `_exclude` that would
  suppress initial generation. Update workflow metadata was unchanged.

## TDD evidence

1. The first execution failed because Copier updates require
   `overwrite=True`; the test harness was corrected before assessing behavior.
2. A deliberate removal of the `papers.csv` `_skip_if_exists` entry then
   produced the intended byte-preservation failure (`0xff` was lost).
3. Restoring the required ownership block made the regression pass.

## Verification

- Focused ownership and smoke suites: `3 passed`.
- Full root suite: `3 passed`.
- Full rendered-template suite: `221 passed`.
- Changed-test Ruff check and format check: passed.
- Changed-test strict mypy: passed.
- Full Ruff lint: passed.
- `git diff --check`: passed.

## Baseline findings

- Full Ruff formatting still reports 32 pre-existing rendered-template files
  that would be reformatted.
- Full mypy still reports 28 pre-existing errors in rendered source/tests,
  including missing annotations/stubs and the two known unused-ignore errors.
- The two existing smoke tests continue to emit `DirtyLocalWarning` while the
  new immutable-source update regression emits none.

## Follow-up hardening

- Extended the ownership regression to cover representative bytes in every
  protected cache root from `copier.yml`: `.cache`, `.pytest_cache`,
  `.mypy_cache`, and `.ruff_cache`.
- Added a direct assertion that the tested protected roots match
  `_skip_if_exists`, so removing any owned entry now fails the regression.
- Kept the update path anchored to a real local Git repository and preserved
  warning-free execution for the update regression itself.
- Re-verified the focused root suite, Ruff check/format, and strict mypy for
  the changed test file.
