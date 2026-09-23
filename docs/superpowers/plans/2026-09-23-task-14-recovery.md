# Task 14 Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make each pipeline persistence phase recover its exact pre-phase files after failure and reject pre-existing relevant-path changes before mutation.

**Architecture:** Add a scoped Git clean-worktree preflight and a small byte-level file transaction in the pipeline. Inventory/state persistence and each conversion batch snapshot their complete write sets, restore those snapshots on failure, and clear only transaction-created staging entries; successful inventory commits remain outside later batch transactions.

**Tech Stack:** Python 3.12, asyncio, pathlib, Git CLI, pytest, Ruff, mypy.

## Global Constraints

- Never silently discard pre-existing user changes.
- Do not use destructive Git reset.
- Follow test-driven development.
- Preserve a successful inventory commit when later conversion fails.

---

### Task 1: Scoped Git safety

**Files:**
- Modify: `template/src/papers_pipeline/git.py`
- Test: `template/tests/test_pipeline.py`

**Interfaces:**
- Produces: `GitRepository.assert_clean(paths: Sequence[Path]) -> None`
- Produces: `GitRepository.clear_staging(paths: Sequence[Path]) -> None`

- [x] Add tests proving relevant dirty paths raise actionable `InfrastructureError`, unrelated changes remain allowed, and failed commits leave no transaction-created staging.
- [x] Run the focused tests and verify they fail.
- [x] Implement porcelain-status preflight and exact-path staging cleanup without `git reset`.
- [x] Run the focused tests and verify they pass.

### Task 2: Pipeline file transactions

**Files:**
- Modify: `template/src/papers_pipeline/pipeline.py`
- Test: `template/tests/test_pipeline.py`

**Interfaces:**
- Consumes: `GitRepository.assert_clean` and `GitRepository.clear_staging`
- Produces: exact-byte/existence rollback around inventory and batch persistence

- [x] Add failing tests for inventory commit failure, formatter failure, batch commit failure, and a successful retry.
- [x] Add a pre-mutation relevant-path preflight and snapshot/restore helper.
- [x] Wrap inventory/state writes and complete batch conversion/index/format/state/commit work in independent transactions.
- [x] Verify focused recovery tests pass.

### Task 3: Event deduplication and report

**Files:**
- Modify: `template/src/papers_pipeline/pipeline.py`
- Modify: `template/tests/test_pipeline.py`
- Modify: `.superpowers/sdd/task-14-report.md`

**Interfaces:**
- Produces: one source cap/continuation event per fetch result

- [x] Add a failing event-count regression test.
- [x] Remove synthesized duplication while retaining fetched canonical events.
- [x] Run all template and root tests, Ruff, mypy, and `git diff --check`.
- [x] Append findings and verification evidence to the Task 14 report.
- [ ] Commit with the required co-author trailer.
