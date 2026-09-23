# Task 13 Report

**Status:** complete

## RED

- Added `template/tests/test_formatting.py` first, covering:
  - exact-path formatting with spaces preserved as single argv items
  - deduped and deterministically sorted formatter inputs
  - empty-input no-op behavior
  - propagation of `CommandRunner` classifications for missing tool, non-zero exit, and timeout
  - deterministic modulo sharding from a sorted corpus
  - shard bound validation
  - index write idempotence and unchanged-file rewrite avoidance
- Verified red with:
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && PYTHONPATH=template/src uv run pytest template/tests/test_formatting.py -v`
- Initial failure:
  - `ModuleNotFoundError: No module named 'papers_pipeline.formatting'`

## GREEN

- Implemented `template/src/papers_pipeline/formatting.py` with:
  - `format_changed(paths, runner)` using exact path arguments only
  - safe argv construction that preserves spaces and avoids shell globbing
  - deterministic deduping/sorting before formatting
  - `shard_paths(paths, shard_index, shard_count)` with bounds validation and one-time sorted modulo partitioning
- Implemented `template/src/papers_pipeline/indexing.py` with:
  - `write_index(root, papers)` returning `root / "README.md"`
  - stable, deterministic index rendering
  - write avoidance when the rendered content already matches disk

## Validation

- Focused Task 13 suite:
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && PYTHONPATH=template/src uv run pytest template/tests/test_formatting.py -v` → `11 passed`
- Full template suite:
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle/template && PYTHONPATH=src uv run pytest tests -q` → `170 passed`
- Ruff:
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && PYTHONPATH=template/src uv run ruff check template/src/papers_pipeline template/tests/test_formatting.py` → `All checks passed!`
- Mypy:
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && PYTHONPATH=template/src uv run mypy template/src/papers_pipeline` → `Success: no issues found in 23 source files`
  - `cd /Users/will/projects/copilot-worktrees/papers-template/will-rice-automatic-barnacle && PYTHONPATH=template/src uv run mypy template/src/papers_pipeline/formatting.py template/src/papers_pipeline/indexing.py template/tests/test_formatting.py` → `Success: no issues found in 3 source files`

## Notes

- No workflow changes were added.
- The implementation stays within the existing `CommandRunner` error classification boundary rather than re-parsing formatter output.
