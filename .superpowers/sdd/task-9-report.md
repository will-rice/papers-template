# Task 9 Report

**Status:** complete

## RED

- Added failing offline tests first for:
  - `BiorxivCrossrefAdapter` contract coverage across both `biorxiv` and `crossref`
  - opaque continuation cursors with provider-specific continuation handling
  - per-record isolation, out-of-window continuation progress, and source-consumed caps
  - Crossref HTML-over-PDF selection plus partial date/author parsing
  - `PapersWithCodeAdapter` continuation safety, conversion-input requirements, and explicit auth/page failures
  - runtime `build_adapters()` registration and Semantic Scholar secret loading behavior
- Added recorded fixtures under:
  - `template/tests/fixtures/adapters/biorxiv_crossref/`
  - `template/tests/fixtures/adapters/papers_with_code/`
- Verified red with:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py tests/adapters/test_contract.py -q`
- Initial failures:
  - `ModuleNotFoundError: No module named 'papers_pipeline.adapters.biorxiv_crossref'`
  - `ModuleNotFoundError: No module named 'papers_pipeline.adapters.papers_with_code'`
  - `ImportError: cannot import name 'build_adapters' from 'papers_pipeline.adapters'`

## GREEN

- Implemented `BiorxivCrossrefAdapter` with:
  - provider validation for `biorxiv` vs `crossref`
  - opaque continuation cursors carrying provider/token/page/source-consumed state
  - bioRxiv offset progression and Crossref cursor progression without exposing raw tokens
  - explicit invalid JSON/payload failures and per-record `PaperError` isolation
  - Crossref HTML link preference, PDF fallback, and robust partial date/author parsing
- Implemented `PapersWithCodeAdapter` with:
  - opaque continuation cursors carrying API next URLs safely
  - strict trusted-next-URL validation for `https://paperswithcode.com/api/v1/papers/`
  - required PDF conversion input validation per record
  - explicit auth / invalid JSON / invalid payload failures
- Expanded the runtime adapter registry to instantiate all six adapter families and only read the Semantic Scholar secret when configured and enabled.
- Kept all behavior fixture-backed and offline; no migrations or unrelated spec changes were made.

## Validation

- Focused Task 9 suite:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py tests/adapters/test_contract.py -q` → `31 passed`
- All adapter suites:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters -v` → `73 passed`
- Full template suite:
  - `cd template && PYTHONPATH=src uv run --project .. pytest -q` → `123 passed`
- Root repository suite:
  - `uv run pytest -q` → `2 passed, 2 warnings` (existing Copier dirty-template warnings)
- Ruff:
  - `cd template && PYTHONPATH=src uv run --project .. ruff check src tests` → `All checks passed!`
- Changed-surface mypy:
  - `cd template && PYTHONPATH=src uv run --project .. mypy src/papers_pipeline/adapters/__init__.py src/papers_pipeline/adapters/biorxiv_crossref.py src/papers_pipeline/adapters/papers_with_code.py tests/conftest.py tests/adapters/test_contract.py tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py` → `Success: no issues found in 7 source files`
- Self-review:
  - inspected the final diff/stat and `git diff --check` after validation; no whitespace issues found

## Concerns

- `uv run pytest -q` at the repository root still reports the pre-existing Copier `DirtyLocalWarning` warnings; Task 9 does not introduce new warnings.

---

## 2026-09-23 Cursor termination findings

### RED

- Added failing regression coverage for:
  - Papers With Code final non-empty pages with `next: null` and blank `next`
  - Crossref final non-empty pages with missing and blank `message.next-cursor`
  - final-page local truncation for both adapters, proving resumable continuation without skipped or duplicated records
- Verified red with:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py -q`
- Initial failures showed both adapters preserving/reusing their current continuation state on exhausted pages, and local truncation replaying the first record instead of resuming within the page.

### GREEN

- Updated `PapersWithCodeAdapter` to:
  - treat `next: null` and blank `next` as end-of-enumeration
  - keep a continuation cursor only when a real API next URL exists or local intra-page truncation must resume
  - resume final-page local truncation from an intra-page index instead of re-emitting earlier records
- Updated `BiorxivCrossrefAdapter` Crossref handling to:
  - treat missing/blank `message.next-cursor` as complete enumeration
  - preserve a cursor only for a real next cursor token or a local intra-page resume point
  - resume final-page local truncation from an intra-page index without skip/duplicate behavior
- Left bioRxiv behavior unchanged.

### Validation

- Focused affected adapters:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py -q` → `29 passed`
- All adapter suites:
  - `cd template && PYTHONPATH=src uv run --project .. pytest tests/adapters -q` → `79 passed`
- Full template suite:
  - `cd template && PYTHONPATH=src uv run --project .. pytest -q` → `129 passed`
- Ruff:
  - `cd template && PYTHONPATH=src uv run --project .. ruff check src tests` → `All checks passed!`
- Mypy:
  - `cd template && PYTHONPATH=src uv run --project .. mypy src/papers_pipeline/adapters/biorxiv_crossref.py src/papers_pipeline/adapters/papers_with_code.py tests/adapters/test_biorxiv_crossref.py tests/adapters/test_papers_with_code.py tests/conftest.py` → `Success: no issues found in 5 source files`
  - `cd template && PYTHONPATH=src uv run --project .. mypy src tests` still reports the repository's pre-existing unrelated typing issues in `tests/test_state_inventory.py`, `tests/test_normalize_topics.py`, `tests/test_config.py`, `tests/test_fixture_transport.py`, and `tests/test_http.py`
- Root repository smoke suite:
  - `uv run pytest -q` → `2 passed, 2 warnings` (existing Copier dirty-template warnings)

### Concerns

- Full-repository `mypy src tests` is still blocked by pre-existing unrelated typing failures outside the changed Task 9 surface.
