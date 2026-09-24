# Task 18 Report

## Status

Complete. The source template and a repository rendered from a clean local Git
source pass formatting, strict type checking, and all offline tests. No
migrations were performed.

## Delivered

- Added source and generated operational documentation covering architecture,
  every validated range, adapter credentials and filters, commands, state and
  backlog behavior, single-worker PDF conversion, fixme recovery, formatting,
  summaries, update pull requests, ownership, and the migration gate.
- Added executable `scripts/smoke-test.sh`.
- Made every Copier smoke render use a clean local Git source and immutable
  commit ref, with `DirtyLocalWarning` promoted to an error.
- Made the generated smoke initialize Git, run
  `uv sync --locked --extra dev`, execute all pre-commit hooks, and run the
  complete fixture-backed suite with `UV_OFFLINE=true`.
- Rendered the lock package name and declared the Hatch wheel package so any
  valid generated slug installs without changing its lock.
- Fixed Ruff formatting and strict mypy findings with annotations, a Git
  protocol, current stubs/dependencies, and concrete response conversions.

## Commits

- `4ca20a5` — `test: validate generated repository offline`
- `39310f0` — `docs: complete papers template validation guide`
- `79cfc16` — `fix: make source smoke environment reproducible`

The report itself is committed after this list is written; its SHA is reported
in the final task response because a commit cannot contain its own SHA.

## Final validation

### Generated smoke

Command:

```text
./scripts/smoke-test.sh
```

Final output:

```text
Resolved 43 packages in 3ms
Checked 42 packages in 5ms
collected 5 items
tests/test_copier_smoke.py::test_template_renders_python_package PASSED
tests/test_copier_smoke.py::test_template_renders_protected_state_files PASSED
tests/test_copier_smoke.py::test_generated_repository_passes_offline_suite PASSED
tests/test_copier_smoke.py::test_generated_readme_documents_operations_and_migration_gate PASSED
tests/test_template_exclusions.py::test_copier_update_preserves_repository_owned_data PASSED
5 passed in 22.30s
```

The generated-repository test also completed its internal locked sync, all
three generated pre-commit hooks, and the complete generated pytest suite with
`UV_OFFLINE=true`.

### Root checks and suites

Command:

```text
uv run pre-commit run --all-files && uv run pytest -v && git diff --check
```

Final output:

```text
ruff check...............................................................Passed
ruff format..............................................................Passed
mypy.....................................................................Passed
collected 5 items
tests/test_copier_smoke.py::test_template_renders_python_package PASSED
tests/test_copier_smoke.py::test_template_renders_protected_state_files PASSED
tests/test_copier_smoke.py::test_generated_repository_passes_offline_suite PASSED
tests/test_copier_smoke.py::test_generated_readme_documents_operations_and_migration_gate PASSED
tests/test_template_exclusions.py::test_copier_update_preserves_repository_owned_data PASSED
5 passed in 22.74s
```

`git diff --check` produced no output.

### Explicit specification coverage

All requested assertions exited 0. Output-producing checks returned:

```text
template/src/papers_pipeline/convert.py:        "pdf": asyncio.Semaphore(1),
template/.github/workflows/template-update.yml:        uses: peter-evans/create-pull-request@22a9089034f40e5a961c8808d113e2c98fb63676
```

The three workflow files exist, there are at least eight adapter Python files,
and neither the nightly workflow nor its script contains the whole-corpus
`papers/**/*.md` glob.

## Deviations

- The requested generated lock could not remain a static `template/uv.lock`:
  its root package name made `uv sync --locked` fail for rendered project
  slugs. It is now `template/uv.lock.jinja`, with only the root package name
  rendered.
- The root smoke environment now declares `httpx`; the pre-existing render
  validation imports the generated CLI, which imports the HTTP pipeline.
- Final generated validation explicitly sets `UV_OFFLINE=true` to enforce the
  no-network requirement rather than merely relying on fixture behavior.

## Blockers

None.
