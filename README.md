# Papers Template

Copier source for standalone, topic-focused `*-papers` repositories.

## Generate a repository

Run `uv run copier copy . ../example-papers`, answer the prompts, then run
`uv sync --locked --extra dev` in the generated repository.

## Validate the template

Run `./scripts/smoke-test.sh`. It renders from a clean local Git source and
runs the generated repository's pre-commit hooks and complete offline test
suite. Run `uv run pytest` for the complete source test suite.

## Ownership boundary

Copier updates pipeline code, tests, workflows, and support files. It never
owns `papers/`, `papers.csv`, `.papers-state.yml`, or caches.

## Migration gate

Migrations of lipsync-papers, tts-papers, asr-papers, and birdclef-papers are
separate follow-on projects. Do not begin a migration until the generated
repository smoke test passes on the template release selected for migration.
