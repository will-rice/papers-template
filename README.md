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

## Migrate a legacy repository

Legacy `*-papers` repositories keep `papers.csv` as
`arxiv_id,title,authors,submitted,categories,url,abstract[,source]` and
markdown at `papers/<year>/<id>.md`. Migrate each one on its own branch, from a
full clone, against an immutable template release:

1. Apply the template over the checkout:
   `uv run copier copy --trust --defaults --overwrite --vcs-ref <release>
   -d project_name=... -d project_slug=... -d topic_description=...
   gh:will-rice/papers-template <repo>`. Copier skips `papers/`,
   `papers.csv`, and `.papers-state.yml`.
2. In the repository, run `uv sync --locked --extra dev`, then
   `uv run python <template>/scripts/migrate_legacy_corpus.py`. It converts
   `papers.csv` to the template schema with the pipeline's own normalization
   and deduplication, `git mv`s each paper to the path the pipeline expects,
   rewrites in-corpus links, replaces legacy front matter with the pipeline's,
   removes legacy indexes, and writes an empty `.papers-state.yml`.
3. Remove every tracked legacy file the template does not render, keeping
   `papers/`, `papers.csv`, `.papers-state.yml`, and `LICENSE`.
4. Port the legacy topic and source settings into `papers.yml`, then run
   `uv run papers-pipeline validate --config papers.yml`,
   `uv run pre-commit run --all-files`, and `uv run pytest`.
5. Open a pull request. After merging, disable `Nightly papers`, run it
   manually, inspect the summary and continuation, then re-enable it.
