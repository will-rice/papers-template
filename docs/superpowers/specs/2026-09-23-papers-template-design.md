# Papers Repository Template Design

**Status:** Approved  
**Date:** 2026-09-23

## Purpose

Build a reusable, updateable Copier template for topic-focused `*-papers`
repositories. The first migrations are:

1. `will-rice/lipsync-papers`
2. `will-rice/tts-papers`
3. `will-rice/asr-papers`
4. `will-rice/birdclef-papers`

The template standardizes paper discovery, metadata normalization, conversion,
formatting, testing, and automation without centralizing runtime operation.
Every generated repository remains independently usable and owns its corpus and
generated outputs.

This design covers the template and the migration path. It does not implement
the template, redesign paper content, or combine the four repositories.

## Root-Cause Evidence

The shared design is motivated by two distinct scaling failures in the existing
repositories:

- `asr-papers` has 5,569 Markdown files. Its nightly conversion step exits with
  status 143 because `formatting.py` passes `papers/**/*.md` to Prettier for
  both whole-corpus `--write` and `--list-different` operations. Work that
  should be incremental therefore scales with the complete converted corpus.
- `birdclef-papers` has 1,777 rows in `papers.csv` but only 283 Markdown
  outputs, leaving 1,494 papers pending. Of those pending papers, 676 have
  `ss:` identifiers. Conversion uses a default `CONVERT_MAX_WORKERS` value of
  8, and the hosted runner loses communication when concurrent conversion
  starves its resources.
- `lipsync-papers`, with 1,145 papers, and `tts-papers`, with 3,418 papers,
  succeed because their converted corpora are incremental and remain within
  hosted-runner limits.

The design must therefore bound work by both input backlog and runner resources.
Increasing workflow timeouts alone is not a fix: unbounded whole-corpus
formatting and excessive PDF concurrency would still consume resources in
proportion to corpus size.

## System Architecture

### Standalone generated repositories

Each generated repository contains:

- pipeline code for discovery, normalization, deduplication, topic filtering,
  conversion, indexing, and changed-file formatting;
- unit, contract, integration, and offline smoke tests;
- scheduled, manual maintenance, validation, and template-update workflows;
- `papers.yml`, the repository-owned declarative configuration;
- `.papers-state.yml`, repository-owned continuation and failure-attempt state;
- `.copier-answers.yml`, recording the template source and selected version.

There is no shared runtime service. A generated repository can run and test
without checking out the template repository or another papers repository.
Template updates arrive only through Copier regeneration.

### Template ownership

Copier owns pipeline code, tests, workflows, and template-supplied support
files. It must not own or overwrite:

- `papers/`;
- `papers.csv`;
- `.papers-state.yml`;
- download, parser, model, or tool caches.

These paths are repository data or runtime state. They are excluded from
template rendering and update conflict resolution. The template may create an
initial empty `.papers-state.yml`, but Copier never owns its subsequent
contents. Repository-specific content outside these paths should be kept in
explicit extension points rather than by modifying template-owned files.

### Configuration and extension boundary

`papers.yml` is the single repository-level configuration surface. It defines:

- repository identity and metadata;
- enabled source adapters and their credentials or secret names;
- declarative topic gates and source-specific filters;
- source-specific incremental date/lookback windows;
- pagination and total-result caps;
- conversion batch count and estimated-cost budgets;
- request timeout, retry, backoff, and total fetch deadline;
- concurrency classes for HTML, LaTeX, and PDF work.

The pipeline schema-validates the entire file before making a network request
or starting conversion. Validation emits concise, actionable errors for unknown
or invalid options, missing required secrets, unsupported option combinations,
and unsafe resource limits.

Topic matching is declarative by default. The supported gates include explicit
include and exclude terms and source metadata constraints sufficient for the
initial four repositories. A repository may opt into one narrowly scoped topic
filter plugin only when its behavior cannot be expressed declaratively. The
plugin receives a normalized paper record and returns a topic decision; it
cannot replace fetching, normalization, deduplication, batching, conversion, or
workflow orchestration.

### Source adapters

Adapters support:

- arXiv;
- Hugging Face Papers;
- Semantic Scholar;
- DBLP;
- bioRxiv/Crossref;
- Papers With Code.

Every adapter implements the same small contract:

1. accept validated adapter configuration, an incremental window, pagination
   limits, and request policy;
2. yield source records plus continuation metadata;
3. distinguish retryable request failures, permanent source or record
   failures, and infrastructure failures.

Adapters do not apply repository-specific topic logic and do not write
`papers.csv`. The shared pipeline normalizes source records into a canonical
shape, applies topic gates, and deduplicates them. Deduplication uses stable
identifiers first and normalized bibliographic identity second, with
deterministic source precedence so identical input always produces identical
CSV output.

## Nightly Pipeline

The nightly workflow runs these stages in order.

### 1. Validate

Validate `papers.yml`, required secrets for enabled adapters, tool availability,
and resource limits before network or conversion work. Invalid configuration
fails the workflow before changing repository files.

### 2. Fetch incrementally

Each enabled adapter uses its configured incremental lookback rather than a
shared global window. All fetches enforce:

- a pagination cap;
- a per-request timeout;
- bounded retries with backoff;
- a total fetch deadline across all adapters.

Pagination and continuation are deterministic. Reaching a configured cap is
reported in the workflow summary and persists the adapter cursor and window in
`.papers-state.yml` for the next run; it is not presented as complete
enumeration. A source record failure is isolated and reported. Exhausted
retries, an expired total deadline, authentication failure, or loss of required
infrastructure fails explicitly rather than returning an empty successful
result.

### 3. Normalize and deduplicate

Normalize fetched records, apply declarative topic gates or the optional topic
plugin, merge duplicates, and update `papers.csv` deterministically. The CSV is
the inventory of accepted papers, not a queue with mutable status fields.
Commit a consistent inventory update, including its `.papers-state.yml`
continuation changes, before starting conversion batches. A run that discovers
no inventory or continuation changes creates no inventory commit.

### 4. Infer the conversion backlog

At the start of every run, infer pending work by comparing accepted rows in
`papers.csv` with the expected generated files under `papers/`. No separate
queue database is required. This makes continuation recoverable after a
cancelled job or partial batch.

`.papers-state.yml` is not a backlog or source of paper truth. It contains only
adapter continuation data and consecutive per-paper failure counters needed
across scheduled runs. Successful conversion removes a paper's failure counter.

Papers with an existing `.fixme.txt` marker remain visible in counts but are
excluded from automatic retries until their input or marker is deliberately
changed.

### 5. Select a bounded batch

Order pending papers by a stable repository-wide key, then select the largest
prefix that fits both:

- the configured maximum paper count; and
- the configured estimated-cost budget.

Estimated cost is a deterministic classification based on the selected input
path and format, not runtime timing. The same CSV and generated-file state
therefore selects the same batch. Both limits are mandatory and schema-bounded
to prevent an unsafe "unlimited" hosted-runner configuration.

### 6. Convert with resource classes

HTML and LaTeX conversion use modest, separately configurable parallelism.
PDF conversion through Marker/PyTorch uses exactly one worker for the entire
job. The implementation must enforce that invariant independently of the
number of general conversion workers.

Failures are classified as follows:

- A paper-specific download, parse, or conversion failure is recorded without
  stopping unrelated papers in the batch.
- A paper-specific failure increments that paper's consecutive-attempt counter
  and concise attempt history in `.papers-state.yml`.
- A paper that fails in three consecutive scheduled attempts is promoted to a
  colocated `.fixme.txt` file containing the paper identity, latest concise
  error, and three-attempt history. Promotion removes the transient failure
  state. This marker prevents endless nightly retries while preserving an
  actionable artifact in the repository.
- Infrastructure and resource failures, including total deadlines, runner
  communication loss, disk exhaustion, process termination, missing tools, or
  model initialization failure, fail the workflow explicitly. They are never
  converted into per-paper success or an empty batch.

### 7. Format only changed files

Run formatters only on files created or changed by the current fetch and
conversion work, plus indexes changed by that work. Do not pass a whole-corpus
glob to either formatting or format validation in the nightly workflow.

Whole-corpus formatting is a separate, manually dispatched maintenance
workflow. It enumerates the corpus deterministically, divides it into bounded
shards, and processes those shards independently.

### 8. Commit successful work

Commit each successful bounded batch independently from the inventory commit.
A batch commit contains its successful generated outputs, associated
`.fixme.txt` promotions, changed indexes, formatting changes, and the matching
failure-counter updates in `.papers-state.yml`. Isolated paper failures do not
discard successful papers. A batch with only isolated paper failures still
commits its failure counters or `.fixme.txt` promotions so repeated-failure
handling can progress. If infrastructure failure prevents the batch from
reaching a consistent, validated state, the workflow must not create a success
commit for that incomplete state.

After each commit, recompute the backlog from `papers.csv` and generated files.
The run may continue with another bounded batch only while the workflow's
remaining deadline and budgets permit.

### 9. Summarize

Write a GitHub Actions job summary containing, at minimum:

- fetched, accepted, deduplicated, and rejected counts by source;
- inventory, generated, pending, attempted, succeeded, failed, and promoted-to-
  fixme counts;
- request, normalization, conversion, formatting, and total timings;
- cap, retry, deadline, and continuation events;
- failure classification and the paths of generated `.fixme.txt` files.

## Template Updates

Template releases are versioned. `.copier-answers.yml` records the template
source and version used by each generated repository.

A scheduled workflow checks for a newer template release. When one exists, it:

1. runs `copier update` against that explicit release;
2. runs schema validation, pre-commit, and the offline test suite;
3. opens a pull request containing the regenerated changes and validation
   results.

The update workflow never pushes template changes directly to `main`. Humans
review update conflicts and the validation result in the pull request. Corpus
and cache exclusions remain outside Copier ownership during update.

## Workflow Security and Reliability

All GitHub Actions are pinned to immutable commit SHAs. Jobs and material steps
have explicit timeouts. Workflow permissions use least privilege: read-only by
default, with contents and pull-request write permissions granted only to jobs
that must commit nightly generated batches or open template-update pull
requests. Secrets are passed only to adapters that declare them and are never
written to summaries, fixtures, logs, or generated files.

Concurrency controls prevent overlapping scheduled mutation runs in one
repository. Cancellation and reruns are safe because inventory and backlog are derived
from committed CSV and generated-file state, while cursors and failure counters
are committed separately as bounded operational state.

## Test Strategy

### Adapter contracts

Run the shared adapter contract suite against recorded fixtures for every
source. Tests cover pagination, continuation, incremental windows, request
timeouts, retryable and permanent errors, malformed records, and credential
requirements. The default test suite is offline and deterministic.

### Pipeline units

Use table-driven tests for:

- normalization;
- stable identifier and bibliographic deduplication;
- source precedence;
- declarative topic gates and the optional plugin boundary;
- backlog inference;
- deterministic count-and-cost batch selection;
- deadline and retry behavior;
- continuation after partial success and cancellation.

### Conversion integration

Use tiny checked-in HTML, LaTeX, and PDF fixtures. Assert successful conversion,
per-paper failure isolation, promotion after three consecutive failures, and
explicit infrastructure failure. Instrument conversion tests to prove HTML and
LaTeX stay within their configured limits and that no two Marker/PyTorch PDF
conversions overlap.

### Formatting and workflows

Assert that nightly formatting receives only the changed/generated paths and
changed indexes. Separately test deterministic sharding in the manual
whole-corpus formatter.

The template smoke test generates a sample repository with Copier and runs
pre-commit plus the complete offline suite in that generated repository. This
test guards the rendered result rather than only the template source.

## Migration Plan

Migrate in increasing order of operational risk:

1. Generate and validate a fixture repository.
2. Migrate `lipsync-papers`.
3. Migrate `tts-papers`.
4. Migrate `asr-papers`.
5. Migrate `birdclef-papers`.

For every real repository:

- preserve the existing corpus and Git history;
- add `papers.yml`, `.papers-state.yml`, and `.copier-answers.yml`;
- reconcile existing repository-specific behavior into declarative
  configuration or, only when necessary, the narrow topic plugin;
- keep `papers/`, `papers.csv`, and caches outside template ownership;
- initially leave the scheduled workflow disabled;
- run the manual end-to-end workflow successfully, inspect its summary and
  commits, and verify continuation on remaining backlog;
- re-enable the schedule only after that manual validation succeeds.

Migration does not regenerate an already converted corpus merely to adopt the
template. Existing generated files satisfy backlog inference and remain
untouched unless their source data or an explicit maintenance operation changes
them.

## Acceptance Criteria

The design is complete when its implementation can demonstrate all of the
following:

- a generated repository operates independently with validated `papers.yml`,
  bounded operational state, and recorded Copier provenance;
- all six adapter families satisfy the same recorded-fixture contract;
- fetching and conversion are bounded by explicit deadlines, pagination,
  count, cost, retry, and concurrency limits;
- Marker/PyTorch PDF conversion never exceeds one concurrent worker;
- a nightly run formats only changed/generated files and changed indexes;
- successful papers survive unrelated paper failures, repeated failures produce
  actionable `.fixme.txt` files, and infrastructure failures fail visibly;
- backlog continuation is inferred from CSV versus generated files;
- template updates arrive as validated pull requests and never direct pushes to
  `main`;
- the generated-repository smoke test passes pre-commit and the offline suite;
- each migrated repository passes a manual workflow before scheduling resumes.

## Explicit Non-Goals

- A centralized service or shared runtime database.
- Template ownership of corpus data, generated paper files, or caches.
- Unbounded nightly backfill.
- Concurrent Marker/PyTorch PDF conversion.
- Whole-corpus nightly formatting.
- Automatic merging of template updates.
- Rewriting corpus history during migration.
