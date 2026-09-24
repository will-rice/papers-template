# Task 16 Report

## Delivered

- Added a weekly and manually dispatched, single-concurrency template update
  workflow that checks out the default branch and only opens a pull request.
- Pinned all actions and update tools, set explicit job/step timeouts, and
  limited permissions to repository contents and pull requests.
- Added an executable update script that accepts only stable immutable release
  tags, requires the existing Copier answers file, rejects older releases,
  exits cleanly when already current, and reports whether a PR is needed.
- Copier updates use the explicit release and answers file. Failures, reject
  files, and invalid diffs stop before locking, syncing, schema validation,
  pre-commit, tests, or pull-request creation.
- Corrected rendered Copier answers to use Copier's remembered metadata,
  including `_commit`, and safe YAML serialization.
- No migrations were added.

## TDD evidence

1. Workflow, script-policy, invalid-ref, and no-op tests were added first and
   failed because the update files and explicit PR gate were absent.
2. The Copier metadata test failed because rendered answers did not record
   `_commit`; YAML parsing then exposed unsafe string serialization.
3. Minimal workflow, script, and answers-template changes made each focused
   test pass.

## Verification

- Focused workflow suite: `30 passed`.
- Full rendered-template suite: `220 passed`.
- Full Copier/root suite: `2 passed` with only expected dirty-template
  warnings.
- Changed-file Ruff check and format check: passed.
- Changed-test strict mypy: passed.
- `bash -n`, YAML parsing, and `git diff --check`: passed.
- Full source mypy retains 2 pre-existing unused-ignore errors in
  `config.py` and `state.py`.
- Full pre-commit reformats unrelated baseline files and reports existing mypy
  findings; those unrelated formatter changes were reverted. Changed-file
  Ruff hooks pass, while the hook's isolated mypy environment still reports
  pre-existing untyped pytest decorators plus the two new parametrized
  decorators. Direct project mypy passes both changed test files.
- `actionlint` was not installed, so the optional check was skipped.

## Policy confirmation

The workflow resolves GitHub's latest published release tag, and the script
independently validates stable release syntax and strict version ordering.
Already-current repositories emit `updated=false`, so the PR step is skipped.
Successful validated updates emit `updated=true`. No update path contains
`git push`; only the SHA-pinned pull-request action can publish changes.

## Credential exposure remediation

- Changed every template-update checkout to `persist-credentials: false` while
  retaining the default-branch ref and full history needed by Copier.
- Split validation from publication. The `validate` job has only
  `contents: read`; release lookup is its sole `GH_TOKEN` consumer, and Copier,
  hooks, pre-commit, and tests receive no GitHub token or persisted Git
  credential.
- The validated index, including new and deleted files, is transferred as a
  binary patch from the read-only job. The `publish` job applies that patch to
  the exact validated base SHA and alone receives `contents: write` and
  `pull-requests: write`.
- The SHA-pinned create-pull-request action is the only step given an explicit
  write token. The workflow remains schedule/manual-only and publishes to a
  dedicated branch, preserving fork isolation and avoiding direct pushes.
- Added a credential policy regression test covering job permissions,
  checkout persistence, token placement, release-resolution isolation, and the
  retained fetch depth/ref.

### Remediation verification

- Credential policy RED: failed on the former workflow-level write permissions.
- Focused policy tests: `2 passed`.
- Full workflow suite: `31 passed`.
- Full rendered-template suite: `221 passed`.
- Copier smoke suite: `2 passed` with expected dirty-template warnings.
- Ruff check/format, Bash syntax, YAML parsing, and `git diff --check`: passed.

### Remaining concern

The public release lookup still uses the validation job's read-only
`GITHUB_TOKEN` to avoid unauthenticated API rate limits. It is scoped to the
single lookup step and is never available to Copier or repository-controlled
validation commands.
