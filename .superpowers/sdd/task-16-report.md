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
