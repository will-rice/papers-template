#!/usr/bin/env bash
set -euo pipefail

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

pipeline_status=0
uv run papers-pipeline nightly --config papers.yml || pipeline_status=$?

managed_paths=(
  "papers"
  "papers.csv"
  ".papers-state.yml"
  "README.md"
  ".convert-batch"
  "inputs"
)
managed_status="$(
  git status --porcelain=v1 --untracked-files=all -- "${managed_paths[@]}"
)"
if [[ -n "$managed_status" ]]; then
  printf '%s\n' \
    "Refusing to push because pipeline-managed files have uncommitted changes:" \
    "$managed_status" >&2
  if ((pipeline_status == 0)); then
    pipeline_status=1
  fi
  exit "$pipeline_status"
fi

push_status=0
git push origin HEAD:main || push_status=$?
if ((pipeline_status != 0)); then
  exit "$pipeline_status"
fi
exit "$push_status"
