#!/usr/bin/env bash
# Build the environment papers are converted in. The nightly and CI both run
# this script, so CI proves the exact environment, from the same lock, before
# a nightly run depends on it. Extra arguments go to `uv sync`.
set -euo pipefail

MODEL_DOWNLOAD_ATTEMPTS=4
MODEL_DOWNLOAD_RETRY_SECONDS=30

# torch is a large download; a slow mirror must not time it out.
export UV_HTTP_TIMEOUT=300
# The convert extra holds docling and pandoc, pinned by uv.lock.
uv sync --locked --extra convert "$@"

# Fetch docling's models now, so a failed download stops the job here instead
# of failing a paper in the middle of a run. docling hardcodes its RapidOCR
# download timeout (docling/utils/utils.py:53) and skips files it already has,
# so a retry resumes after a slow CDN read.
for ((attempt = 1; ; attempt++)); do
  download_status=0
  uv run docling-tools models download layout tableformer rapidocr ||
    download_status=$?
  if ((download_status == 0)); then
    break
  fi
  if ((attempt == MODEL_DOWNLOAD_ATTEMPTS)); then
    echo "Model download failed after $attempt attempts" >&2
    exit "$download_status"
  fi
  echo "Model download attempt $attempt failed; retrying in ${MODEL_DOWNLOAD_RETRY_SECONDS}s" >&2
  sleep "$MODEL_DOWNLOAD_RETRY_SECONDS"
done
echo "DOCLING_ARTIFACTS_PATH=$HOME/.cache/docling/models" >> "$GITHUB_ENV"

npm install --global prettier@3.6.2

mkdir -p "$HOME/.local/bin"
ln -sf "$(uv run python -c 'import pypandoc; print(pypandoc.get_pandoc_path())')" \
  "$HOME/.local/bin/pandoc"
echo "$HOME/.local/bin" >> "$GITHUB_PATH"
