#!/usr/bin/env bash
set -euo pipefail

UV_OFFLINE=1 uv sync --locked --offline
UV_OFFLINE=1 uv run pytest \
  tests/test_copier_smoke.py \
  tests/test_template_exclusions.py \
  tests/test_validation_policy.py \
  -v
