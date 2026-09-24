#!/usr/bin/env bash
set -euo pipefail

uv sync --locked
uv run pytest tests/test_copier_smoke.py tests/test_template_exclusions.py -v
