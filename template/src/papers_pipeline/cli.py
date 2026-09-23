"""Command-line entrypoint for the papers pipeline."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Sequence

from papers_pipeline.config import load_config
from papers_pipeline.errors import ConfigError


def app(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline CLI."""

    parser = argparse.ArgumentParser(prog="papers-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--config", type=Path, default=Path("papers.yml"))

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "validate":
        try:
            load_config(args.config, os.environ)
        except ConfigError as error:
            print(f"error: fix {args.config}: {error}", file=sys.stderr)
            return 2
        print(f"valid: {args.config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(app())
