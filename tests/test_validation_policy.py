from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).parents[1]


def _pre_commit_config(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_pre_commit_configs_use_only_locked_local_tools() -> None:
    for relative in (".pre-commit-config.yaml", "template/.pre-commit-config.yaml"):
        config = _pre_commit_config(ROOT / relative)
        assert config["repos"] == [
            {
                "repo": "local",
                "hooks": [
                    {
                        "id": "ruff-check",
                        "name": "ruff check",
                        "entry": "ruff check --fix",
                        "language": "system",
                        "types": ["python"],
                    },
                    {
                        "id": "ruff-format",
                        "name": "ruff format",
                        "entry": "ruff format",
                        "language": "system",
                        "types": ["python"],
                    },
                    {
                        "id": "mypy",
                        "name": "mypy",
                        "entry": "mypy",
                        "language": "system",
                        "require_serial": True,
                        "types": ["python"],
                    },
                ],
            }
        ]


def test_smoke_provisions_locked_dependencies_before_offline_validation() -> None:
    smoke = (ROOT / "scripts/smoke-test.sh").read_text(encoding="utf-8")
    smoke_test = (ROOT / "tests/test_copier_smoke.py").read_text(encoding="utf-8")
    update = (ROOT / "template/.github/scripts/template-update.sh").read_text(
        encoding="utf-8"
    )

    assert "uv sync --locked" in smoke
    assert "UV_OFFLINE=1 uv sync" not in smoke
    assert '["uv", "sync", "--locked", "--extra", "dev"]' in smoke_test
    assert 'offline_environment["UV_OFFLINE"] = "1"' in smoke_test
    assert update.index("uv sync --locked --extra dev") < update.index(
        "export UV_OFFLINE=1"
    )
