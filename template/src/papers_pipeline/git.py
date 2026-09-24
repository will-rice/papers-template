from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from papers_pipeline.errors import CommitCompletedError, InfrastructureError


class GitOperations(Protocol):
    def commit(self, paths: Sequence[Path], message: str) -> str | None: ...

    def assert_clean(self, paths: Sequence[Path]) -> None: ...

    def clear_staging(self, paths: Sequence[Path]) -> None: ...


class GitRepository:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def commit(self, paths: Sequence[Path], message: str) -> str | None:
        relative = self._relative_paths(paths)
        if not relative:
            return None

        try:
            relative = [
                path
                for path in relative
                if (self.root / path).exists() or self._is_tracked(path)
            ]
            if not relative:
                return None
            subprocess.run(
                ["git", "add", "-A", "--", *relative],
                cwd=self.root,
                check=True,
            )
            changed = subprocess.run(
                ["git", "diff", "--cached", "--quiet", "--", *relative],
                cwd=self.root,
                check=False,
            )
            if changed.returncode == 0:
                return None
            if changed.returncode != 1:
                raise subprocess.CalledProcessError(changed.returncode, changed.args)
            old_head = self._head_sha()
            subprocess.run(
                ["git", "commit", "--only", "-m", message, "--", *relative],
                cwd=self.root,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            raise InfrastructureError(f"git commit failed: {message}") from error

        try:
            new_head = self._head_sha(fallback=True)
            if new_head is None or new_head == old_head:
                raise OSError("git commit completed but HEAD did not advance")
            return new_head
        except (OSError, subprocess.CalledProcessError) as error:
            raise CommitCompletedError(
                f"git commit completed but its SHA could not be validated: {message}"
            ) from error

    def assert_clean(self, paths: Sequence[Path]) -> None:
        relative = self._relative_paths(paths)
        if not relative:
            return
        try:
            result = subprocess.run(
                [
                    "git",
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    "--",
                    *relative,
                ],
                cwd=self.root,
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            raise InfrastructureError("git clean-worktree preflight failed") from error
        dirty = result.stdout.strip()
        if dirty:
            raise InfrastructureError(
                "uncommitted changes in pipeline-managed paths; commit or stash "
                f"them before retrying:\n{dirty}"
            )

    def clear_staging(self, paths: Sequence[Path]) -> None:
        relative = self._relative_paths(paths)
        if not relative:
            return
        try:
            staged_output = subprocess.check_output(
                ["git", "diff", "--cached", "--name-only", "-z", "--", *relative],
                cwd=self.root,
            )
            staged = [path.decode() for path in staged_output.split(b"\0") if path]
            if not staged:
                return
            head = subprocess.run(
                ["git", "rev-parse", "--verify", "HEAD"],
                cwd=self.root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if head.returncode == 0:
                subprocess.run(
                    ["git", "restore", "--staged", "--", *staged],
                    cwd=self.root,
                    check=True,
                )
            else:
                subprocess.run(
                    ["git", "rm", "--cached", "--ignore-unmatch", "--", *staged],
                    cwd=self.root,
                    check=True,
                )
        except (OSError, subprocess.CalledProcessError) as error:
            raise InfrastructureError(
                "git staging cleanup failed after transaction failure"
            ) from error

    def _is_tracked(self, path: str) -> bool:
        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--", path],
            cwd=self.root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.returncode == 0

    def _head_sha(self, *, fallback: bool = False) -> str | None:
        try:
            output = subprocess.check_output(
                ["git", "rev-parse", "--verify", "HEAD"],
                cwd=self.root,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except subprocess.CalledProcessError:
            if not fallback:
                return None
            output = subprocess.check_output(
                ["git", "log", "-1", "--format=%H", "HEAD"],
                cwd=self.root,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        sha = output.strip()
        if len(sha) not in (40, 64):
            raise OSError("git returned an invalid commit SHA")
        try:
            int(sha, 16)
        except ValueError as error:
            raise OSError("git returned an invalid commit SHA") from error
        return sha

    def _relative_paths(self, paths: Sequence[Path]) -> list[str]:
        relative: set[str] = set()
        for path in paths:
            absolute = path if path.is_absolute() else self.root / path
            try:
                item = absolute.resolve().relative_to(self.root)
            except ValueError as error:
                raise InfrastructureError(
                    f"git path is outside repository: {path}"
                ) from error
            relative.add(str(item))
        return sorted(relative)
