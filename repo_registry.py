from __future__ import annotations

import json
import os
from pathlib import Path

REPO_CONFIG_PATH = os.getenv("REPO_CONFIG_PATH", "./repos.json")


class RepoRegistryError(RuntimeError):
    pass


class RepoRegistry:
    def __init__(self, config_path: str = REPO_CONFIG_PATH):
        self.config_path = config_path
        self._cache: dict | None = None

    def _load(self) -> dict:
        if self._cache is None:
            with open(self.config_path, "r", encoding="utf-8") as fh:
                self._cache = json.load(fh)
        return self._cache

    def get(self, repo_name: str) -> dict:
        data = self._load()
        repo = data.get(repo_name)
        if not repo:
            raise RepoRegistryError(f"Unknown repo '{repo_name}' in {self.config_path}")
        repo_path = repo.get("path")
        if not repo_path or not Path(repo_path).exists():
            raise RepoRegistryError(
                f"Repo '{repo_name}' path is missing or does not exist: {repo_path}"
            )
        return repo
