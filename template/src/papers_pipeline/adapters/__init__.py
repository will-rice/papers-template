from __future__ import annotations

from .arxiv import ArxivAdapter
from .base import Adapter, FetchPage, FetchWindow, collect_records
from .dblp import DblpAdapter
from .huggingface import HuggingFaceAdapter
from .semantic_scholar import SemanticScholarAdapter

__all__ = [
    "Adapter",
    "ArxivAdapter",
    "DblpAdapter",
    "FetchPage",
    "FetchWindow",
    "HuggingFaceAdapter",
    "SemanticScholarAdapter",
    "collect_records",
]
