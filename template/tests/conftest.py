from __future__ import annotations

from datetime import datetime, timezone

import pytest

from papers_pipeline.config import TopicConfig
from papers_pipeline.models import Paper, SourceRecord


@pytest.fixture
def source_record() -> SourceRecord:
    return SourceRecord(
        source="semantic_scholar",
        source_id="semantic-1",
        title=" Neural   Speech\nRecognition ",
        abstract=" A system for speech understanding. ",
        authors=(" Ada  Lovelace ", " Grace\nHopper "),
        published=datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc),
        url="https://example.test/papers/semantic-1",
        input_format="pdf",
        input_url="https://example.test/papers/semantic-1.pdf",
        categories=("cs.CL", "cs.AI", "cs.CL"),
        doi=None,
        arxiv_id=None,
    )


@pytest.fixture
def paper() -> Paper:
    return Paper(
        identifier="semantic_scholar:semantic-1",
        title="Neural Speech Recognition",
        abstract="A system for speech understanding.",
        authors=("Ada Lovelace", "Grace Hopper"),
        published=datetime(2024, 1, 2, 12, 30, tzinfo=timezone.utc),
        url="https://example.test/papers/semantic-1",
        source="semantic_scholar",
        input_format="pdf",
        input_url="https://example.test/papers/semantic-1.pdf",
        categories=("cs.AI", "cs.CL"),
        doi=None,
        arxiv_id=None,
    )


@pytest.fixture
def topic_config() -> TopicConfig:
    return TopicConfig(
        include_any=["speech"],
        include_all=[],
        exclude_any=[],
        categories=[],
        plugin=None,
    )
