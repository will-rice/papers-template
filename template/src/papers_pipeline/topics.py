import re
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import cast

from pydantic import ValidationError

from papers_pipeline.config import TopicConfig
from papers_pipeline.errors import ConfigError
from papers_pipeline.models import Paper

# Spaces and every Unicode hyphen or dash used between the words of a term.
TERM_SEPARATOR = r"[\s\-\u2010-\u2015]+"


@dataclass(frozen=True)
class TopicDecision:
    accepted: bool
    reason: str


def build_topic_gate(config: TopicConfig) -> Callable[[Paper], TopicDecision]:
    plugin = config.plugin
    if plugin is not None:
        plugin = _validated_plugin_reference(plugin)
        function = _load_plugin(plugin)

        def plugin_gate(paper: Paper) -> TopicDecision:
            decision = function(paper)
            if not isinstance(decision, TopicDecision):
                raise ConfigError("topic plugin must return TopicDecision")
            return decision

        return plugin_gate

    patterns = {
        term: _term_pattern(term)
        for term in (*config.exclude_any, *config.include_all, *config.include_any)
    }

    def gate(paper: Paper) -> TopicDecision:
        text = _search_text(paper)
        for term in config.exclude_any:
            if patterns[term].search(text):
                return TopicDecision(False, f"matched excluded term: {term}")
        missing_required_terms = [
            term for term in config.include_all if not patterns[term].search(text)
        ]
        if missing_required_terms:
            return TopicDecision(
                False,
                f"missing required terms: {', '.join(missing_required_terms)}",
            )
        if config.include_any and not any(
            patterns[term].search(text) for term in config.include_any
        ):
            return TopicDecision(
                False,
                f"missing included term: {', '.join(config.include_any)}",
            )
        if config.categories and not set(config.categories).intersection(
            paper.categories
        ):
            return TopicDecision(
                False,
                f"missing included category: {', '.join(config.categories)}",
            )
        return TopicDecision(True, "accepted")

    return gate


def _term_pattern(term: str) -> re.Pattern[str]:
    """Compile a configured term into a whole-word matcher.

    A term matches whole words, with a simple plural on its last word. Spaces
    and hyphens in a term are interchangeable, so "lip sync" matches "lip-sync".
    A trailing `*` marks a stem that matches any word ending ("vocaliz*").

    Args:
        term: The configured term.

    Returns:
        A pattern to search casefolded text with.

    Raises:
        ConfigError: If `*` appears anywhere but the end, or the term is empty.
    """
    normalized = term.casefold().strip()
    stem = normalized.removesuffix("*")
    words = [word for word in re.split(r"[\s\-]+", stem) if word]
    if not words or "*" in stem:
        raise ConfigError(f"invalid topic term: {term!r}")
    body = TERM_SEPARATOR.join(re.escape(word) for word in words)
    if normalized.endswith("*"):
        return re.compile(rf"(?<!\w){body}")
    if words[-1].endswith("y"):
        body = f"{body[:-1]}(?:y|ies)"
    return re.compile(rf"(?<!\w){body}(?:es|s)?(?!\w)")


def _validated_plugin_reference(plugin: str) -> str:
    try:
        TopicConfig(
            include_any=[],
            include_all=[],
            exclude_any=[],
            categories=[],
            plugin=plugin,
        )
    except ValidationError as error:
        raise ConfigError("topic.plugin must use module:function format") from error
    return plugin


def _load_plugin(plugin: str) -> Callable[[Paper], object]:
    module_name, function_name = plugin.split(":", 1)
    try:
        function = getattr(import_module(module_name), function_name)
    except (ImportError, AttributeError) as error:
        raise ConfigError(f"invalid topic plugin: {plugin}") from error
    if not callable(function):
        raise ConfigError(f"invalid topic plugin: {plugin}")
    return cast(Callable[[Paper], object], function)


def _search_text(paper: Paper) -> str:
    return f"{paper.title} {paper.abstract}".casefold()
