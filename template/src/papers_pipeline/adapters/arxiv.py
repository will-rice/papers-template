import base64
import json
import re
from datetime import datetime, timezone
from xml.etree import ElementTree

from papers_pipeline.adapters.base import FetchPage, FetchWindow, collect_records
from papers_pipeline.config import AdapterConfig
from papers_pipeline.errors import InfrastructureError, PaperError
from papers_pipeline.http import RequestClient
from papers_pipeline.models import SourceRecord

ATOM = {
    "a": "http://www.w3.org/2005/Atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}


class ArxivAdapter:
    name = "arxiv"
    record_sources = frozenset({"arxiv"})

    async def fetch(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
    ) -> FetchPage:
        return await self._fetch_page(
            window, cursor, client, config, sweep_updates=False
        )

    async def fetch_forward(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
    ) -> FetchPage:
        """Fetch papers first submitted in the window, then those updated in it.

        The API has no filter on a paper's latest version date: ``lastUpdatedDate``
        in ``search_query`` matches the v1 date. Its documented ``sortBy`` value does
        order by the latest version, so the second phase walks that order newest
        first and stops at the window start. A paper updated during the walk moves
        to the front, which can repeat an entry but cannot skip one.
        """
        return await self._fetch_page(
            window, cursor, client, config, sweep_updates=True
        )

    async def _fetch_page(
        self,
        window: FetchWindow,
        cursor: str | None,
        client: RequestClient,
        config: AdapterConfig,
        *,
        sweep_updates: bool,
    ) -> FetchPage:
        state = _decode_cursor(cursor)
        sweeping = bool(state["updates"])
        query = config.filters.get("search_query", "all:*")
        text = await client.get_text(
            "https://export.arxiv.org/api/query",
            {
                "search_query": query if sweeping else _windowed_query(query, window),
                "start": str(state["start"]),
                "max_results": str(config.page_size),
                "sortBy": "lastUpdatedDate" if sweeping else "submittedDate",
                "sortOrder": "descending" if sweeping else "ascending",
            },
            {},
            is_incomplete=lambda body: _is_empty_before_total(body, state["start"]),
        )
        try:
            root = ElementTree.fromstring(text)
        except ElementTree.ParseError as error:
            raise InfrastructureError(f"invalid XML from arxiv: {error}") from error

        entries = tuple(root.findall("a:entry", ATOM))
        past_window = False
        if sweeping:
            kept, date_errors, past_window = _entries_updated_in_window(entries, window)
            records, record_errors = collect_records(kept, self._record)
            errors = (*date_errors, *record_errors)
        else:
            parsed_records, errors = collect_records(entries, self._record)
            records = tuple(
                record
                for record in parsed_records
                if window.start <= record.published <= window.end
            )
        if not entries or past_window:
            return FetchPage(
                records=records,
                next_cursor=(
                    _encode_cursor({"consumed": 0, "page": 0, "start": 0, "updates": 1})
                    if sweep_updates and not sweeping and not entries
                    else None
                ),
                capped=False,
                permanent_errors=errors,
            )

        next_state = {
            "consumed": state["consumed"] + len(entries),
            "page": state["page"] + 1,
            "start": state["start"] + len(entries),
        }
        if sweeping:
            next_state["updates"] = 1
        next_cursor = _encode_cursor(next_state)
        capped = len(entries) >= config.max_results
        return FetchPage(
            records=records,
            next_cursor=next_cursor,
            capped=capped,
            permanent_errors=errors,
        )

    def _record(self, entry: ElementTree.Element) -> SourceRecord:
        identifier = _required_identifier(entry)
        published = _required_datetime(
            _find_text(entry, "a:published"), field="published", identifier=identifier
        )
        title = _required_text(
            _find_text(entry, "a:title"), field="title", identifier=identifier
        )
        abstract = _required_text(
            _find_text(entry, "a:summary"), field="summary", identifier=identifier
        )
        authors = tuple(
            author
            for author in (
                _clean(node.findtext("a:name", namespaces=ATOM, default=""))
                for node in entry.findall("a:author", ATOM)
            )
            if author
        )
        if not authors:
            raise PaperError(f"arxiv record missing authors: {identifier}")

        categories = tuple(
            category
            for category in (
                _clean(node.attrib.get("term", ""))
                for node in entry.findall("a:category", ATOM)
            )
            if category
        )
        return SourceRecord(
            source=self.name,
            source_id=identifier,
            arxiv_id=identifier,
            title=title,
            abstract=abstract,
            authors=authors,
            published=published,
            url=f"https://arxiv.org/abs/{identifier}",
            input_format="pdf",
            input_url=f"https://arxiv.org/pdf/{identifier}",
            categories=categories,
        )


def _entries_updated_in_window(
    entries: tuple[ElementTree.Element, ...], window: FetchWindow
) -> tuple[list[ElementTree.Element], list[str], bool]:
    """Return entries updated in the window, date errors, and whether the walk ended."""
    kept: list[ElementTree.Element] = []
    errors: list[str] = []
    past_window = False
    for entry in entries:
        try:
            updated = _required_datetime(
                _find_text(entry, "a:updated"),
                field="updated",
                identifier=_required_identifier(entry),
            )
        except PaperError as error:
            errors.append(str(error))
            continue
        past_window = past_window or updated < window.start
        if window.start <= updated <= window.end:
            kept.append(entry)
    return kept, errors, past_window


def _is_empty_before_total(text: str, start: int) -> bool:
    """Return whether a page has no entries although results remain after start."""
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        return False
    total = root.findtext("opensearch:totalResults", namespaces=ATOM)
    return (
        root.find("a:entry", ATOM) is None and total is not None and start < int(total)
    )


def _encode_cursor(state: dict[str, int]) -> str:
    payload = json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> dict[str, int]:
    if cursor is None:
        return {"consumed": 0, "page": 0, "start": 0, "updates": 0}
    padding = "=" * (-len(cursor) % 4)
    try:
        decoded = base64.urlsafe_b64decode(f"{cursor}{padding}".encode("ascii"))
        data = json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError) as error:
        raise InfrastructureError(
            f"invalid arxiv continuation cursor: {cursor}"
        ) from error
    if not isinstance(data, dict):
        raise InfrastructureError(f"invalid arxiv continuation cursor: {cursor}")
    consumed = data.get("consumed")
    page = data.get("page")
    start = data.get("start")
    # Cursors saved before the update sweep existed have no phase.
    updates = data.get("updates", 0)
    if consumed is None and isinstance(start, int):
        consumed = start
    if (
        not isinstance(consumed, int)
        or consumed < 0
        or not isinstance(page, int)
        or page < 0
        or not isinstance(start, int)
        or start < 0
        or updates not in (0, 1)
    ):
        raise InfrastructureError(f"invalid arxiv continuation cursor: {cursor}")
    return {"consumed": consumed, "page": page, "start": start, "updates": updates}


def _required_identifier(entry: ElementTree.Element) -> str:
    raw_identifier = _required_text(
        _find_text(entry, "a:id"), field="id", identifier="<unknown>"
    )
    normalized = re.sub(
        r"(?i)^arxiv:", "", raw_identifier.rstrip("/").rsplit("/abs/", 1)[-1]
    )
    identifier = _clean(normalized)
    if not identifier:
        raise PaperError("arxiv record missing id: <unknown>")
    return identifier


def _find_text(entry: ElementTree.Element, path: str) -> str:
    return entry.findtext(path, namespaces=ATOM, default="")


def _required_datetime(value: str, *, field: str, identifier: str) -> datetime:
    if not value:
        raise PaperError(f"arxiv record missing {field} timestamp: {identifier}")
    try:
        published = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise PaperError(
            f"arxiv record invalid {field} timestamp: {identifier}"
        ) from error
    if published.tzinfo is None or published.utcoffset() is None:
        raise PaperError(f"arxiv record invalid {field} timestamp: {identifier}")
    return published


def _required_text(value: str, *, field: str, identifier: str) -> str:
    cleaned = _clean(value)
    if not cleaned:
        raise PaperError(f"arxiv record missing {field}: {identifier}")
    return cleaned


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _windowed_query(query: str, window: FetchWindow) -> str:
    start = window.start.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
    end = window.end.astimezone(timezone.utc).strftime("%Y%m%d%H%M")
    return f"({query}) AND submittedDate:[{start} TO {end}]"
