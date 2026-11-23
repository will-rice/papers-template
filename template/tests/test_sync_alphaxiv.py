import json
import subprocess
from pathlib import Path
from typing import Any

import httpx2
import pytest
from mcp import Client
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from papers_pipeline.alphaxiv_sync import (
    SYNC_ATTEMPTS,
    dropped_arxiv_ids,
    main,
    read_arxiv_ids,
    sync,
    sync_collection,
)

COLLECTION = "Speech Enhancement"


def fake_alphaxiv(
    folder_names: list[str],
    *,
    save_error: str | None = None,
) -> tuple[MCPServer, list[dict[str, object]]]:
    """Return an in-process alphaXiv MCP server and the calls it receives."""
    server = MCPServer("alphaxiv")
    folders: dict[str, set[str]] = {
        f"folder-{index}": set() for index in range(len(folder_names))
    }
    names = dict(zip(folders, folder_names, strict=True))
    saves: list[dict[str, object]] = []

    @server.tool(structured_output=False)
    def list_library() -> str:
        return json.dumps(
            {
                "folders": [
                    {
                        "folder_id": folder_id,
                        "name": names[folder_id],
                        "paper_count": len(papers),
                    }
                    for folder_id, papers in folders.items()
                ]
            }
        )

    @server.tool(structured_output=False)
    def save_papers_to_folder(folder_id: str, paper_ids_or_urls: list[str]) -> str:
        if save_error is not None:
            raise ToolError(save_error)
        saves.append({"folder_id": folder_id, "paper_ids_or_urls": paper_ids_or_urls})
        folders[folder_id].update(paper_ids_or_urls)
        return json.dumps({"saved": len(paper_ids_or_urls)})

    @server.tool(structured_output=False)
    def remove_papers_from_folder(folder_id: str, paper_ids_or_urls: list[str]) -> str:
        saves.append(
            {"removed_from": folder_id, "paper_ids_or_urls": paper_ids_or_urls}
        )
        folders[folder_id].difference_update(paper_ids_or_urls)
        return json.dumps({"removed": paper_ids_or_urls})

    return server, saves


def write_inventory(path: Path, *arxiv_ids: str) -> None:
    """Write and commit an inventory, so the sync can read its history."""
    rows = "".join(f"arxiv,{arxiv_id}\n" for arxiv_id in arxiv_ids)
    path.write_text(f"source,arxiv_id\ndblp,\n{rows}", encoding="utf-8")
    if not (path.parent / ".git").exists():
        subprocess.run(["git", "init", "-q"], cwd=path.parent, check=True)
    subprocess.run(["git", "add", path.name], cwd=path.parent, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@localhost",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "inventory",
        ],
        cwd=path.parent,
        check=True,
    )


def test_read_arxiv_ids_keeps_any_source_deduplicates_and_drops_versions(
    tmp_path: Path,
) -> None:
    inventory = tmp_path / "papers.csv"
    inventory.write_text(
        "source,arxiv_id\n"
        "dblp,\n"
        "arxiv,2608.26403v1\n"
        "huggingface,2608.26403v2\n"
        "semantic_scholar,2608.28493v2\n"
        "arxiv,hep-th/9901001v3\n",
        encoding="utf-8",
    )

    assert read_arxiv_ids(inventory) == [
        "2608.26403",
        "2608.28493",
        "hep-th/9901001",
    ]


async def test_sync_collection_saves_inventory_in_batches(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    arxiv_ids = [f"2609.{index:05d}v1" for index in range(51)]
    write_inventory(inventory, *arxiv_ids)
    server, saves = fake_alphaxiv(["Other", COLLECTION])

    async with Client(server, raise_exceptions=True) as client:
        await sync_collection(inventory, COLLECTION, client)

    expected = [arxiv_id.removesuffix("v1") for arxiv_id in arxiv_ids]
    assert saves == [
        {"folder_id": "folder-1", "paper_ids_or_urls": expected[:50]},
        {"folder_id": "folder-1", "paper_ids_or_urls": expected[50:]},
    ]


def test_dropped_arxiv_ids_reads_rows_removed_from_history(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.00001v1", "2608.00002v1", "2608.00003v1")
    # 2608.00002 is dropped; 2608.00003 only gains a version.
    write_inventory(inventory, "2608.00001v1", "2608.00003v2", "2608.00004v1")

    assert dropped_arxiv_ids(inventory, read_arxiv_ids(inventory)) == ["2608.00002"]


def test_stripped_old_style_ids_never_reach_alphaxiv(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "9812006v1", "9608012v1", "2608.00001v1")
    assert read_arxiv_ids(inventory) == ["2608.00001"]

    write_inventory(inventory, "cs/9812006v1", "9608012v1", "2608.00001v1")

    current = read_arxiv_ids(inventory)
    assert current == ["cs/9812006", "2608.00001"]
    assert dropped_arxiv_ids(inventory, current) == []


async def test_sync_collection_removes_dropped_papers(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.00001v1", "2608.00002v1")
    write_inventory(inventory, "2608.00001v1")
    server, calls = fake_alphaxiv([COLLECTION])

    async with Client(server, raise_exceptions=True) as client:
        await sync_collection(inventory, COLLECTION, client)

    assert calls == [
        {"folder_id": "folder-0", "paper_ids_or_urls": ["2608.00001"]},
        {"removed_from": "folder-0", "paper_ids_or_urls": ["2608.00002"]},
    ]


@pytest.mark.parametrize("folder_names", [["Other"], [COLLECTION, COLLECTION]])
async def test_sync_collection_requires_exactly_one_folder(
    tmp_path: Path,
    folder_names: list[str],
) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.26403v1")
    server, saves = fake_alphaxiv(folder_names)

    async with Client(server) as client:
        with pytest.raises(ValueError, match="expected one alphaXiv folder"):
            await sync_collection(inventory, COLLECTION, client)
    assert saves == []


async def test_sync_collection_reports_tool_errors(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.26403v1")
    server, _ = fake_alphaxiv([COLLECTION], save_error="write failed")

    async with Client(server) as client:
        with pytest.raises(ValueError, match="save_papers_to_folder failed: .*write"):
            await sync_collection(inventory, COLLECTION, client)


def alphaxiv_http_stub(
    failures: dict[str, list[httpx2.Response]],
    *,
    tool_is_error: bool = False,
    folder_name: str = COLLECTION,
) -> tuple[httpx2.MockTransport, list[str]]:
    """Return an MCP HTTP edge that first answers each method with its failures."""
    posted: list[str] = []

    def handle(request: httpx2.Request) -> httpx2.Response:
        if request.method != "POST":
            return httpx2.Response(405)
        message = json.loads(request.content)
        method = message["method"]
        tool = message["params"]["name"] if method == "tools/call" else None
        posted.append(tool or method)
        if "id" not in message:
            return httpx2.Response(202)
        if failures.get(method):
            return failures[method].pop(0)
        if method == "server/discover":
            error = {"code": -32601, "message": "Method not found"}
            return httpx2.Response(
                200, json={"jsonrpc": "2.0", "id": message["id"], "error": error}
            )
        if method == "initialize":
            result: dict[str, object] = {
                "protocolVersion": message["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "alphaxiv", "version": "1"},
            }
        elif method == "tools/list":
            result = {"tools": []}
        elif tool == "list_library":
            folders = [{"folder_id": "folder-0", "name": folder_name}]
            text = json.dumps({"folders": folders})
            result = {"content": [{"type": "text", "text": text}], "isError": False}
        else:
            result = {
                "content": [{"type": "text", "text": '{"saved": 1}'}],
                "isError": tool_is_error,
            }
        reply = {"jsonrpc": "2.0", "id": message["id"], "result": result}
        return httpx2.Response(200, json=reply)

    return httpx2.MockTransport(handle), posted


async def no_wait(_: float) -> None:
    """Skip the delay between sync attempts."""


@pytest.mark.parametrize(
    ("method", "failure"),
    [
        ("initialize", httpx2.Response(504)),
        (
            "tools/call",
            httpx2.Response(
                200, headers={"content-type": "text/event-stream"}, content=b""
            ),
        ),
    ],
    ids=["gateway-timeout-during-setup", "sse-ended-during-tool-call"],
)
async def test_sync_retries_transient_failures(
    tmp_path: Path, method: str, failure: httpx2.Response
) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.26403v1")
    transport, posted = alphaxiv_http_stub({method: [failure]})

    await sync(inventory, COLLECTION, "key", transport=transport, sleep=no_wait)

    assert posted.count("initialize") == 2
    assert "save_papers_to_folder" in posted


async def test_sync_gives_up_after_bounded_attempts(tmp_path: Path) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.26403v1")
    transport, posted = alphaxiv_http_stub(
        {"initialize": [httpx2.Response(504) for _ in range(99)]}
    )

    with pytest.raises(ExceptionGroup):
        await sync(inventory, COLLECTION, "key", transport=transport, sleep=no_wait)

    assert posted.count("initialize") == SYNC_ATTEMPTS


@pytest.mark.parametrize(
    ("stub_options", "failures"),
    [
        ({"tool_is_error": True}, {}),
        ({"folder_name": "Other"}, {}),
        ({}, {"initialize": [httpx2.Response(401)]}),
    ],
    ids=["tool-error", "missing-folder", "rejected-api-key"],
)
async def test_sync_does_not_retry_permanent_failures(
    tmp_path: Path,
    stub_options: dict[str, Any],
    failures: dict[str, list[httpx2.Response]],
) -> None:
    inventory = tmp_path / "papers.csv"
    write_inventory(inventory, "2608.26403v1")
    transport, posted = alphaxiv_http_stub(failures, **stub_options)

    with pytest.raises((ValueError, ExceptionGroup)):
        await sync(inventory, COLLECTION, "key", transport=transport, sleep=no_wait)

    assert posted.count("initialize") == 1


@pytest.mark.parametrize(
    ("api_key", "collection"), [("", COLLECTION), ("axv2_test-key", "")]
)
def test_main_rejects_empty_configuration(
    monkeypatch: pytest.MonkeyPatch,
    api_key: str,
    collection: str,
) -> None:
    monkeypatch.setenv("ALPHAXIV_API_KEY", api_key)
    monkeypatch.setenv("ALPHAXIV_COLLECTION", collection)

    with pytest.raises(ValueError, match="must be set"):
        main()
