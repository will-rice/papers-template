from __future__ import annotations

import httpx
import pytest


@pytest.mark.asyncio
async def test_fixture_transport_returns_recorded_response(
    fixture_transport,
) -> None:
    transport = fixture_transport({"https://example.test/papers": (200, "ok")})

    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get("https://example.test/papers")

    assert response.status_code == 200
    assert response.text == "ok"
