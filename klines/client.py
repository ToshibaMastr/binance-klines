import asyncio

import httpx


class Client:
    def __init__(self, workers: int = 5):
        limits = httpx.Limits(
            max_keepalive_connections=workers,
            max_connections=workers * 2,
            keepalive_expiry=30.0,
        )
        self.client = httpx.AsyncClient(
            limits=limits,
            http2=True,
        )
        self.semaphore = asyncio.Semaphore(workers)

    def set_base_url(self, base_url: str):
        self.client.base_url = base_url

    async def get(self, endpoint: str, params: dict | None = None) -> httpx.Response:
        async with self.semaphore:
            return await self.client.get(endpoint, params=params)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()
