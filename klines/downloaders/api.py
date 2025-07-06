from asyncio import as_completed, create_task

import numpy as np

from ..client import Client
from ..utils import interval2ms
from .base import BaseDownloader

APIS = [f"https://api{i}.binance.com" for i in range(1, 5)]


class ApiDownloader(BaseDownloader):
    def __init__(self, limit: int = 1000):
        super().__init__()
        self.limit = limit

    async def download(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ) -> np.ndarray:
        client.set_base_url(APIS[1])

        step = interval2ms(interval)

        tasks = []
        collected = 0

        chunks = []
        for current in range(start, end, step * self.limit):
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current,
                "endTime": current + step * self.limit,
                "limit": self.limit,
            }
            collected += self.limit

            tasks.append(create_task(client.get("/api/v3/klines", params)))

        total = 0
        for task in as_completed(tasks):
            resp = await task
            resp.raise_for_status()
            data = resp.json()

            chunk = np.array(data, dtype=np.float64)
            chunks.append(chunk)
            total += len(chunk)

        chunks = np.vstack(chunks)

        return chunks
