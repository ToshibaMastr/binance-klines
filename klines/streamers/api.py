from asyncio import as_completed, create_task

import numpy as np

from ..client import Client
from ..utils import interval2ms
from .base import BaseStreamer

APIS = [f"https://api{i}.binance.com" for i in range(1, 5)]
LIMIT = 1000


class ApiStreamer(BaseStreamer):
    async def stream(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ):
        client.set_base_url(APIS[1])
        step = interval2ms(interval)

        tasks = []
        for current in range(start, end, step * LIMIT):
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current,
                "endTime": current + step * LIMIT,
                "limit": LIMIT,
            }
            tasks.append(create_task(client.get("/api/v3/klines", params)))

        for task in as_completed(tasks):
            resp = await task
            resp.raise_for_status()
            data = resp.json()
            yield np.array(data, dtype=np.float64)
