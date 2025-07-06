import zipfile
from asyncio import as_completed, create_task
from datetime import datetime
from io import BytesIO

import numpy as np
from dateutil.relativedelta import relativedelta

from ..client import Client
from .base import BaseStreamer

DATA_API = "https://data.binance.vision/data/futures/um/monthly/klines"


class VisionStreamer(BaseStreamer):
    async def stream(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ):
        client.set_base_url(DATA_API)

        current = datetime.utcfromtimestamp(start // 1000)
        end_date = datetime.utcfromtimestamp(end // 1000)

        tasks = []
        while current <= end_date:
            date = current.strftime("%Y-%m")
            filepath = f"/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
            tasks.append(create_task(client.get(filepath)))
            current += relativedelta(months=1)

        for task in as_completed(tasks):
            resp = await task
            if resp.status_code == 404:
                continue
            resp.raise_for_status()

            yield self._parse_zip(BytesIO(resp.content))

    @staticmethod
    def _parse_zip(raw: BytesIO) -> np.ndarray:
        with zipfile.ZipFile(raw) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                chunk = np.genfromtxt(f, delimiter=",", skip_header=1, dtype=float)
                return chunk
