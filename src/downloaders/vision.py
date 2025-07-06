import asyncio
import zipfile
from datetime import datetime
from io import BytesIO

import numpy as np
from dateutil.relativedelta import relativedelta

from ..client import Client
from .base import BaseDownloader

DATA_API = "https://data.binance.vision/data/futures/um/monthly/klines"


class VisionDownloader(BaseDownloader):
    def __init__(self):
        pass

    async def download(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ) -> np.ndarray:
        client.set_base_url(DATA_API)

        current = datetime.utcfromtimestamp(start // 1000)
        end_date = datetime.utcfromtimestamp(end // 1000)

        tasks = []
        chunks = []

        while current <= end_date:
            date = current.strftime("%Y-%m")
            filepath = f"/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
            task = asyncio.create_task(client.get(filepath))
            tasks.append(task)
            current += relativedelta(months=1)

        for task in asyncio.as_completed(tasks):
            resp = await task
            if resp.status_code == 404:
                continue
            resp.raise_for_status()

            chunk = parse_zip(BytesIO(resp.content))
            chunks.append(chunk)

        chunks = np.vstack(chunks)

        return chunks


def parse_zip(raw: BytesIO) -> np.ndarray:
    with zipfile.ZipFile(raw) as z:
        name = z.namelist()[0]
        with z.open(name) as f:
            chunk = np.genfromtxt(f, delimiter=",", skip_header=1, dtype=float)
            return chunk
