import numpy as np

from ..client import Client


class BaseDownloader:
    async def download(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ) -> np.ndarray: ...
