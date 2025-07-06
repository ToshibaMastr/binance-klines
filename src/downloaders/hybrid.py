import numpy as np

from ..client import Client
from ..utils import interval2ms
from .api import ApiDownloader
from .base import BaseDownloader
from .vision import VisionDownloader


class HybridDownloader(BaseDownloader):
    def __init__(self):
        self.apid = ApiDownloader()
        self.visiond = VisionDownloader()

    async def download(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ) -> np.ndarray:
        step = interval2ms(interval)

        length = (end - start) // step
        ohlcv = np.full((length, 6), np.nan, dtype=np.float64)

        data = await self.visiond.download(client, symbol, interval, start, end)
        indices = ((data[:, 0] - start) // step).astype(int)
        ohlcv = apply(ohlcv, indices, data)

        starts, ends = splits(ohlcv)

        for st, en in zip(starts, ends):
            st = start + st * step
            en = start + en * step

            data = await self.apid.download(client, symbol, interval, st, en)
            indices = ((data[:, 0] - start) // step).astype(int)
            ohlcv = apply(ohlcv, indices, data)

        return ohlcv


def apply(ohlcv, indices, chunk) -> np.ndarray:
    valid = (indices >= 0) & (indices < len(ohlcv))
    ohlcv[indices[valid]] = chunk[valid, :6]
    return ohlcv


def splits(data) -> tuple[np.ndarray, np.ndarray]:
    mask = np.isnan(data[:, 0])
    changes = np.diff(np.pad(mask.astype(int), (1, 1)))
    return np.where(changes == 1)[0], np.where(changes == -1)[0]
