from dataclasses import dataclass
from typing import Callable, Literal

import numpy as np
import pandas as pd

from .client import Client
from .streamers import ApiStreamer, VisionStreamer
from .utils import interval2freq, interval2ms

StreamerType = Literal["hybrid", "api", "vision"]

STREAMERS: dict[StreamerType, type] = {
    "api": ApiStreamer,
    "vision": VisionStreamer,
}


@dataclass
class ProgressEvent:
    percent: float


class OHLCVDownloader:
    def __init__(
        self, max_workers: int = 4, streamers: list[StreamerType] = ["vision", "api"]
    ):
        self.progress_hook: Callable | None = None
        self.max_workers = max_workers

        self.streamers = [STREAMERS[s]() for s in streamers]

    async def download(
        self, symbol: str, interval: str, start: int, end: int
    ) -> pd.DataFrame:
        async with Client(self.max_workers) as client:
            step = interval2ms(interval)
            length = (end - start) // step
            ohlcv = np.full((length, 6), np.nan, dtype=np.float64)

            for streamer in self.streamers:
                for st, en in zip(*self._splits(ohlcv)):
                    st = start + st * step
                    en = start + en * step

                    async for chunk in streamer.stream(
                        client, symbol, interval, st, en
                    ):
                        indices = ((chunk[:, 0] - start) // step).astype(int)
                        ohlcv = self._apply(ohlcv, indices, chunk)
                        percent = (1 - sum(np.isnan(ohlcv[:, 0])) / length) * 100
                        self._hook_progress(ProgressEvent(percent))

        df = self._create_dataframe(ohlcv, interval)

        return df

    @staticmethod
    def _create_dataframe(ohlcv: np.ndarray, interval: str) -> pd.DataFrame:
        columns = pd.Index(["timestamp", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(ohlcv, columns=columns)

        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Moscow")
        df.set_index("timestamp", inplace=True)

        return df.asfreq(interval2freq(interval))

    @staticmethod
    def _splits(data) -> tuple[np.ndarray, np.ndarray]:
        mask = np.isnan(data[:, 0])
        changes = np.diff(np.pad(mask.astype(int), (1, 1)))
        return np.where(changes == 1)[0], np.where(changes == -1)[0]

    @staticmethod
    def _apply(ohlcv, indices, chunk) -> np.ndarray:
        valid = (indices >= 0) & (indices < len(ohlcv))
        ohlcv[indices[valid]] = chunk[valid, :6]
        return ohlcv

    def _hook_progress(self, status: ProgressEvent):
        if self.progress_hook:
            self.progress_hook(status)

    def set_progress_hook(self, progress_hook: Callable):
        self.progress_hook = progress_hook
