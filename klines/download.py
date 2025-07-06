from typing import Literal

import numpy as np
import pandas as pd

from .client import Client
from .downloaders import ApiDownloader, HybridDownloader, VisionDownloader
from .utils import interval2freq

DownloaderType = Literal["hybrid", "api", "vision"]

DOWNLOADERS: dict[DownloaderType, type] = {
    "hybrid": HybridDownloader,
    "api": ApiDownloader,
    "vision": VisionDownloader,
}


async def download(
    symbol: str, interval: str, start: int, end: int, downloader_type: DownloaderType
) -> pd.DataFrame:
    if downloader_type not in DOWNLOADERS:
        raise ValueError(f"Unsupported downloader type: {downloader_type}")

    downloader_class = DOWNLOADERS[downloader_type]
    downloader = downloader_class()

    async with Client() as client:
        ohlcv = await downloader.download(client, symbol, interval, start, end)

    return _create_dataframe(ohlcv, interval)


def _create_dataframe(ohlcv: np.ndarray, interval: str) -> pd.DataFrame:
    columns = pd.Index(["timestamp", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(ohlcv, columns=columns)

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Moscow")
    df.set_index("timestamp", inplace=True)

    return df.asfreq(interval2freq(interval))
