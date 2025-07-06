import numpy as np
import pandas as pd

from .client import Client
from .downloaders import HybridDownloader
from .utils import interval2freq


async def download(symbol: str, interval: str, start: int, end: int) -> pd.DataFrame:
    downloader = HybridDownloader()

    async with Client() as client:
        ohlcv = await downloader.download(client, symbol, interval, start, end)

    df = _create_dataframe(ohlcv, interval)

    return df


def _create_dataframe(ohlcv: np.ndarray, interval: str) -> pd.DataFrame:
    columns = pd.Index(["timestamp", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(ohlcv, columns=columns)

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Europe/Moscow")
    df.set_index("timestamp", inplace=True)

    return df.asfreq(interval2freq(interval))
