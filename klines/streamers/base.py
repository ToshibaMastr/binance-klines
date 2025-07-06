from typing import AsyncIterator, Callable

import numpy as np

from ..client import Client


class BaseStreamer:
    def __init__(self):
        self._progress_hooks: list[Callable] = []

    async def stream(
        self, client: Client, symbol: str, interval: str, start: int, end: int
    ) -> AsyncIterator[np.ndarray]:
        raise NotImplementedError
        yield np.array([], dtype=np.float64)
