def interval2ms(interval: str) -> int:
    num = int(interval[:-1])
    unit = interval[-1]

    multipliers = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }

    if unit not in multipliers:
        raise ValueError(f"Unsupported interval unit: {unit}")

    return multipliers[unit] * num * 1000


def interval2freq(interval: str) -> str:
    if interval.endswith("m"):
        return interval[:-1] + "min"
    elif interval.endswith("h"):
        return interval[:-1] + "H"
    elif interval.endswith("d"):
        return interval[:-1] + "D"
    return interval
