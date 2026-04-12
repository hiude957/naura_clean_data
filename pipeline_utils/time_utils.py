from __future__ import annotations

import re

import pandas as pd


_COMPACT_MILLISECOND_PATTERN = re.compile(
    r"^(\d{4}[/-]\d{1,2}[/-]\d{1,2}\s+\d{1,2}:\d{2}:)(\d{3,8})$"
)
_SUPPORTED_TIME_FORMATS = [
    "%Y/%m/%d %H/%M/%S.%f",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
]


def normalize_compact_millisecond_text(series: pd.Series) -> pd.Series:
    def repl(match: re.Match[str]) -> str:
        prefix = match.group(1)
        tail = match.group(2)
        if len(tail) < 3:
            return match.group(0)
        seconds = tail[:2]
        fraction = tail[2:]
        if not fraction:
            return f"{prefix}{seconds}"
        return f"{prefix}{seconds}.{fraction}"

    return series.astype(str).str.strip().str.replace(
        _COMPACT_MILLISECOND_PATTERN,
        repl,
        regex=True,
    )


def robust_to_datetime(series: pd.Series) -> pd.Series:
    normalized = normalize_compact_millisecond_text(series)
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    for fmt in _SUPPORTED_TIME_FORMATS:
        parsed = pd.to_datetime(normalized, format=fmt, errors="coerce")
        mask = out.isna() & parsed.notna()
        if mask.any():
            out.loc[mask] = parsed.loc[mask]

    mask = out.isna()
    if mask.any():
        out.loc[mask] = pd.to_datetime(normalized[mask], errors="coerce")
    return out


def format_timestamp_millis(value: pd.Timestamp) -> str:
    if pd.isna(value):
        return ""
    millis = value.microsecond // 1000
    return (
        f"{value.year}/{value.month}/{value.day} "
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{millis:03d}"
    )
