from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd


CHANNEL_TO_ID_KEY = "io_channel_to_id"
ID_TO_CHANNEL_KEY = "id_to_io_channel"


def _normalize_channel_map(raw_map: Dict[str, object]) -> Dict[str, int]:
    return {str(key): int(value) for key, value in raw_map.items()}


def _build_reverse_map(channel_map: Dict[str, int]) -> Dict[str, str]:
    return {str(value): key for key, value in channel_map.items()}


def load_password_book(path: str | Path) -> Dict[str, Dict[str, object]]:
    book_path = Path(path)
    if book_path.exists():
        with open(book_path, "r", encoding="utf-8") as f:
            book = json.load(f)
    else:
        book = {}

    channel_map = _normalize_channel_map(book.get(CHANNEL_TO_ID_KEY, {}))
    book[CHANNEL_TO_ID_KEY] = channel_map
    book[ID_TO_CHANNEL_KEY] = _build_reverse_map(channel_map)
    return book


def save_password_book(path: str | Path, book: Dict[str, Dict[str, object]]) -> None:
    book_path = Path(path)
    book_path.parent.mkdir(parents=True, exist_ok=True)
    with open(book_path, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=2)


def update_io_mapping(
    book: Dict[str, Dict[str, object]],
    io_channels: Iterable[object],
) -> Dict[str, int]:
    channel_map = _normalize_channel_map(book.get(CHANNEL_TO_ID_KEY, {}))
    next_id = max(channel_map.values(), default=0) + 1

    normalized_channels = sorted(
        {
            str(value).strip()
            for value in io_channels
            if pd.notna(value) and str(value).strip()
        }
    )

    for channel in normalized_channels:
        if channel not in channel_map:
            channel_map[channel] = next_id
            next_id += 1

    book[CHANNEL_TO_ID_KEY] = channel_map
    book[ID_TO_CHANNEL_KEY] = _build_reverse_map(channel_map)
    return channel_map
