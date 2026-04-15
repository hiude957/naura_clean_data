#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


DAY_PREFIX_RE = re.compile(r"^(\d{8})")
DEFAULT_INPUT_ROOT = Path("/home/lyh/naura/anonymized(0316~0407)")
DEFAULT_OUTPUT_ROOT = Path("/home/lyh/naura/aligned_output")
APC_PREFIX = "Process Start Time: "
TIMESTAMP_FMT = "%Y/%m/%d %H:%M:%S.%f"
UNIX_EPOCH = datetime(1970, 1, 1)
INTERPOLATION_STEP_MS = 110

SOURCE_CODES = {
    "apc": 1,
    "livedata": 2,
    "logapc": 3,
    "loglivedata": 4,
}


@dataclass
class SensorRow:
    ts_ms: int
    timestamp_raw: str
    source: str
    values: List[Optional[float]]
    event_map: Dict[int, float] = field(default_factory=dict)
    seq: int = 0


@dataclass(frozen=True)
class WindowIndex:
    windows: List[Tuple[int, int]]
    starts: List[int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily aligned APC/Livedata/Log datasets.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--days", nargs="*", default=[])
    return parser.parse_args()


def normalize_day_sort_key(name: str) -> Tuple[str, str]:
    match = DAY_PREFIX_RE.match(name)
    return (match.group(1) if match else name, name)


def discover_days(root: Path, selected_days: Iterable[str]) -> List[Path]:
    selected = {day.strip() for day in selected_days if day.strip()}
    if not root.exists():
        raise FileNotFoundError(f"Input root not found: {root}")
    days = [path for path in root.iterdir() if path.is_dir()]
    days.sort(key=lambda path: normalize_day_sort_key(path.name))
    if not selected:
        return days
    return [path for path in days if path.name in selected or normalize_day_sort_key(path.name)[0] in selected]


def parse_log_timestamp(text: str) -> datetime:
    return datetime.strptime(text, TIMESTAMP_FMT)


def parse_livedata_timestamp(text: str) -> datetime:
    if "." in text:
        return datetime.strptime(text, TIMESTAMP_FMT)
    return datetime.strptime(text, "%Y/%m/%d %H:%M:%S:%f")


def format_timestamp_raw(value: datetime) -> str:
    millis = value.microsecond // 1000
    return (
        f"{value.year}/{value.month}/{value.day} "
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{millis:03d}"
    )


def to_unix_ms(value: datetime) -> int:
    return int((value - UNIX_EPOCH).total_seconds() * 1000)


def float_or_none(text: str) -> Optional[float]:
    raw = text.strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def load_apc_rows(file_path: Path, expected_headers: Optional[List[str]], seq_base: int) -> Tuple[List[str], List[SensorRow], Tuple[int, int]]:
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        first_line = f.readline().rstrip("\n").rstrip("\r")
        if not first_line.startswith(APC_PREFIX):
            raise ValueError(f"APC file missing process start header: {file_path}")
        start_dt = datetime.strptime(first_line[len(APC_PREFIX):].strip(), "%Y/%m/%d %H:%M:%S")

        header_line = ""
        for line in f:
            if line.startswith("Time\t"):
                header_line = line.rstrip("\n").rstrip("\r")
                break
        if not header_line:
            raise ValueError(f"APC file missing tabular header: {file_path}")

        header = header_line.split("\t")
        if not header or header[0] != "Time":
            raise ValueError(f"Unexpected APC header in {file_path}")
        data_headers = header[1:]
        if expected_headers is not None and data_headers != expected_headers:
            raise ValueError(f"Sensor header mismatch in {file_path}")

        rows: List[SensorRow] = []
        for row_idx, line in enumerate(f):
            raw_line = line.rstrip("\n").rstrip("\r")
            if not raw_line.strip():
                continue
            parts = raw_line.split("\t")
            if len(parts) != len(header):
                continue
            offset_s = float(parts[0])
            ts_dt = start_dt + timedelta(seconds=offset_s)
            rows.append(
                SensorRow(
                    ts_ms=to_unix_ms(ts_dt),
                    timestamp_raw=format_timestamp_raw(ts_dt),
                    source="apc",
                    values=[float_or_none(value) for value in parts[1:]],
                    seq=seq_base + row_idx,
                )
            )

    if not rows:
        raise ValueError(f"APC file has no rows: {file_path}")
    return data_headers, rows, (rows[0].ts_ms, rows[-1].ts_ms)


def load_livedata_rows(file_path: Path, expected_headers: Optional[List[str]], seq_base: int) -> Tuple[List[str], List[SensorRow]]:
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader, None)
        if not header or header[0] != "Time":
            raise ValueError(f"Unexpected livedata header in {file_path}")
        data_headers = header[1:]
        if expected_headers is not None and data_headers != expected_headers:
            raise ValueError(f"Sensor header mismatch in {file_path}")

        rows: List[SensorRow] = []
        for row_idx, parts in enumerate(reader):
            if not parts:
                continue
            if len(parts) != len(header):
                continue
            ts_dt = parse_livedata_timestamp(parts[0])
            rows.append(
                SensorRow(
                    ts_ms=to_unix_ms(ts_dt),
                    timestamp_raw=format_timestamp_raw(ts_dt),
                    source="livedata",
                    values=[float_or_none(value) for value in parts[1:]],
                    seq=seq_base + row_idx,
                )
            )
    return data_headers, rows


def read_sensor_headers(file_path: Path) -> List[str]:
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        first_line = f.readline().rstrip("\n").rstrip("\r")
        if first_line.startswith(APC_PREFIX):
            header_line = ""
            for line in f:
                if line.startswith("Time\t"):
                    header_line = line.rstrip("\n").rstrip("\r")
                    break
            if not header_line:
                raise ValueError(f"APC file missing tabular header: {file_path}")
        else:
            header_line = first_line

    header = header_line.split("\t")
    if not header or header[0] != "Time":
        raise ValueError(f"Unexpected sensor header in {file_path}")
    return header[1:]


def merge_windows(windows: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not windows:
        return []
    merged: List[Tuple[int, int]] = []
    for start, end in sorted(windows):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def build_window_index(windows: Sequence[Tuple[int, int]]) -> WindowIndex:
    merged = merge_windows(windows)
    return WindowIndex(merged, [start for start, _ in merged])


def is_in_windows(ts_ms: int, window_index: WindowIndex) -> bool:
    pos = bisect_right(window_index.starts, ts_ms) - 1
    if pos < 0:
        return False
    return ts_ms <= window_index.windows[pos][1]


def interpolate_value(left: Optional[float], right: Optional[float], ratio: float) -> Optional[float]:
    if left is None and right is None:
        return None
    if left is None:
        return right
    if right is None:
        return left
    return left + (right - left) * ratio


def build_interpolated_livedata_rows(
    livedata_rows: Sequence[SensorRow],
    mask_indices: Sequence[int],
    apc_window_index: WindowIndex,
    apc_exact_ts: set[int],
    seq_base: int,
) -> List[SensorRow]:
    if len(livedata_rows) < 2:
        return []

    mask_set = set(mask_indices)
    out: List[SensorRow] = []
    next_seq = seq_base
    for left, right in zip(livedata_rows, livedata_rows[1:]):
        delta = right.ts_ms - left.ts_ms
        if delta <= INTERPOLATION_STEP_MS:
            continue
        cursor = left.ts_ms + INTERPOLATION_STEP_MS
        while cursor < right.ts_ms:
            if cursor in apc_exact_ts or is_in_windows(cursor, apc_window_index):
                cursor += INTERPOLATION_STEP_MS
                continue
            ratio = (cursor - left.ts_ms) / delta
            interpolated_values: List[Optional[float]] = []
            for idx, (left_value, right_value) in enumerate(zip(left.values, right.values)):
                if idx in mask_set:
                    interpolated_values.append(left_value)
                else:
                    interpolated_values.append(interpolate_value(left_value, right_value, ratio))
            cursor_dt = UNIX_EPOCH + timedelta(milliseconds=cursor)
            out.append(
                SensorRow(
                    ts_ms=cursor,
                    timestamp_raw=format_timestamp_raw(cursor_dt),
                    source="livedata",
                    values=interpolated_values,
                    seq=next_seq,
                )
            )
            next_seq += 1
            cursor += INTERPOLATION_STEP_MS
    return out


def load_global_io_ids(day_dirs: Sequence[Path]) -> List[int]:
    io_ids = set()
    for day_dir in day_dirs:
        for log_file in sorted((day_dir / "log").glob("*_log_anonymized.txt")):
            with open(log_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    io_ids.add(int(row["io_id"]))
    return sorted(io_ids)


def load_global_sensor_headers(day_dirs: Sequence[Path]) -> List[str]:
    for day_dir in day_dirs:
        sensor_dir = day_dir / "sensor"
        for apc_file in sorted(sensor_dir.glob("apc_*_anonymized.txt")):
            return read_sensor_headers(apc_file)
        for livedata_file in sorted(sensor_dir.glob("livedata*_anonymized.txt")):
            return read_sensor_headers(livedata_file)
    return []


def load_day_events(day_dir: Path) -> "OrderedDict[int, Dict[int, float]]":
    events: "OrderedDict[int, Dict[int, float]]" = OrderedDict()
    for log_file in sorted((day_dir / "log").glob("*_log_anonymized.txt")):
        with open(log_file, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                ts_ms = to_unix_ms(parse_log_timestamp(row["timestamp"]))
                io_id = int(row["io_id"])
                io_value = float(row["io_value"])
                if ts_ms not in events:
                    events[ts_ms] = {}
                events[ts_ms][io_id] = io_value
    return OrderedDict(sorted(events.items()))


def sensor_row_priority(row: SensorRow) -> int:
    if row.source == "apc":
        return 0
    if row.source == "livedata":
        return 1
    return 2


def build_day_sensor_rows(day_dir: Path, global_sensor_headers: Sequence[str]) -> Tuple[List[str], List[int], List[SensorRow], Dict[str, int]]:
    sensor_dir = day_dir / "sensor"
    apc_files = sorted(sensor_dir.glob("apc_*_anonymized.txt"))
    livedata_files = sorted(sensor_dir.glob("livedata*_anonymized.txt"))

    expected_headers: Optional[List[str]] = list(global_sensor_headers) if global_sensor_headers else None
    all_rows: List[SensorRow] = []
    apc_exact_ts: set[int] = set()
    apc_windows: List[Tuple[int, int]] = []
    stats = {
        "apc_rows": 0,
        "livedata_raw_rows": 0,
        "livedata_interp_rows": 0,
        "livedata_exact_ts_dropped": 0,
    }
    seq_base = 0

    for apc_file in apc_files:
        headers, rows, window = load_apc_rows(apc_file, expected_headers, seq_base)
        expected_headers = headers
        seq_base += len(rows) + 1
        apc_windows.append(window)
        apc_exact_ts.update(row.ts_ms for row in rows)
        all_rows.extend(rows)
        stats["apc_rows"] += len(rows)

    apc_window_index = build_window_index(apc_windows)
    mask_indices = [idx for idx, header in enumerate(expected_headers or []) if header.startswith("mask_")]

    for livedata_file in livedata_files:
        headers, raw_rows = load_livedata_rows(livedata_file, expected_headers, seq_base)
        if expected_headers is None:
            expected_headers = headers
            mask_indices = [idx for idx, header in enumerate(expected_headers) if header.startswith("mask_")]
        seq_base += len(raw_rows) + 1
        kept_raw_rows = []
        for row in raw_rows:
            if row.ts_ms in apc_exact_ts:
                stats["livedata_exact_ts_dropped"] += 1
                continue
            kept_raw_rows.append(row)
        all_rows.extend(kept_raw_rows)
        stats["livedata_raw_rows"] += len(kept_raw_rows)

        interp_rows = build_interpolated_livedata_rows(
            raw_rows,
            mask_indices,
            apc_window_index,
            apc_exact_ts,
            seq_base,
        )
        seq_base += len(interp_rows) + 1
        all_rows.extend(interp_rows)
        stats["livedata_interp_rows"] += len(interp_rows)

    if expected_headers is None:
        expected_headers = list(global_sensor_headers)

    all_rows.sort(key=lambda row: (row.ts_ms, sensor_row_priority(row), row.seq))
    return expected_headers, mask_indices, all_rows, stats


def find_preferred_exact_row(rows: Sequence[SensorRow]) -> Dict[int, int]:
    by_ts: Dict[int, List[Tuple[int, SensorRow]]] = {}
    for idx, row in enumerate(rows):
        by_ts.setdefault(row.ts_ms, []).append((idx, row))
    preferred: Dict[int, int] = {}
    for ts_ms, items in by_ts.items():
        items.sort(key=lambda item: (sensor_row_priority(item[1]), item[1].seq))
        preferred[ts_ms] = items[0][0]
    return preferred


def build_action_rows(sensor_rows: List[SensorRow], events: "OrderedDict[int, Dict[int, float]]", sensor_width: int) -> Tuple[List[SensorRow], Dict[str, int]]:
    preferred_exact_row = find_preferred_exact_row(sensor_rows)
    sensor_ts = [row.ts_ms for row in sensor_rows]
    rows_out = list(sensor_rows)
    if sensor_rows:
        first_sensor_ts = sensor_rows[0].ts_ms
        last_sensor_ts = sensor_rows[-1].ts_ms
    else:
        first_sensor_ts = None
        last_sensor_ts = None

    stats = {"event_on_sensor_rows": 0, "inserted_action_rows": 0, "logonly_rows": 0}
    next_seq = (max((row.seq for row in rows_out), default=0) + 1) * 10

    for ts_ms, event_map in events.items():
        exact_idx = preferred_exact_row.get(ts_ms)
        if exact_idx is not None:
            rows_out[exact_idx].event_map.update(event_map)
            stats["event_on_sensor_rows"] += 1
            continue

        if not sensor_rows or first_sensor_ts is None or last_sensor_ts is None or ts_ms < first_sensor_ts or ts_ms > last_sensor_ts:
            source = "logonly"
            values = [None] * sensor_width
            stats["logonly_rows"] += 1
        else:
            prev_pos = bisect_left(sensor_ts, ts_ms) - 1
            prev_sensor = sensor_rows[prev_pos]
            values = list(prev_sensor.values)
            source = "logapc" if prev_sensor.source == "apc" else "loglivedata"
        rows_out.append(
            SensorRow(
                ts_ms=ts_ms,
                timestamp_raw=format_timestamp_raw(UNIX_EPOCH + timedelta(milliseconds=ts_ms)),
                source=source,
                values=values,
                event_map=dict(event_map),
                seq=next_seq,
            )
        )
        next_seq += 10
        stats["inserted_action_rows"] += 1

    rows_out.sort(key=lambda row: (row.ts_ms, sensor_row_priority(row), row.seq))
    return rows_out, stats


def build_stateful_rows(rows: Sequence[SensorRow], io_ids: Sequence[int], starting_state: Dict[int, float]) -> Tuple[List[List[object]], Dict[int, float]]:
    current_state = {io_id: starting_state.get(io_id, 0.0) for io_id in io_ids}
    out_rows: List[List[object]] = []
    for row in rows:
        evt_values = [0] * len(io_ids)
        for idx, io_id in enumerate(io_ids):
            if io_id in row.event_map:
                evt_values[idx] = 1
                current_state[io_id] = row.event_map[io_id]
        state_values = [current_state[io_id] for io_id in io_ids]
        out_rows.append(
            [
                row.timestamp_raw,
                row.ts_ms,
                row.source,
                *row.values,
                *evt_values,
                *state_values,
            ]
        )
    return out_rows, current_state


def write_tsv(path: Path, header: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(header)
        for row in rows:
            formatted = []
            for value in row:
                if value is None:
                    formatted.append("")
                elif isinstance(value, float) and math.isnan(value):
                    formatted.append("")
                else:
                    formatted.append(value)
            writer.writerow(formatted)


def build_headers(sensor_headers: Sequence[str], io_ids: Sequence[int], for_training: bool) -> List[str]:
    prefix = ["timestamp_raw", "ts_ms", "source_code" if for_training else "source"]
    evt_headers = [f"evt_{io_id}" for io_id in io_ids]
    state_headers = [f"state_{io_id}" for io_id in io_ids]
    return [*prefix, *sensor_headers, *evt_headers, *state_headers]


def convert_training_rows(rows: Sequence[Sequence[object]]) -> List[List[object]]:
    out: List[List[object]] = []
    for row in rows:
        source = row[2]
        if source == "logonly":
            continue
        source_code = SOURCE_CODES[source]
        clean_row = list(row)
        clean_row[2] = source_code
        out.append(clean_row)
    return out


def process_day(
    day_dir: Path,
    io_ids: Sequence[int],
    sensor_headers: Sequence[str],
    current_state: Dict[int, float],
    output_root: Path,
) -> Dict[str, object]:
    sensor_headers, _, sensor_rows, sensor_stats = build_day_sensor_rows(day_dir, sensor_headers)
    events = load_day_events(day_dir)
    rows_with_actions, action_stats = build_action_rows(sensor_rows, events, len(sensor_headers))
    original_rows, ending_state = build_stateful_rows(rows_with_actions, io_ids, current_state)
    training_rows = convert_training_rows(original_rows)

    original_header = build_headers(sensor_headers, io_ids, for_training=False)
    training_header = build_headers(sensor_headers, io_ids, for_training=True)

    original_path = output_root / "original_daily" / day_dir.name / f"{day_dir.name}_aligned_original.tsv"
    training_path = output_root / "train_daily" / day_dir.name / f"{day_dir.name}_aligned_train.tsv"
    report_path = output_root / "reports" / f"{day_dir.name}_aligned_report.json"

    write_tsv(original_path, original_header, original_rows)
    write_tsv(training_path, training_header, training_rows)

    report = {
        "day": day_dir.name,
        "original_output": str(original_path),
        "training_output": str(training_path),
        "sensor_headers": len(sensor_headers),
        "io_ids": list(io_ids),
        "sensor_stats": sensor_stats,
        "action_stats": action_stats,
        "event_timestamps": len(events),
        "original_rows": len(original_rows),
        "training_rows": len(training_rows),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"report": report, "ending_state": ending_state}


def main() -> None:
    args = parse_args()
    all_day_dirs = discover_days(args.input_root, [])
    day_dirs = discover_days(args.input_root, args.days)
    if not day_dirs:
        raise FileNotFoundError(f"No day directories found under {args.input_root}")

    io_ids = load_global_io_ids(all_day_dirs)
    sensor_headers = load_global_sensor_headers(all_day_dirs)
    current_state = {io_id: 0.0 for io_id in io_ids}
    summary = {"days": [], "io_ids": io_ids, "output_root": str(args.output_root)}
    print(
        f"[INFO] days={len(day_dirs)} global_days={len(all_day_dirs)} "
        f"io_ids={len(io_ids)} sensor_headers={len(sensor_headers)}",
        flush=True,
    )

    for day_dir in day_dirs:
        print(f"[START] {day_dir.name}", flush=True)
        result = process_day(day_dir, io_ids, sensor_headers, current_state, args.output_root)
        current_state = result["ending_state"]
        summary["days"].append(result["report"])
        print(
            f"[OK] {day_dir.name}: original_rows={result['report']['original_rows']} "
            f"training_rows={result['report']['training_rows']}",
            flush=True,
        )

    summary_path = args.output_root / "reports" / "aligned_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] summary={summary_path}", flush=True)


if __name__ == "__main__":
    main()
