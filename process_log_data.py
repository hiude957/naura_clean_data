#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

import pipeline_config as C
from pipeline_utils.io_mapping import load_password_book, save_password_book, update_io_mapping
from pipeline_utils.time_utils import format_timestamp_millis, robust_to_datetime


RAW_LOG_COLUMNS = ["timestamp", "instrument", "action", "value"]
ANON_LOG_COLUMNS = ["timestamp", "io_id", "io_value"]


def parse_log_line(line: str) -> Optional[Union[Dict[str, object], List[Dict[str, object]]]]:
    parts = line.split(maxsplit=4)
    if len(parts) < 5:
        return None

    timestamp = f"{parts[0]} {parts[1]}"
    content = parts[4].strip()
    for rule in C.MATCH_RULES:
        match = re.search(rule["pattern"], content)
        if not match:
            continue

        processed = rule["processor"](match)
        if isinstance(processed, list):
            return [
                {
                    "timestamp": timestamp,
                    "instrument": inst,
                    "action": action,
                    "value": val,
                }
                for inst, action, val in processed
            ]

        inst, action, val = processed
        return {"timestamp": timestamp, "instrument": inst, "action": action, "value": val}

    return None


def classify_value_drop_reason(raw_value: object) -> str:
    if raw_value is None:
        return "value_missing"

    raw_text = str(raw_value).strip().lower()
    if raw_text in {"", "none", "null", "nan"}:
        return "value_missing"

    return "value_not_numeric"


def write_log_report(path: Path, report: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("===== Log Processing Report =====\n")
        f.write(f"day={report['day']}\n")
        f.write(f"input_dir={report['input_dir']}\n")
        f.write(f"raw_output={report['raw_output']}\n")
        f.write(f"anonymized_output={report['anonymized_output']}\n")
        f.write(f"matched_rows={report['matched_rows']}\n")
        f.write(f"valid_rows={report['valid_rows']}\n")
        f.write(f"dropped_invalid_timestamp_rows={report['dropped_invalid_timestamp_rows']}\n")
        f.write(f"dropped_empty_instrument_rows={report['dropped_empty_instrument_rows']}\n")
        f.write(f"dropped_missing_value_rows={report['dropped_missing_value_rows']}\n")
        f.write(f"dropped_non_numeric_value_rows={report['dropped_non_numeric_value_rows']}\n")
        f.write("\n")

        f.write(f"processed_files_count={len(report['processed_files'])}\n")
        if report["processed_files"]:
            f.write("processed_files:\n")
            for file_path in report["processed_files"]:
                f.write(f"  - {file_path}\n")
        else:
            f.write("processed_files: <NONE>\n")
        f.write("\n")

        f.write(f"ignored_files_count={len(report['ignored_files'])}\n")
        if report["ignored_files"]:
            f.write("ignored_files:\n")
            for file_path in report["ignored_files"]:
                f.write(f"  - {file_path}\n")
        else:
            f.write("ignored_files: <NONE>\n")
        f.write("\n")

        f.write(f"failed_files_count={len(report['failed_files'])}\n")
        if report["failed_files"]:
            f.write("failed_files:\n")
            for item in report["failed_files"]:
                f.write(f"  - file={item['file']} | error={item['error']}\n")
        else:
            f.write("failed_files: <NONE>\n")


def _list_day_files(day_path: Path) -> List[Path]:
    if not day_path.exists():
        return []
    return sorted([path for path in day_path.iterdir() if path.is_file()])


def process_one_day(input_day: C.InputDay) -> Dict[str, object]:
    day_paths = C.get_day_output_paths(input_day.day)
    all_files = _list_day_files(input_day.path)
    target_files = [path for path in all_files if path.suffix.lower() == ".txt"]
    ignored_files = [str(path) for path in all_files if path.suffix.lower() != ".txt"]

    all_rows: List[Dict[str, object]] = []
    processed_files: List[str] = []
    failed_files: List[Dict[str, str]] = []

    for file_path in target_files:
        file_rows: List[Dict[str, object]] = []
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line_number, line in enumerate(f, start=1):
                    if not line.strip():
                        continue
                    try:
                        result = parse_log_line(line)
                    except Exception as exc:
                        raise ValueError(f"line {line_number}: {exc}") from exc
                    if result is None:
                        continue
                    if isinstance(result, list):
                        file_rows.extend(result)
                    else:
                        file_rows.append(result)
            processed_files.append(str(file_path))
            all_rows.extend(file_rows)
        except Exception as exc:
            failed_files.append({"file": str(file_path), "error": str(exc)})

    raw_df = pd.DataFrame(all_rows, columns=RAW_LOG_COLUMNS)
    if raw_df.empty:
        raw_df = pd.DataFrame(columns=RAW_LOG_COLUMNS)
        valid_raw_df = raw_df.copy()
        dropped_invalid_timestamp_rows = 0
        dropped_empty_instrument_rows = 0
        dropped_missing_value_rows = 0
        dropped_non_numeric_value_rows = 0
    else:
        raw_df["parsed_timestamp"] = robust_to_datetime(raw_df["timestamp"])
        raw_df["instrument"] = raw_df["instrument"].astype(str).str.strip()
        raw_df["raw_value"] = raw_df["value"]
        raw_df["value"] = pd.to_numeric(raw_df["value"], errors="coerce")

        invalid_value_mask = raw_df["value"].isna()
        dropped_reasons = raw_df.loc[invalid_value_mask, "raw_value"].apply(classify_value_drop_reason)
        dropped_invalid_timestamp_rows = int(raw_df["parsed_timestamp"].isna().sum())
        dropped_empty_instrument_rows = int(raw_df["instrument"].eq("").sum())
        dropped_missing_value_rows = int((dropped_reasons == "value_missing").sum())
        dropped_non_numeric_value_rows = int((dropped_reasons == "value_not_numeric").sum())

        valid_raw_df = raw_df[
            raw_df["parsed_timestamp"].notna()
            & raw_df["instrument"].ne("")
            & raw_df["value"].notna()
        ].copy()
        valid_raw_df["timestamp"] = valid_raw_df["parsed_timestamp"].apply(format_timestamp_millis)
        valid_raw_df = valid_raw_df[RAW_LOG_COLUMNS]

    valid_raw_df.to_csv(day_paths.raw_log_path, index=False, sep="\t")

    book = load_password_book(C.MAPPING_FILE)
    io_map = update_io_mapping(book, valid_raw_df["instrument"].tolist())
    save_password_book(C.MAPPING_FILE, book)

    anonymized_df = valid_raw_df[["timestamp", "instrument", "value"]].copy()
    if anonymized_df.empty:
        anonymized_df = pd.DataFrame(columns=ANON_LOG_COLUMNS)
    else:
        anonymized_df["io_id"] = anonymized_df["instrument"].map(io_map)
        if anonymized_df["io_id"].isna().any():
            missing_instruments = (
                anonymized_df.loc[anonymized_df["io_id"].isna(), "instrument"].drop_duplicates().tolist()
            )
            raise ValueError(f"These instruments could not be mapped to io_id: {missing_instruments}")
        anonymized_df["io_id"] = anonymized_df["io_id"].astype(int)
        anonymized_df = anonymized_df.rename(columns={"value": "io_value"})[ANON_LOG_COLUMNS]

    anonymized_df.to_csv(day_paths.anon_log_path, index=False, sep="\t")

    report = {
        "day": input_day.day,
        "input_dir": str(input_day.path),
        "processed_files": processed_files,
        "ignored_files": ignored_files,
        "failed_files": failed_files,
        "matched_rows": int(len(raw_df)),
        "valid_rows": int(len(valid_raw_df)),
        "dropped_invalid_timestamp_rows": dropped_invalid_timestamp_rows,
        "dropped_empty_instrument_rows": dropped_empty_instrument_rows,
        "dropped_missing_value_rows": dropped_missing_value_rows,
        "dropped_non_numeric_value_rows": dropped_non_numeric_value_rows,
        "raw_output": str(day_paths.raw_log_path),
        "anonymized_output": str(day_paths.anon_log_path),
    }
    write_log_report(day_paths.log_report_path, report)

    print(f"===== Log Processing Done [{input_day.day}] =====")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def main() -> None:
    input_days = C.discover_input_days(C.LOG_INPUT_ROOT)
    if not input_days:
        raise FileNotFoundError(f"No day directories found under log input root: {C.LOG_INPUT_ROOT}")

    processed_days: List[str] = []
    failed_days: Dict[str, str] = {}

    for input_day in input_days:
        try:
            process_one_day(input_day)
            processed_days.append(input_day.day)
        except Exception as exc:
            failed_days[input_day.day] = str(exc)
            print(f"[ERROR][LOG][{input_day.day}] {exc}")

    if not processed_days:
        raise ValueError("No log day was processed successfully.")

    overall_summary = {
        "processed_days": processed_days,
        "failed_days": failed_days,
        "raw_output_root": str(C.RAW_ROOT),
        "anonymized_output_root": str(C.ANON_ROOT),
    }
    print("===== Log Processing Summary =====")
    print(json.dumps(overall_summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
