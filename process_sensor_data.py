#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

import pipeline_config as C
from pipeline_utils.dc_utils import (
    FORMAT_PROCESS_START,
    FORMAT_TIME_HEADER,
    DcSourceFile,
    build_selected_file_export,
    load_dc_source_file,
    write_dc_like_output,
)


@dataclass
class ProcessedSensorFile:
    source: DcSourceFile
    raw_df: pd.DataFrame
    anonymized_df: pd.DataFrame
    missing_headers: List[str]
    invalid_time_rows: int
    base_name: str = ""
    raw_output_path: str = ""
    anonymized_output_path: str = ""


def apply_minmax(
    df: pd.DataFrame,
    gas_headers: Sequence[str],
    minmax_map: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    out = df.copy()
    mins = pd.Series({header: minmax_map[header]["min"] for header in gas_headers})
    maxs = pd.Series({header: minmax_map[header]["max"] for header in gas_headers})
    numeric_df = out.loc[:, gas_headers].apply(pd.to_numeric, errors="coerce")
    out.loc[:, gas_headers] = (numeric_df - mins) / (maxs - mins)
    return out


def add_header_mask_columns(
    df: pd.DataFrame,
    gas_headers: Sequence[str],
    anon_map: Dict[str, str],
    missing_headers: Sequence[str],
) -> pd.DataFrame:
    missing_set = set(missing_headers)
    mask_df = pd.DataFrame(
        {
            f"mask_{anon_map[header]}": pd.Series(
                0 if header in missing_set else 1,
                index=df.index,
                dtype="int64",
            )
            for header in gas_headers
        }
    )
    return pd.concat([df, mask_df], axis=1, copy=False).copy()


def build_anonymized_sensor_df(
    raw_df: pd.DataFrame,
    gas_headers: Sequence[str],
    anon_map: Dict[str, str],
    minmax_map: Dict[str, Dict[str, float]],
    missing_headers: Sequence[str],
) -> pd.DataFrame:
    out = apply_minmax(raw_df, gas_headers, minmax_map)
    anon_headers = [anon_map[header] for header in gas_headers]
    mask_headers = [f"mask_{anon_map[header]}" for header in gas_headers]
    out = out.rename(columns=anon_map)
    out = out[["Time", *anon_headers]]
    out = add_header_mask_columns(out, gas_headers, anon_map, missing_headers)
    return out[["Time", *anon_headers, *mask_headers]]


def build_apc_base_name(process_start_time: pd.Timestamp) -> str:
    return f"apc_{process_start_time.strftime('%Y%m%d_%H%M%S')}"


def assign_output_base_names(records: List[ProcessedSensorFile]) -> None:
    apc_records = [record for record in records if record.source.file_format == FORMAT_PROCESS_START]
    livedata_records = [record for record in records if record.source.file_format == FORMAT_TIME_HEADER]

    apc_name_counts: Dict[str, int] = {}
    for record in apc_records:
        if record.source.process_start_time is None:
            raise ValueError(f"APC file is missing process_start_time: {record.source.file_path}")
        base_name = build_apc_base_name(record.source.process_start_time)
        apc_name_counts[base_name] = apc_name_counts.get(base_name, 0) + 1
        record.base_name = (
            base_name
            if apc_name_counts[base_name] == 1
            else f"{base_name}_{apc_name_counts[base_name]}"
        )

    if len(livedata_records) == 1:
        livedata_records[0].base_name = "livedata"
    else:
        for idx, record in enumerate(livedata_records, start=1):
            record.base_name = f"livedata_{idx}"


def write_sensor_report(path: Path, report: Dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("===== Sensor Processing Report =====\n")
        f.write(f"day={report['day']}\n")
        f.write(f"input_dir={report['input_dir']}\n")
        f.write(f"processed_files_count={len(report['processed_files'])}\n")
        f.write(f"ignored_files_count={len(report['ignored_files'])}\n")
        f.write(f"failed_files_count={len(report['failed_files'])}\n")
        f.write("\n")

        f.write("processed_files:\n")
        if report["processed_files"]:
            for item in report["processed_files"]:
                f.write(
                    "  - "
                    f"input={item['input']} | "
                    f"type={item['file_type']} | "
                    f"raw_output={item['raw_output']} | "
                    f"anonymized_output={item['anonymized_output']} | "
                    f"invalid_time_rows={item['invalid_time_rows']} | "
                    f"missing_headers={item['missing_headers']}\n"
                )
        else:
            f.write("  <NONE>\n")
        f.write("\n")

        f.write("ignored_files:\n")
        if report["ignored_files"]:
            for file_path in report["ignored_files"]:
                f.write(f"  - {file_path}\n")
        else:
            f.write("  <NONE>\n")
        f.write("\n")

        f.write("failed_files:\n")
        if report["failed_files"]:
            for item in report["failed_files"]:
                f.write(f"  - file={item['file']} | error={item['error']}\n")
        else:
            f.write("  <NONE>\n")


def _list_day_files(day_path: Path) -> List[Path]:
    if not day_path.exists():
        return []
    return sorted([path for path in day_path.iterdir() if path.is_file()])


def process_one_day(input_day: C.InputDay) -> Dict[str, object]:
    day_paths = C.get_day_output_paths(input_day.day)
    all_files = _list_day_files(input_day.path)
    target_files = [path for path in all_files if path.suffix.lower() == ".dc"]
    ignored_files = [str(path) for path in all_files if path.suffix.lower() != ".dc"]

    apc_specs = C.get_apc_header_specs()
    gas_headers = [spec.raw_header for spec in apc_specs]
    anon_map = {spec.raw_header: spec.index_name for spec in apc_specs}
    default_map = {spec.raw_header: spec.default_value for spec in apc_specs}
    minmax_map = {
        spec.raw_header: {"min": spec.min_value, "max": spec.max_value}
        for spec in apc_specs
    }

    processed_records: List[ProcessedSensorFile] = []
    failed_files: List[Dict[str, str]] = []

    for file_path in target_files:
        try:
            source = load_dc_source_file(file_path)
            selected = build_selected_file_export(
                source,
                gas_headers,
                default_map,
                fill_present_na=True,
            )
            anonymized_df = build_anonymized_sensor_df(
                selected.df,
                gas_headers,
                anon_map,
                minmax_map,
                selected.missing_headers,
            )
            processed_records.append(
                ProcessedSensorFile(
                    source=source,
                    raw_df=selected.df,
                    anonymized_df=anonymized_df,
                    missing_headers=selected.missing_headers,
                    invalid_time_rows=selected.invalid_time_rows,
                )
            )
        except Exception as exc:
            failed_files.append({"file": str(file_path), "error": str(exc)})

    assign_output_base_names(processed_records)

    processed_items: List[Dict[str, object]] = []
    for record in processed_records:
        raw_output_path = day_paths.raw_sensor_dir / f"{record.base_name}.txt"
        anonymized_output_path = day_paths.anon_sensor_dir / f"{record.base_name}_anonymized.txt"
        write_dc_like_output(raw_output_path, record.raw_df, record.source)
        write_dc_like_output(anonymized_output_path, record.anonymized_df, record.source)
        record.raw_output_path = str(raw_output_path)
        record.anonymized_output_path = str(anonymized_output_path)
        processed_items.append(
            {
                "input": str(record.source.file_path),
                "file_type": "apc" if record.source.file_format == FORMAT_PROCESS_START else "livedata",
                "raw_output": str(raw_output_path),
                "anonymized_output": str(anonymized_output_path),
                "invalid_time_rows": int(record.invalid_time_rows),
                "missing_headers": list(record.missing_headers),
            }
        )

    report = {
        "day": input_day.day,
        "input_dir": str(input_day.path),
        "processed_files": processed_items,
        "ignored_files": ignored_files,
        "failed_files": failed_files,
    }
    write_sensor_report(day_paths.sensor_report_path, report)

    print(f"===== Sensor Processing Done [{input_day.day}] =====")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def main() -> None:
    input_days = C.discover_input_days(C.DC_INPUT_ROOT)
    if not input_days:
        raise FileNotFoundError(f"No day directories found under sensor input root: {C.DC_INPUT_ROOT}")

    processed_days: List[str] = []
    failed_days: Dict[str, str] = {}

    for input_day in input_days:
        try:
            process_one_day(input_day)
            processed_days.append(input_day.day)
        except Exception as exc:
            failed_days[input_day.day] = str(exc)
            print(f"[ERROR][SENSOR][{input_day.day}] {exc}")

    if not processed_days:
        raise ValueError("No sensor day was processed successfully.")

    overall_summary = {
        "processed_days": processed_days,
        "failed_days": failed_days,
        "raw_output_root": str(C.RAW_ROOT),
        "anonymized_output_root": str(C.ANON_ROOT),
    }
    print("===== Sensor Processing Summary =====")
    print(json.dumps(overall_summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
