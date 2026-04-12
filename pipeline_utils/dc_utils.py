from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

from pipeline_utils.time_utils import robust_to_datetime


PROCESS_START_PREFIX = "process start time:"
FORMAT_PROCESS_START = "process_start_offset"
FORMAT_TIME_HEADER = "time_header"
PROCESS_START_HEADER_ROW = 13
PROCESS_START_DROP_COLS = 2
PROCESS_START_BLANK_LINE_COUNT = 12


@dataclass(frozen=True)
class DcSourceFile:
    file_path: Path
    file_name: str
    file_format: str
    process_start_line: str
    process_start_time: Optional[pd.Timestamp]
    raw_df: pd.DataFrame
    time_text: pd.Series
    ts: pd.Series


@dataclass(frozen=True)
class DcSelectionResult:
    df: pd.DataFrame
    present_headers: List[str]
    missing_headers: List[str]
    invalid_time_rows: int


def read_first_line(file_path: Path) -> str:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return f.readline().rstrip("\r\n").lstrip("\ufeff")


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(col).strip().lstrip("\ufeff") for col in out.columns]
    return out


def detect_dc_format(file_path: Path) -> str:
    first_line = read_first_line(file_path).strip().lower()
    if first_line.startswith(PROCESS_START_PREFIX):
        return FORMAT_PROCESS_START
    return FORMAT_TIME_HEADER


def parse_process_start_time_text(raw_value: str, file_path: Path) -> pd.Timestamp:
    parsed = robust_to_datetime(pd.Series([raw_value])).iloc[0]
    if pd.isna(parsed):
        raise ValueError(f"Invalid Process Start Time in file: {file_path} | raw={raw_value}")
    return parsed


def parse_process_start_time(file_path: Path) -> tuple[str, pd.Timestamp]:
    first_line = read_first_line(file_path).strip()
    if not first_line.lower().startswith(PROCESS_START_PREFIX):
        raise ValueError(f"Process Start Time not found in first line: {file_path}")

    raw_value = first_line.split(":", 1)[1].strip()
    return first_line, parse_process_start_time_text(raw_value, file_path)


def _read_process_start_format(
    file_path: Path,
) -> tuple[str, pd.Timestamp, pd.DataFrame, pd.Series, pd.Series]:
    process_start_line, process_start_time = parse_process_start_time(file_path)
    raw_df = pd.read_csv(
        file_path,
        sep="\t",
        header=PROCESS_START_HEADER_ROW,
        low_memory=False,
        dtype=str,
        keep_default_na=False,
    )
    raw_df = clean_columns(raw_df)

    if raw_df.empty:
        empty_time = pd.Series(dtype="object")
        empty_ts = pd.Series(dtype="datetime64[ns]")
        return process_start_line, process_start_time, raw_df, empty_time, empty_ts
    if raw_df.shape[1] <= PROCESS_START_DROP_COLS:
        raise ValueError(
            f"Process-start dc format expects at least 3 columns after header line 14: {file_path}"
        )

    cleaned_df = raw_df.iloc[:, PROCESS_START_DROP_COLS:].reset_index(drop=True)
    time_text = cleaned_df.iloc[:, 0].astype(str).reset_index(drop=True)
    offset_ms = pd.to_numeric(time_text, errors="coerce")
    ts = process_start_time + pd.to_timedelta(offset_ms, unit="s")
    return process_start_line, process_start_time, cleaned_df, time_text, ts


def _read_time_header_format(
    file_path: Path,
) -> tuple[str, None, pd.DataFrame, pd.Series, pd.Series]:
    raw_df = pd.read_csv(
        file_path,
        sep="\t",
        low_memory=False,
        dtype=str,
        keep_default_na=False,
    )
    raw_df = clean_columns(raw_df)

    if raw_df.empty:
        empty_time = pd.Series(dtype="object")
        empty_ts = pd.Series(dtype="datetime64[ns]")
        return "", None, raw_df, empty_time, empty_ts

    time_text = raw_df.iloc[:, 0].astype(str).reset_index(drop=True)
    ts = robust_to_datetime(time_text)
    return "", None, raw_df.reset_index(drop=True), time_text, ts


def load_dc_source_file(file_path: str | Path) -> DcSourceFile:
    source_path = Path(file_path)
    dc_format = detect_dc_format(source_path)
    if dc_format == FORMAT_PROCESS_START:
        process_start_line, process_start_time, raw_df, time_text, ts = _read_process_start_format(source_path)
    else:
        process_start_line, process_start_time, raw_df, time_text, ts = _read_time_header_format(source_path)

    return DcSourceFile(
        file_path=source_path,
        file_name=source_path.name,
        file_format=dc_format,
        process_start_line=process_start_line,
        process_start_time=process_start_time,
        raw_df=raw_df,
        time_text=time_text,
        ts=ts,
    )


def build_selected_file_export(
    source: DcSourceFile,
    gas_headers: Sequence[str],
    default_map: Dict[str, float],
    *,
    fill_present_na: bool,
) -> DcSelectionResult:
    if source.raw_df.empty:
        return DcSelectionResult(
            df=pd.DataFrame(columns=["Time", *gas_headers]),
            present_headers=[],
            missing_headers=list(gas_headers),
            invalid_time_rows=0,
        )

    out = pd.DataFrame({"Time": source.time_text.reset_index(drop=True)})
    present_headers: List[str] = []
    missing_headers: List[str] = []

    for header in gas_headers:
        if header in source.raw_df.columns:
            column = pd.to_numeric(source.raw_df[header], errors="coerce")
            if fill_present_na:
                column = column.fillna(default_map[header])
            out[header] = column.reset_index(drop=True)
            present_headers.append(header)
        else:
            out[header] = pd.Series(default_map[header], index=source.raw_df.index, dtype="float64")
            missing_headers.append(header)

    invalid_time_rows = int(source.ts.notna().eq(False).sum()) if not source.ts.empty else 0
    return DcSelectionResult(
        df=out[["Time", *gas_headers]].reset_index(drop=True),
        present_headers=present_headers,
        missing_headers=missing_headers,
        invalid_time_rows=invalid_time_rows,
    )


def write_dc_like_output(
    output_path: Path,
    df: pd.DataFrame,
    source: DcSourceFile,
) -> None:
    if source.file_format != FORMAT_PROCESS_START:
        df.to_csv(output_path, index=False, sep="\t")
        return

    table_text = df.to_csv(index=False, sep="\t", lineterminator="\n")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(source.process_start_line)
        f.write("\n")
        f.write("\n" * PROCESS_START_BLANK_LINE_COUNT)
        f.write(table_text)
