#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 1：清洗原始 log 文本，提取控制命令事件。

输入：LOG_INPUT_DIR 下的 *.txt
输出：
- step1_log_events.json
- step1_log_events.txt   （制表符分隔的纯文本，不用 Excel 类格式）
- step1_log_summary.json

说明：
- 正则匹配规则来自 pipeline_config.MATCH_RULES
- 控制命令 value 的归一化由 processor 自己完成
- 遇到被占用/上锁/权限不足的文件，会在终端明确打印并跳过
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Dict, List, Optional, Union

import pandas as pd

import pipeline_config as C


def robust_to_datetime(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    formats = [
        "%Y/%m/%d %H/%M/%S.%f",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for fmt in formats:
        parsed = pd.to_datetime(s, format=fmt, errors="coerce")
        mask = out.isna() & parsed.notna()
        if mask.any():
            out.loc[mask] = parsed.loc[mask]
    mask = out.isna()
    if mask.any():
        out.loc[mask] = pd.to_datetime(s[mask], errors="coerce")
    return out


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
            rows = []
            for inst, action, val in processed:
                rows.append({"timestamp": timestamp, "instrument": inst, "action": action, "value": val})
            return rows
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


def main() -> None:
    log_dir = pathlib.Path(C.LOG_INPUT_DIR)
    if not log_dir.exists():
        raise FileNotFoundError(f"找不到日志目录: {log_dir}")

    all_rows: List[Dict[str, object]] = []
    txt_files = sorted(log_dir.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"目录下没有 txt 文件: {log_dir}")

    locked_files: List[str] = []
    failed_files: List[str] = []
    parsed_files = 0

    for file_path in txt_files:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                parsed_files += 1
                for line in f:
                    if not line.strip():
                        continue
                    result = parse_log_line(line)
                    if result is None:
                        continue
                    if isinstance(result, list):
                        all_rows.extend(result)
                    else:
                        all_rows.append(result)
        except PermissionError as e:
            print(f"[LOCKED][STEP1] 文件被占用或无权限打开，已跳过: {file_path} | {e}")
            locked_files.append(str(file_path))
        except OSError as e:
            msg = str(e).lower()
            if "used by another process" in msg or "permission denied" in msg or "sharing violation" in msg:
                print(f"[LOCKED][STEP1] 文件可能被占用/上锁，已跳过: {file_path} | {e}")
                locked_files.append(str(file_path))
            else:
                print(f"[ERROR][STEP1] 读取失败，已跳过: {file_path} | {e}")
                failed_files.append(str(file_path))
        except Exception as e:
            print(f"[ERROR][STEP1] 读取失败，已跳过: {file_path} | {e}")
            failed_files.append(str(file_path))

    if not all_rows:
        raise ValueError("没有提取到任何控制命令，请检查 MATCH_RULES，或确认源文件没有全部被锁。")

    df = pd.DataFrame(all_rows)
    df["timestamp"] = robust_to_datetime(df["timestamp"])
    df["instrument"] = df["instrument"].astype(str).str.strip()
    df["raw_value"] = df["value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    dropped_value_df = df[df["value"].isna()].copy()
    dropped_value_df["drop_reason"] = dropped_value_df["raw_value"].apply(classify_value_drop_reason)
    dropped_value_df = dropped_value_df[["timestamp", "instrument", "action", "raw_value", "drop_reason"]]
    dropped_value_df.to_csv(C.STEP1_DROPPED_VALUE_PATH, index=False, sep="\t")

    df = df[df["timestamp"].notna()].copy()
    df = df[df["instrument"].ne("") & df["value"].notna()].copy()
    df = df.drop(columns=["raw_value"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    with open(C.STEP1_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2, default=str)
    df.to_csv(C.STEP1_TXT_PATH, index=False, sep="\t")

    summary = {
        "log_dir": str(log_dir),
        "total_txt_files": len(txt_files),
        "parsed_files": parsed_files,
        "locked_files_count": len(locked_files),
        "failed_files_count": len(failed_files),
        "locked_files": locked_files,
        "failed_files": failed_files,
        "dropped_invalid_value_rows": int(len(dropped_value_df)),
        "dropped_missing_value_rows": int((dropped_value_df["drop_reason"] == "value_missing").sum()),
        "dropped_non_numeric_value_rows": int((dropped_value_df["drop_reason"] == "value_not_numeric").sum()),
        "event_rows": int(len(df)),
        "outputs": {
            "json": str(C.STEP1_JSON_PATH),
            "txt": str(C.STEP1_TXT_PATH),
            "dropped_invalid_value_txt": str(C.STEP1_DROPPED_VALUE_PATH),
        },
    }
    with open(C.STEP1_SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("===== Step 1 完成 =====")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
