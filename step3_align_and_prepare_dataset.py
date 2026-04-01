#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 3：把 Step 1 的控制命令事件和 Step 2 的 APC 状态按时间对齐，生成训练文件。

输入：
- step1_log_events.json
- step2_apc_state_raw.txt

输出：
- event_table.csv
- train.csv
- val.csv
- meta.json
- summary.json

说明：
- 对齐方式：merge_asof(direction='backward')
- 只使用控制命令发生之前最近一次 APC 状态
- APC 归一化不再从 train 拟合，而是严格使用配置里的 min / max
- APC 脱敏列名不再来自 password_book，而是直接使用配置里的 index_name
- 当前只输出 train / val，不再输出 test
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Sequence, Tuple

import numpy as np
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


def load_password_book(path: str) -> Dict[str, Dict[str, int]]:
    p = Path(path)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            book = json.load(f)

    else:
        book = {}

    book.setdefault("io_channel_to_id", {})
    return book
    
  


def save_password_book(path: str, book: Dict[str, Dict[str, str]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(book, f, ensure_ascii=False, indent=2)


def update_io_mapping(book: Dict[str, Dict[str, int]], io_channels: Sequence[str]) -> Dict[str, str]:
    channel_map = book["io_channel_to_id"]
    next_id = max([int(v) for v in channel_map.values()], default=0) + 1
    for ch in sorted(set[str](str(v).strip() for v in io_channels if pd.notna(v) and str(v).strip())):
        if ch not in channel_map:
            channel_map[ch] = next_id
            next_id += 1

    return channel_map


def resolve_train_val_sizes(n: int, train_ratio: float, val_ratio: float) -> Tuple[int, int]:
    if n <= 0:
        return 0, 0
    if not 0 < train_ratio < 1:
        raise ValueError(f"TRAIN_RATIO 必须在 (0, 1) 之间，当前为: {train_ratio}")
    if not 0 <= val_ratio < 1:
        raise ValueError(f"VAL_RATIO 必须在 [0, 1) 之间，当前为: {val_ratio}")
    if train_ratio + val_ratio > 1:
        raise ValueError(
            f"TRAIN_RATIO + VAL_RATIO 不能大于 1，当前为: {train_ratio + val_ratio}"
        )

    n_train = int(n * train_ratio)
    if n == 1:
        return 1, 0
    n_train = min(max(n_train, 1), n - 1)
    n_val = n - n_train
    return n_train, n_val


def ensure_apc_columns(
    apc_state: pd.DataFrame,
    apc_headers: Sequence[str],
    apc_defaults: Dict[str, float],
) -> pd.DataFrame:
    out = apc_state.copy()
    for header in apc_headers:
        if header not in out.columns:
            out[header] = apc_defaults[header]
    return out[["ts", *apc_headers]]


def apply_config_minmax(
    df: pd.DataFrame,
    apc_headers: Sequence[str],
    apc_minmax: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    out = df.copy()
    for header in apc_headers:
        out[header] = pd.to_numeric(out[header], errors="coerce")
        cmin = apc_minmax[header]["min"]
        cmax = apc_minmax[header]["max"]
        out[header] = (out[header] - cmin) / (cmax - cmin)
    return out


def safe_write_csv(df: pd.DataFrame, path: Path, *, sep: str = ",") -> None:
    try:
        df.to_csv(path, index=False, sep=sep)
    except PermissionError as e:
        raise PermissionError(f"输出文件可能正被占用/上锁，请先关闭后重试: {path} | {e}")


def main() -> None:
    if not C.STEP1_JSON_PATH.exists():
        raise FileNotFoundError(f"找不到 Step 1 输出: {C.STEP1_JSON_PATH}")
    if not C.STEP2_STATE_PATH.exists():
        raise FileNotFoundError(f"找不到 Step 2 输出: {C.STEP2_STATE_PATH}")

    apc_specs = C.get_apc_header_specs()
    apc_headers = [spec.raw_header for spec in apc_specs]
    apc_defaults = {spec.raw_header: spec.default_value for spec in apc_specs}
    apc_minmax = {
        spec.raw_header: {"min": spec.min_value, "max": spec.max_value}
        for spec in apc_specs
    }
    apc_anon_map = {spec.raw_header: spec.index_name for spec in apc_specs}

    io_df = pd.read_json(C.STEP1_JSON_PATH)
    io_df = io_df.rename(columns={"timestamp": "ts", "instrument": "io_channel", "value": "io_value"})
    io_df["ts"] = robust_to_datetime(io_df["ts"])
    io_df = io_df[io_df["ts"].notna()].copy()
    io_df["io_channel"] = io_df["io_channel"].astype(str).str.strip()
    io_df["io_value"] = pd.to_numeric(io_df["io_value"], errors="coerce")
    io_df = io_df[io_df["io_channel"].ne("") & io_df["io_value"].notna()].copy()
    io_df = io_df.sort_values("ts").reset_index(drop=True)

    apc_state = pd.read_csv(C.STEP2_STATE_PATH, sep="\t", low_memory=False)
    #apc_state["ts"] = robust_to_datetime(apc_state["ts"])
    #apc_state = apc_state[apc_state["ts"].notna()].copy()
    #apc_state = ensure_apc_columns(apc_state, apc_headers, apc_defaults)
    #apc_state[apc_headers] = apc_state[apc_headers].fillna(value=apc_defaults)
    #apc_state = apc_state.sort_values("ts").reset_index(drop=True)

    aligned = pd.merge_asof(
        io_df.sort_values("ts"),
        apc_state.sort_values("ts"),
        on="ts",
        direction="backward",
        allow_exact_matches=True,
    )

    events_without_history = int(aligned[apc_headers].isna().all(axis=1).sum())
    aligned = aligned[~aligned[apc_headers].isna().all(axis=1)].copy()
    if aligned.empty:
        raise ValueError("对齐后没有可用样本，请检查日志时间和 APC 时间线。")

    aligned[apc_headers] = aligned[apc_headers].fillna(value=apc_defaults)
    book = load_password_book(C.MAPPING_FILE)
    io_map = update_(book, aligned["io_channel"].tolist())
    save_password_book(C.MAPPING_FILE, book)

    aligned["io_id"] = aligned["io_channel"].astype(str).str.strip().map(io_map)
    if aligned["io_id"].isna().any():
        missing = aligned.loc[aligned["io_id"].isna(), "io_channel"].drop_duplicates().tolist()
        raise ValueError(f"以下 io_channel 未能映射到 io_id: {missing}")

    aligned["io_id"] = aligned["io_id"].astype(int)



    dt_sec = aligned["ts"].diff().dt.total_seconds().fillna(0.0).clip(lower=0)
    aligned["dt_sec"] = dt_sec
    aligned["log_dt_sec"] = np.log1p(dt_sec)

    sec_of_day = (
        aligned["ts"].dt.hour * 3600
        + aligned["ts"].dt.minute * 60
        + aligned["ts"].dt.second
        + aligned["ts"].dt.microsecond / 1_000_000.0
    )
    angle = 2 * np.pi * (sec_of_day / 86400.0)
    aligned["tod_sin"] = np.sin(angle)
    aligned["tod_cos"] = np.cos(angle)

    required_numeric = ["io_value", "io_id", "dt_sec", "log_dt_sec", "tod_sin", "tod_cos", *apc_headers]
    aligned[required_numeric] = aligned[required_numeric].replace([np.inf, -np.inf], np.nan)
    before_clean = len(aligned)
    aligned = aligned[aligned[required_numeric].notna().all(axis=1)].copy()
    after_clean = len(aligned)

    aligned = aligned.sort_values("ts").reset_index(drop=True)
    aligned = apply_config_minmax(aligned, apc_headers, apc_minmax)

    book = load_password_book(C.MAPPING_FILE)
    inst_map = update_io_mapping(book, aligned["io_channel"].tolist())
    save_password_book(C.MAPPING_FILE, book)

    out = aligned.copy()
    out["io_channel"] = out["io_channel"].astype(str).map(inst_map)
    out = out.rename(columns=apc_anon_map)
    apc_anon_headers = [apc_anon_map[header] for header in apc_headers]

    n_train, n_val = resolve_train_val_sizes(len(out), C.TRAIN_RATIO, C.VAL_RATIO)
    train_df = out.iloc[:n_train].copy()
    val_df = out.iloc[n_train:].copy()

    safe_write_csv(out, C.STEP3_EVENT_PATH)
    safe_write_csv(train_df, C.STEP3_TRAIN_PATH)
    safe_write_csv(val_df, C.STEP3_VAL_PATH)

    meta = {
        "task": "semiconductor_io_to_apc_forecasting",
        "alignment": {
            "method": "merge_asof(direction='backward')",
            "uses_future_apc": False,
            "events_without_history_dropped": events_without_history,
        },
        "timestamp_col": "ts",
        "io_channel_col": "io_channel",
        "io_value_col": "io_value",
        "raw_apc_headers": apc_headers,
        "anonymized_apc_headers": apc_anon_headers,
        "apc_header_mapping": apc_anon_map,
        "num_apc": len(apc_anon_headers),
        "num_io": int(out["io_channel"].nunique()),
        "feature_cols": ["io_value", *apc_anon_headers, "io_id", "dt_sec", "log_dt_sec", "tod_sin", "tod_cos"],
        "engineered_feature_cols": ["io_id", "dt_sec", "log_dt_sec", "tod_sin", "tod_cos"],
        "enc_in_raw": len(["io_value", *apc_anon_headers, "io_id", "dt_sec", "log_dt_sec", "tod_sin", "tod_cos"]),
        "c_out": len(apc_anon_headers),
        "apc_minmax_scaler": apc_minmax,
        "apc_default_values": apc_defaults,
        "apc_header_source": "pipeline_config.GAS_HEADERS",
        "password_book": str(Path(C.MAPPING_FILE).resolve()),
        "split": {
            "mode": "train_val_only",
            "requested_train_ratio": C.TRAIN_RATIO,
            "requested_val_ratio": C.VAL_RATIO,
            "effective_train_rows": int(len(train_df)),
            "effective_val_rows": int(len(val_df)),
        },
    }

    summary = {
        "step1_input": str(C.STEP1_JSON_PATH),
        "step2_input": str(C.STEP2_STATE_PATH),
        "aligned_rows_before_clean": int(before_clean),
        "aligned_rows_after_clean": int(after_clean),
        "dropped_rows_due_to_nan_or_inf": int(before_clean - after_clean),
        "events_without_apc_history": int(events_without_history),
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "num_io": int(out["io_channel"].nunique()),
        "num_apc": int(len(apc_anon_headers)),
        "outputs": {
            "event_table": str(C.STEP3_EVENT_PATH.resolve()),
            "train": str(C.STEP3_TRAIN_PATH.resolve()),
            "val": str(C.STEP3_VAL_PATH.resolve()),
            "meta": str(C.STEP3_META_PATH.resolve()),
            "summary": str(C.STEP3_SUMMARY_PATH.resolve()),
        },
    }

    with open(C.STEP3_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(C.STEP3_SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("===== Step 3 完成 =====")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
