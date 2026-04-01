#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤 2：解析 APC / LiveData 的 .dc 文件，按配置构建 APC 状态表。

输入：
- DC_INPUT_DIR 下的 *.dc
- pipeline_config.GAS_HEADERS（二维数组配置）

输出：
- step2_apc_state_raw.txt （制表符分隔文本）
- step2_apc_summary.json

说明：
- 日志处理逻辑不变，这一步只处理 APC
- 输出 APC 列严格按照配置顺序生成
- 某列在某个 .dc 文件中不存在时，整列直接用默认值补齐
- 某列存在但局部空白时，先按时间前向填充，再用默认值补掉最前面的空白
- 遇到被占用/上锁/权限不足的文件，会在终端明确打印并跳过
"""

from __future__ import annotations

import glob
import json
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

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


def read_one_dc_file(
    file_path: str,
    apc_headers: Sequence[str],
    apc_defaults: Dict[str, float],
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """把单个 .dc 文件标准化成统一列结构。"""

    raw_df = pd.read_csv(file_path, sep="\t", low_memory=False)
    if raw_df.empty:
        return pd.DataFrame(columns=["ts", *apc_headers]), [], list(apc_headers)

    ts_col = raw_df.columns[0]
    out = pd.DataFrame({"ts": robust_to_datetime(raw_df[ts_col])})
    out = out[out["ts"].notna()].copy()

    present_headers: List[str] = []
    missing_headers: List[str] = []

    for header in apc_headers:
        if header in raw_df.columns:
            values = pd.to_numeric(raw_df.loc[out.index, header], errors="coerce")
            out[header] = values
            present_headers.append(header)
        else:
            out[header] = apc_defaults[header]
            missing_headers.append(header)

    out = out.reset_index(drop=True)
    return out, present_headers, missing_headers


def main() -> None:
    dc_dir = Path(C.DC_INPUT_DIR)
    if not dc_dir.exists():
        raise FileNotFoundError(f"找不到 APC 目录: {dc_dir}")

    apc_specs = C.get_apc_header_specs()
    apc_headers = [spec.raw_header for spec in apc_specs]
    apc_defaults = {spec.raw_header: spec.default_value for spec in apc_specs}

    files = sorted(glob.glob(str(dc_dir / "*.dc")))
    if not files:
        raise FileNotFoundError(f"目录下没有 .dc 文件: {dc_dir}")

    dfs: List[pd.DataFrame] = []
    headers_found_in_any_file = set()
    locked_files: List[str] = []
    failed_files: List[str] = []
    parsed_files = 0
    structural_default_cells = 0

    for f in files:
        try:
            df, present_headers, missing_headers = read_one_dc_file(f, apc_headers, apc_defaults)
            parsed_files += 1
            dfs.append(df)
            headers_found_in_any_file.update(present_headers)
            structural_default_cells += len(df) * len(missing_headers)
            with open(C.STEP2_MISSING_HEADER_PATH, 'a', encoding="utf-8") as log_f:
                log_f.write(f"[File]:{f}\n")
                log_f.write(f"row={len(df)}\n")
                log_f.write(f"miss_headers_count={len(missing_headers)}\n")
                if missing_headers:
                    log_f.write("missing_headers:\n")
                    for h in missing_headers:
                        log_f.write(f"   - {h}\n")
                else:
                    log_f.write("missing_headers: <NONE>\n")
                log_f.write("\n")

                  

        except PermissionError as e:
            print(f"[LOCKED][STEP2] 文件被占用或无权限打开，已跳过: {f} | {e}")
            locked_files.append(str(f))
        except OSError as e:
            msg = str(e).lower()
            if "used by another process" in msg or "permission denied" in msg or "sharing violation" in msg:
                print(f"[LOCKED][STEP2] 文件可能被占用/上锁，已跳过: {f} | {e}")
                locked_files.append(str(f))
            else:
                print(f"[ERROR][STEP2] 读取失败，已跳过: {f} | {e}")
                failed_files.append(str(f))
        except Exception as e:
            print(f"[ERROR][STEP2] 读取失败，已跳过: {f} | {e}")
            failed_files.append(str(f))

    if not dfs:
        raise ValueError("没有成功读取任何 .dc 文件，请检查目录、权限或文件锁。")

    apc_state = pd.concat(dfs, axis=0, ignore_index=True)
    apc_state = apc_state.sort_values("ts").reset_index(drop=True)

    apc_state[apc_headers] = apc_state[apc_headers].ffill()
    leading_default_cells = int(apc_state[apc_headers].isna().sum().sum())
    apc_state[apc_headers] = apc_state[apc_headers].fillna(value=apc_defaults)

    apc_state.to_csv(C.STEP2_STATE_PATH, index=False, sep="\t")

    missing_in_all_files = [h for h in apc_headers if h not in headers_found_in_any_file]
    summary = {
        "dc_dir": str(dc_dir),
        "total_dc_files": len(files),
        "parsed_files": parsed_files,
        "locked_files_count": len(locked_files),
        "failed_files_count": len(failed_files),
        "locked_files": locked_files,
        "failed_files": failed_files,
        "configured_apc_count": len(apc_headers),
        "configured_apc_headers": apc_headers,
        "headers_found_in_any_file_count": len(headers_found_in_any_file),
        "headers_found_in_any_file": sorted(headers_found_in_any_file),
        "headers_missing_in_all_files_count": len(missing_in_all_files),
        "headers_missing_in_all_files": missing_in_all_files,
        "default_fill": {
            "structural_default_cells": int(structural_default_cells),
            "leading_default_cells_after_ffill": int(leading_default_cells),
            "total_default_cells": int(structural_default_cells + leading_default_cells),
        },
        "apc_state_rows": int(len(apc_state)),
        "output_txt": str(C.STEP2_STATE_PATH),
    }
    with open(C.STEP2_SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("===== Step 2 完成 =====")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
