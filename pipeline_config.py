from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class ApcHeaderSpec:
    """Structured config for one APC header."""

    raw_header: str
    index_name: str
    default_value: float
    min_value: float
    max_value: float


@dataclass(frozen=True)
class InputDay:
    """One discovered input day directory."""

    day: str
    path: Path


@dataclass(frozen=True)
class DayOutputPaths:
    """All output locations for one processing day."""

    day: str
    raw_day_dir: Path
    raw_log_dir: Path
    raw_sensor_dir: Path
    anon_day_dir: Path
    anon_log_dir: Path
    anon_sensor_dir: Path
    raw_log_path: Path
    anon_log_path: Path
    log_report_path: Path
    sensor_report_path: Path


DAY_DIR_REGEX = re.compile(r"^\d{8}$")


# ==============================
# User editable config
# ==============================

# 1) Raw log root.
#    Expected structure:
#    LOG_INPUT_ROOT/
#      20260317/*.txt
#      20260318/*.txt
LOG_INPUT_ROOT = r"\\192.168.68.11\你的路径\log"

# 2) APC / LiveData root.
#    Expected structure:
#    DC_INPUT_ROOT/
#      20260317/*.dc
#      20260318/*.dc
DC_INPUT_ROOT = r"\\192.168.68.11\你的路径\livedata-apc"

# Backward-compatible aliases.
LOG_INPUT_DIR = LOG_INPUT_ROOT
DC_INPUT_DIR = DC_INPUT_ROOT

# 3) Output root
OUTPUT_ROOT = r"D:\\project\\semiconductor_preprocess"

# 4) Mapping file for stable IO channel ids
MAPPING_FILE = r"D:\\project\\semiconductor_preprocess\\password_book.json"

# 5) Optional day filter.
#    Leave empty to auto-discover every available day folder.
PROCESS_DAYS: List[str] = []

# 6) APC config
#    Each row must be:
#    [raw_header, index_name, default_value, min_value, max_value]
GAS_HEADERS = [
    # ["APC_HEADER_1", "col_000", 0.0, 0.0, 100.0],
    # ["APC_HEADER_2", "col_001", 0.0, -1.0, 1.0],
]

# 7) Output subdirectories
RAW_OUTPUT_DIRNAME = "raw"
ANON_OUTPUT_DIRNAME = "anonymized"

# 8) Log regex rules
MATCH_RULES = [
    {
        "name": "V1~V9",
        "pattern": r"/Control/PM5000/control/sw/((?:V\d+(?:Edge)?|FinalValve(?:Edge)?)):(do_(Open|Close)Valve start)",
        "processor": lambda m: (
            "V5Center_DOV5"
            if m.group(1) == "V5"
            else "FinalEdge_FinalValveEdgeDo"
            if m.group(1) == "FinalValveEdge"
            else f"{m.group(1)}_Do{m.group(1)}",
            m.group(2),
            1 if m.group(3) == "Open" else 0,
        ),
    },
]


@lru_cache(maxsize=1)
def get_apc_header_specs() -> Tuple[ApcHeaderSpec, ...]:
    """Validate and convert GAS_HEADERS to typed config."""

    if not GAS_HEADERS:
        raise ValueError("GAS_HEADERS is empty. Please fill APC config in pipeline_config.py first.")

    specs: List[ApcHeaderSpec] = []
    seen_headers = set()
    seen_indices = set()

    for idx, row in enumerate(GAS_HEADERS):
        if not isinstance(row, (list, tuple)) or len(row) != 5:
            raise ValueError(
                f"GAS_HEADERS[{idx}] format error. Expected "
                "[raw_header, index_name, default_value, min_value, max_value]."
            )

        raw_header, index_name, default_value, min_value, max_value = row
        raw_header = str(raw_header).strip()
        index_name = str(index_name).strip()

        if not raw_header:
            raise ValueError(f"GAS_HEADERS[{idx}] raw_header cannot be empty.")
        if not index_name:
            raise ValueError(f"GAS_HEADERS[{idx}] index_name cannot be empty.")
        if raw_header in seen_headers:
            raise ValueError(f"Duplicate raw_header in GAS_HEADERS: {raw_header}")
        if index_name in seen_indices:
            raise ValueError(f"Duplicate index_name in GAS_HEADERS: {index_name}")

        try:
            default_value = float(default_value)
            min_value = float(min_value)
            max_value = float(max_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"GAS_HEADERS[{idx}] default_value / min_value / max_value must be numeric."
            ) from exc

        if max_value <= min_value:
            raise ValueError(
                f"GAS_HEADERS[{idx}] max_value must be greater than min_value: "
                f"raw_header={raw_header}, min={min_value}, max={max_value}"
            )

        specs.append(
            ApcHeaderSpec(
                raw_header=raw_header,
                index_name=index_name,
                default_value=default_value,
                min_value=min_value,
                max_value=max_value,
            )
        )
        seen_headers.add(raw_header)
        seen_indices.add(index_name)

    return tuple(specs)


def get_apc_raw_headers() -> List[str]:
    return [spec.raw_header for spec in get_apc_header_specs()]


def get_apc_index_map() -> Dict[str, str]:
    return {spec.raw_header: spec.index_name for spec in get_apc_header_specs()}


def get_apc_minmax_map() -> Dict[str, Dict[str, float]]:
    return {
        spec.raw_header: {"min": spec.min_value, "max": spec.max_value}
        for spec in get_apc_header_specs()
    }


def get_apc_default_map() -> Dict[str, float]:
    return {spec.raw_header: spec.default_value for spec in get_apc_header_specs()}


def normalize_day_name(name: str) -> str:
    raw_name = str(name).strip()
    if DAY_DIR_REGEX.fullmatch(raw_name):
        return raw_name
    safe_name = re.sub(r"[^0-9A-Za-z_-]+", "_", raw_name).strip("_")
    return safe_name or "current_day"


def discover_input_days(root: str | Path) -> Tuple[InputDay, ...]:
    root_path = Path(root)
    if not root_path.exists():
        raise FileNotFoundError(f"Input root not found: {root_path}")

    selected_days = {str(day).strip() for day in PROCESS_DAYS if str(day).strip()}
    day_dirs: List[InputDay] = []

    for child in sorted(root_path.iterdir()):
        if not child.is_dir():
            continue
        if not DAY_DIR_REGEX.fullmatch(child.name):
            continue
        if selected_days and child.name not in selected_days:
            continue
        day_dirs.append(InputDay(day=child.name, path=child))

    if day_dirs:
        return tuple(day_dirs)

    if any(item.is_file() for item in root_path.iterdir()):
        day = normalize_day_name(root_path.name)
        if selected_days and day not in selected_days:
            return tuple()
        return (InputDay(day=day, path=root_path),)

    return tuple()


ROOT = Path(OUTPUT_ROOT)
ROOT.mkdir(parents=True, exist_ok=True)

RAW_ROOT = ROOT / RAW_OUTPUT_DIRNAME
ANON_ROOT = ROOT / ANON_OUTPUT_DIRNAME


def get_day_output_paths(day: str) -> DayOutputPaths:
    day_name = normalize_day_name(day)

    raw_day_dir = RAW_ROOT / day_name
    raw_log_dir = raw_day_dir / "log"
    raw_sensor_dir = raw_day_dir / "sensor"

    anon_day_dir = ANON_ROOT / day_name
    anon_log_dir = anon_day_dir / "log"
    anon_sensor_dir = anon_day_dir / "sensor"

    for directory in (
        raw_log_dir,
        raw_sensor_dir,
        anon_log_dir,
        anon_sensor_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    return DayOutputPaths(
        day=day_name,
        raw_day_dir=raw_day_dir,
        raw_log_dir=raw_log_dir,
        raw_sensor_dir=raw_sensor_dir,
        anon_day_dir=anon_day_dir,
        anon_log_dir=anon_log_dir,
        anon_sensor_dir=anon_sensor_dir,
        raw_log_path=raw_log_dir / f"{day_name}_log_raw.txt",
        anon_log_path=anon_log_dir / f"{day_name}_log_anonymized.txt",
        log_report_path=raw_log_dir / f"{day_name}_log_report.txt",
        sensor_report_path=raw_sensor_dir / f"{day_name}_sensor_report.txt",
    )
