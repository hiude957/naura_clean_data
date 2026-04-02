from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class ApcHeaderSpec:
    """APC 列配置。

    raw_header: 原始 APC 表头
    index_name: 输出到数据集时使用的脱敏列名
    min_value: 归一化下界
    max_value: 归一化上界
    default_value: 缺列 / 空白时使用的默认值
    """

    raw_header: str
    index_name: str
    min_value: float
    max_value: float
    default_value: float


# ==============================
# 用户只需要改这里
# ==============================

# 1) 原始日志目录（*.txt）
LOG_INPUT_DIR = r"\\192.168.68.11\你的路径\log\20260310"

# 2) APC / LiveData 目录（*.dc）
DC_INPUT_DIR = r"\\192.168.68.11\你的路径\livedata-apc\20260310"

# 3) 输出根目录
OUTPUT_ROOT = r"D:\\project\\semiconductor_preprocess\\20260310"

# 4) 密码本（仅用于日志 instrument 的稳定脱敏）
MAPPING_FILE = r"D:\\project\\semiconductor_preprocess\\password_book.json"

# 5) APC 配置。
#    每一行都是一个二维数组元素，格式固定为：
#    [原始表头, 脱敏列名, min, max, 默认值]
#
#    示例：
#    ["APC_HEADER_1", "Col_0", 0.0, 100.0, 0.0]
#    ["APC_HEADER_2", "Col_1", -1.0, 1.0, 0.0]
GAS_HEADERS = [
    # ["APC_HEADER_1", "Col_0", 0.0, 100.0, 0.0],
    # ["APC_HEADER_2", "Col_1", 0.0, 1.0, 0.0],
]

# 6) 训练集 / 验证集比例
#    当前只切 train / val，不再输出 test。
#    如果 TRAIN_RATIO + VAL_RATIO < 1，剩余部分会自动并入 val。
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15

# 7) 是否保存调试中间文件
SAVE_DEBUG_FILES = True

# 8) 输出文件命名
#    中间文件统一用 txt/json，避免在内网被 Excel 类程序直接占用/上锁
STEP1_JSON = "step1_log_events.json"
STEP1_TXT = "step1_log_events.txt"
STEP1_SUMMARY_JSON = "step1_log_summary.json"
STEP1_DROPPED_VALUE_TXT = "step1_dropped_invalid_value.txt"
STEP2_STATE_TXT = "step2_apc_state_raw.txt"
STEP2_SUMMARY_JSON = "step2_apc_summary.json"
STEP2_MISSING_HEADER_TXT = "step2_apc_header.txt"
STEP3_EVENT_CSV = "event_table.csv"
STEP3_TRAIN_CSV = "train.csv"
STEP3_VAL_CSV = "val.csv"
STEP3_TEST_CSV = "test.csv"  # 兼容保留，当前不再输出
STEP3_META_JSON = "meta.json"
STEP3_SUMMARY_JSON = "summary.json"

# 9) 日志正则规则
#    在这里补齐你现场验证好的规则。
#    注意：控制命令 value 的归一化由你在 processor 里自己完成。
MATCH_RULES = [
    {
        "name": "V1~V9",
        "pattern": r"/Control/PM5000/control/sw/((?:V\d+(?:Edge)?|FinalValve(?:Edge)?)):(do_(Open|Close)Valve start)",
        "processor": lambda m: (
            "V5Center_DOV5" if m.group(1) == "V5" else
            "FinalEdge_FinalValveEdgeDo" if m.group(1) == "FinalValveEdge" else
            f"{m.group(1)}_Do{m.group(1)}",
            m.group(2),
            1 if m.group(3) == "Open" else 0,
        ),
    },
]


@lru_cache(maxsize=1)
def get_apc_header_specs() -> Tuple[ApcHeaderSpec, ...]:
    """把 GAS_HEADERS 解析成结构化配置。

    注意：
    - 这里做延迟校验，避免 Step 1 因为 APC 配置未填写而无法运行。
    - Step 2 / Step 3 需要 APC 配置时再调用这个函数。
    """

    if not GAS_HEADERS:
        raise ValueError("GAS_HEADERS 为空，请先在 pipeline_config.py 中填写 APC 配置。")

    specs: List[ApcHeaderSpec] = []
    seen_headers = set()
    seen_indices = set()

    for idx, row in enumerate(GAS_HEADERS):
        if not isinstance(row, (list, tuple)) or len(row) != 5:
            raise ValueError(
                f"GAS_HEADERS[{idx}] 格式错误，必须是长度为 5 的数组："
                "[原始表头, 脱敏列名, min, max, 默认值]"
            )

        raw_header, index_name, min_value, max_value, default_value = row
        raw_header = str(raw_header).strip()
        index_name = str(index_name).strip()

        if not raw_header:
            raise ValueError(f"GAS_HEADERS[{idx}] 的原始表头不能为空。")
        if not index_name:
            raise ValueError(f"GAS_HEADERS[{idx}] 的脱敏列名不能为空。")
        if raw_header in seen_headers:
            raise ValueError(f"GAS_HEADERS 中原始表头重复：{raw_header}")
        if index_name in seen_indices:
            raise ValueError(f"GAS_HEADERS 中脱敏列名重复：{index_name}")

        try:
            min_value = float(min_value)
            max_value = float(max_value)
            default_value = float(default_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"GAS_HEADERS[{idx}] 的 min / max / 默认值必须能转成数字。"
            ) from exc

        if max_value <= min_value:
            raise ValueError(
                f"GAS_HEADERS[{idx}] 的 max 必须大于 min："
                f"raw_header={raw_header}, min={min_value}, max={max_value}"
            )

        specs.append(
            ApcHeaderSpec(
                raw_header=raw_header,
                index_name=index_name,
                min_value=min_value,
                max_value=max_value,
                default_value=default_value,
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


# ==============================
# 一般不用改的内部路径
# ==============================
ROOT = Path(OUTPUT_ROOT)
ROOT.mkdir(parents=True, exist_ok=True)

STEP1_JSON_PATH = ROOT / STEP1_JSON
STEP1_TXT_PATH = ROOT / STEP1_TXT
STEP1_SUMMARY_PATH = ROOT / STEP1_SUMMARY_JSON
STEP1_DROPPED_VALUE_PATH = ROOT / STEP1_DROPPED_VALUE_TXT
STEP2_STATE_PATH = ROOT / STEP2_STATE_TXT
STEP2_SUMMARY_PATH = ROOT / STEP2_SUMMARY_JSON
STEP2_MISSING_HEADER_PATH = ROOT /STEP2_MISSING_HEADER_TXT
STEP3_EVENT_PATH = ROOT / STEP3_EVENT_CSV
STEP3_TRAIN_PATH = ROOT / STEP3_TRAIN_CSV
STEP3_VAL_PATH = ROOT / STEP3_VAL_CSV
STEP3_TEST_PATH = ROOT / STEP3_TEST_CSV  # 兼容保留，当前不再使用
STEP3_META_PATH = ROOT / STEP3_META_JSON
STEP3_SUMMARY_PATH = ROOT / STEP3_SUMMARY_JSON
