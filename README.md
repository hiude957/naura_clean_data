# clean_data

这是一个面向半导体数字孪生项目的数据脱敏工具集，主要负责两件事：

- 从 `log` 文本中提取控制命令并输出脱敏结果
- 从 `APC / LiveData` 的 `.dc` 文本中提取传感器数据并输出脱敏结果

另外提供 `build_aligned_daily_dataset.py`，用于把已经脱敏并归一化的 `log`、`apc`、`livedata` 结果合并为逐日对齐表和训练表。

## 输入范围

### Log

目录结构：

```text
LOG_INPUT_ROOT/
  20260317/
    *.txt
  20260318/
    *.txt
```

程序只读取 `*.txt`。

### APC / LiveData

目录结构：

```text
DC_INPUT_ROOT/
  20260317/
    *.dc
  20260318/
    *.dc
```

程序只读取 `*.dc`。

### Excel 文件说明

Excel 类文件因为加密不可读取，不属于本程序输入范围。

- `.xls`
- `.xlsx`
- 其他非目标扩展名

这些文件会被忽略，并记录到对应日期的报告文件里，但不会作为解析失败处理。

## 当前入口脚本

- `process_log_data.py`
  - 处理 `log` 文本
  - 单次运行内同时输出 `raw` 与 `anonymized`
- `process_sensor_data.py`
  - 处理 `APC / LiveData` 的 `.dc`
  - 单次运行内同时输出 `raw` 与 `anonymized`
- `build_aligned_daily_dataset.py`
  - 输入 `anonymized(0316~0407)` 下的已处理 `log` 与 `sensor`
  - 输出逐日原始对齐表与逐日训练表
  - 只使用 Python 标准库，不依赖 pandas

## 对齐训练表生成

运行：

```bash
python3 clean_data/build_aligned_daily_dataset.py
```

指定输出目录或指定日期：

```bash
python3 clean_data/build_aligned_daily_dataset.py --output-root aligned_output --days 20260316 20260319
```

即使指定 `--days`，脚本仍会从整个输入根目录扫描完整 `io_id` 集合，保证输出列结构一致。

默认输入：

```text
/home/lyh/naura/anonymized(0316~0407)
```

默认输出：

```text
/home/lyh/naura/aligned_output
```

输出目录结构：

```text
aligned_output/
  original_daily/
    20260316/
      20260316_aligned_original.tsv
  train_daily/
    20260316/
      20260316_aligned_train.tsv
  reports/
    20260316_aligned_report.json
    aligned_summary.json
```

核心规则：

- APC 的 `Time` 会换算成绝对时间戳；输出中保留 `timestamp_raw` 与 `ts_ms`，不保留原始 `Time`。
- APC 与 LiveData 同一毫秒完全重合时保留 APC 行，丢弃该毫秒的 LiveData 行。
- LiveData 原始行保留；只在 APC 未覆盖的 LiveData 时间段内按 `110ms` 做线性插值。
- `log` 中相同 `timestamp + io_id` 的 action 只保留后发生的一条；相同 timestamp 的不同 io_id 会写到同一时刻的 `evt_*` 与 `state_*`。
- action 精确落在 sensor 行时间戳上时，写到该 sensor 行；否则插入 action 行并继承上一条 sensor 状态。
- 插入 action 行继承 APC 时 `source=logapc`，继承 LiveData 时 `source=loglivedata`，超出 sensor 覆盖范围时 `source=logonly`。
- 原始对齐表保留 `source` 字符串；训练表删除全部 `logonly` 行，并把来源映射为 `source_code`：`1=apc`，`2=livedata`，`3=logapc`，`4=loglivedata`。
- 训练表列包含 `timestamp_raw`、`ts_ms`、`source_code`、全部 sensor/mask 列、`evt_{id}`、`state_{id}`。

## 公共模块

```text
pipeline_utils/
  __init__.py
  time_utils.py
  io_mapping.py
  dc_utils.py
```

- `time_utils.py`
  - 统一时间解析
- `io_mapping.py`
  - 统一 `password_book.json` 的读取、保存、ID 分配
- `dc_utils.py`
  - 统一 `.dc` 双格式识别、读取、列清洗、按源格式写回

## 配置方式

配置文件：`pipeline_config.py`

核心配置：

- `LOG_INPUT_ROOT`
- `DC_INPUT_ROOT`
- `OUTPUT_ROOT`
- `MAPPING_FILE`
- `PROCESS_DAYS`
- `GAS_HEADERS`
- `MATCH_RULES`

`GAS_HEADERS` 格式：

```python
[raw_header, index_name, default_value, min_value, max_value]
```

## 输出目录结构

```text
OUTPUT_ROOT/
  raw/
    20260317/
      log/
        20260317_log_raw.txt
        20260317_log_report.txt
      sensor/
        apc_20260317_103726.txt
        livedata.txt
        livedata_2.txt
        20260317_sensor_report.txt
  anonymized/
    20260317/
      log/
        20260317_log_anonymized.txt
      sensor/
        apc_20260317_103726_anonymized.txt
        livedata_anonymized.txt
        livedata_2_anonymized.txt
```

## 命名规则

### Log

- raw：`YYYYMMDD_log_raw.txt`
- anonymized：`YYYYMMDD_log_anonymized.txt`
- report：`YYYYMMDD_log_report.txt`

### Sensor

#### APC

如果文件首行是：

```text
Process Start Time: 2026/3/17 10:37:26
```

输出基础名为：

```text
apc_20260317_103726.txt
```

如果同一天出现多个 APC 文件解析出相同的开始时间，则自动追加编号：

```text
apc_20260317_103726.txt
apc_20260317_103726_2.txt
```

脱敏版只在基础名后追加 `_anonymized`：

```text
apc_20260317_103726_anonymized.txt
```

#### LiveData

如果当天只有 1 个成功处理的 LiveData：

```text
livedata.txt
```

如果当天有多个成功处理的 LiveData：

```text
livedata_1.txt
livedata_2.txt
```

脱敏版同样只追加 `_anonymized`：

```text
livedata_1_anonymized.txt
```

## 报告文件

### Log 报告

位置：

```text
raw/YYYYMMDD/log/YYYYMMDD_log_report.txt
```

内容包括：

- 成功处理的文件
- 被忽略的文件
- 读取失败或解析失败的文件
- 匹配行数与有效行数
- 各类被过滤行的统计
- 输出文件路径

### Sensor 报告

位置：

```text
raw/YYYYMMDD/sensor/YYYYMMDD_sensor_report.txt
```

内容包括：

- 每个成功处理文件对应的输入路径
- 文件类型：`apc` 或 `livedata`
- raw 输出路径
- anonymized 输出路径
- 缺失的 header
- 无效时间行数
- 被忽略的文件
- 读取失败或解析失败的文件

## 运行方式

安装依赖：

```bash
pip install -r requirements.txt
```

运行：

```bash
python process_log_data.py
python process_sensor_data.py
```

如果只想处理指定日期，可以在 `pipeline_config.py` 中设置：

```python
PROCESS_DAYS = ["20260317"]
```
