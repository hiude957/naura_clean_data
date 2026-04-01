# clean_data

这是一个面向半导体工艺数据的三步预处理脚本集合，用来把原始日志和 APC / LiveData 数据整理成可用于训练的表格数据。

核心流程如下：

1. 从原始 `log` 文本中提取控制命令事件。
2. 从 `APC / LiveData` 的 `.dc` 文件中构建按时间排序的 APC 状态表。
3. 将控制事件与 APC 状态按时间对齐，生成训练用数据集与元数据。

## 目录结构

- `pipeline_config.py`
  - 统一配置入口，包含输入输出路径、APC 列配置、训练集划分比例、日志匹配规则等。
- `step1_clean_log_commands.py`
  - 清洗原始日志，提取控制命令事件。
- `step2_build_apc_state.py`
  - 解析 `.dc` 文件，生成 APC 状态表。
- `step3_align_and_prepare_dataset.py`
  - 对齐 Step 1 / Step 2 的结果，输出训练集和说明文件。
- `requestment.txt`
  - 本项目依赖清单。

## 脚本职责说明

### 1. `step1_clean_log_commands.py`

输入：

- `pipeline_config.LOG_INPUT_DIR` 目录下的 `*.txt`

处理逻辑：

- 按行读取日志文本。
- 使用 `pipeline_config.MATCH_RULES` 中定义的正则规则匹配控制命令。
- 解析出 `timestamp`、`instrument`、`action`、`value`。
- 对时间做鲁棒解析，并过滤空值与非法值。

输出：

- `step1_log_events.json`
- `step1_log_events.txt`
- `step1_log_summary.json`

对应关键位置：

- 配置规则入口：[pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py#L82)
- Step 1 主逻辑：[step1_clean_log_commands.py](/C:/Users/24152/Desktop/naura/clean_data/step1_clean_log_commands.py#L71)

### 2. `step2_build_apc_state.py`

输入：

- `pipeline_config.DC_INPUT_DIR` 目录下的 `*.dc`
- `pipeline_config.GAS_HEADERS` 中定义的 APC 列配置

处理逻辑：

- 逐个读取 `.dc` 文件。
- 以首列作为时间戳列。
- 按 `GAS_HEADERS` 指定顺序提取 APC 列。
- 缺失列直接使用默认值补齐。
- 对已存在但局部为空的 APC 值先做前向填充，再用默认值补齐最前面的空值。

输出：

- `step2_apc_state_raw.txt`
- `step2_apc_summary.json`
- `step2_apc_header.txt`（记录每个 `.dc` 文件缺失的 APC 表头）

对应关键位置：

- APC 配置定义：[pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py#L50)
- APC 配置校验：[pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py#L98)
- Step 2 主逻辑：[step2_build_apc_state.py](/C:/Users/24152/Desktop/naura/clean_data/step2_build_apc_state.py#L85)

### 3. `step3_align_and_prepare_dataset.py`

输入：

- Step 1 输出的 `step1_log_events.json`
- Step 2 输出的 `step2_apc_state_raw.txt`

处理逻辑：

- 将控制事件重命名为训练字段，如 `io_channel`、`io_value`。
- 使用 `merge_asof(direction='backward')` 将每个控制事件对齐到它发生前最近的一条 APC 状态。
- 基于配置中的 `min/max` 对 APC 数值归一化。
- 生成时间差、对数时间差、日内周期正余弦等特征。
- 按 `TRAIN_RATIO` / `VAL_RATIO` 切分训练集和验证集。

输出：

- `event_table.csv`
- `train.csv`
- `val.csv`
- `meta.json`
- `summary.json`

对应关键位置：

- 训练集比例配置：[pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py#L58)
- Step 3 主逻辑：[step3_align_and_prepare_dataset.py](/C:/Users/24152/Desktop/naura/clean_data/step3_align_and_prepare_dataset.py#L143)
- 时间对齐逻辑：[step3_align_and_prepare_dataset.py](/C:/Users/24152/Desktop/naura/clean_data/step3_align_and_prepare_dataset.py#L174)
- APC 归一化逻辑：[step3_align_and_prepare_dataset.py](/C:/Users/24152/Desktop/naura/clean_data/step3_align_and_prepare_dataset.py#L122)

## 配置说明

项目主要通过 [pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py) 控制。

建议优先检查以下配置项：

- `LOG_INPUT_DIR`
  - 原始日志目录，供 Step 1 使用。
- `DC_INPUT_DIR`
  - APC / LiveData 数据目录，供 Step 2 使用。
- `OUTPUT_ROOT`
  - 所有中间结果和最终结果的输出目录。
- `MAPPING_FILE`
  - IO 通道映射文件路径。
- `GAS_HEADERS`
  - APC 字段配置，格式固定为：
    - `[原始表头, 脱敏列名, min, max, 默认值]`
- `TRAIN_RATIO` / `VAL_RATIO`
  - 训练集和验证集比例。
- `MATCH_RULES`
  - 日志正则匹配规则，决定哪些控制命令会被抽取出来。

## 运行方式

建议使用 Python 3.10 及以上版本。

安装依赖：

```bash
pip install -r requestment.txt
```

按顺序执行：

```bash
python step1_clean_log_commands.py
python step2_build_apc_state.py
python step3_align_and_prepare_dataset.py
```

如果是 Windows 并使用 `py` 启动器，也可以：

```bash
py -3 step1_clean_log_commands.py
py -3 step2_build_apc_state.py
py -3 step3_align_and_prepare_dataset.py
```

## 输出结果说明

整个流程的输出目录由 `OUTPUT_ROOT` 决定。

通常会产生以下文件：

- Step 1
  - `step1_log_events.json`
  - `step1_log_events.txt`
  - `step1_log_summary.json`
- Step 2
  - `step2_apc_state_raw.txt`
  - `step2_apc_summary.json`
  - `step2_apc_header.txt`
- Step 3
  - `event_table.csv`
  - `train.csv`
  - `val.csv`
  - `meta.json`
  - `summary.json`

## 依赖说明

第三方依赖很少，只有：

- `pandas`
- `numpy`

其余依赖均来自 Python 标准库，例如 `json`、`pathlib`、`glob`、`re`、`dataclasses` 等。

## 已知注意事项

1. `GAS_HEADERS` 默认是空列表。
   - 如果不先在 [pipeline_config.py](/C:/Users/24152/Desktop/naura/clean_data/pipeline_config.py#L50) 中填写 APC 配置，Step 2 和 Step 3 会直接报错。

2. 当前终端环境里没有可用的 Python 解释器。
   - 我本次只能静态阅读代码并整理文档，无法在本机实际跑通脚本验证输出。

3. `step3_align_and_prepare_dataset.py` 中存在一处明显的调用名不一致。
   - 在 [step3_align_and_prepare_dataset.py](/C:/Users/24152/Desktop/naura/clean_data/step3_align_and_prepare_dataset.py#L189) 调用了 `update_(...)`。
   - 但文件中实际定义的是 `update_io_mapping(...)`，位置在 [step3_align_and_prepare_dataset.py](/C:/Users/24152/Desktop/naura/clean_data/step3_align_and_prepare_dataset.py#L79)。
   - 这会导致 Step 3 在运行到该位置时报错。你这次要求不修改代码，所以这里仅记录，不做修复。

4. 日志与 APC 文件都包含“文件被占用 / 权限不足”的跳过逻辑。
   - 如果源文件位于共享目录或正在被其他程序占用，脚本会跳过对应文件，并把统计信息写进 summary。

## 适合补充的后续内容

如果你后面还需要，我可以继续补：

- 标准命名的 `requirements.txt`
- 更详细的字段说明表
- 一份面向新同事的中文使用手册
