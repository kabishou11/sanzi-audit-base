# 三资专项审计报告处理说明

这是一个用于批量处理“三资专项审计报告”的项目，目标是把人工报告中的经验沉淀出来，分成 3 层产物：

1. 原始提取
   只提取“审计发现的主要问题”这一章，不改写原文。
2. 单报告分析
   对每一份报告里的子标题和正文做结构化分析，提炼问题类型、风险标签、候选规则。
3. 汇总分析
   把所有报告放在一起做聚合与模型长文分析，输出总报告、核心频次表和规则沉淀建议。

如果你现在最关心“跑完后到底会得到什么”，先看本文的“产出物说明”和“最常用流程”两节。

如果你想直接运行最近整理好的几个独立脚本，优先看：
- `scripts/README.md`

## 一、目录结构

项目当前主要目录如下：

```text
input_reports/
  sample/                 样例输入目录
  batch/                  批量输入目录

outputs/
  extracted_md/           第一步：原始提取结果
  per_report_analysis/    第二步：单报告分析结果
  aggregate/              第三步：汇总分析结果
  cache/                  中间缓存
    llm_raw/              大模型原始响应缓存
  logs/                   日志目录
```

你后续批量处理时，直接把报告放到 `input_reports/batch/` 即可。

## 二、输入文件放哪里

样例测试目录：
- `input_reports/sample/`

正式批量处理目录：
- `input_reports/batch/`

当前支持：
- `.doc`
- `.docx`

程序会自动跳过类似 `~$xxx.doc` 这种 Word 临时锁文件。

## 三、最常用流程

### 1. 首次初始化环境

```powershell
uv venv .venv --python 3.13
uv sync
```

### 2. 配置 `.env`

如果还没有 `.env`，先从模板复制：

```powershell
Copy-Item .env.example .env
```

然后填写模型配置。当前项目已经按 OpenAI 兼容接口方式接好，像 MiniMax 这种兼容接口可以直接用：

```env
OPENAI_API_KEY=你的密钥
OPENAI_BASE_URL=https://你的兼容接口地址/v1
OPENAI_MODEL=MiniMax-M2.7
OPENAI_REASONING_SPLIT=true
OPENAI_TIMEOUT=120
OPENAI_MAX_RETRIES=3
OPENAI_PROMPT_VERSION=v1
AGGREGATE_NARRATIVE_ENABLED=true
AGGREGATE_NARRATIVE_MAX_SUBSECTIONS=120
AGGREGATE_NARRATIVE_TARGET_LENGTH=4500
AGGREGATE_NARRATIVE_TEMPERATURE=0.3

INPUT_DIR=input_reports/sample
OUTPUT_DIR=outputs
LOG_LEVEL=INFO
DOC_CONVERTER=auto
KEEP_INTERMEDIATE=true
```

如果 `OPENAI_API_KEY` 留空，程序仍然可以运行，但第二步分析会退化为纯规则分析，不会调用大模型。

其中汇总长文分析相关配置含义：
- `AGGREGATE_NARRATIVE_ENABLED`：是否启用汇总长文分析（预留开关）。
- `AGGREGATE_NARRATIVE_MAX_SUBSECTIONS`：用于汇总长文分析的子标题样本上限。
- `AGGREGATE_NARRATIVE_TARGET_LENGTH`：目标报告长度提示（字符数）。
- `AGGREGATE_NARRATIVE_TEMPERATURE`：汇总长文生成温度（建议低温保证稳定性）。

### 3. 运行整套流程

处理样例目录：

```powershell
uv run audit-pipeline run-all --input-dir input_reports/sample
```

处理批量目录：

```powershell
uv run audit-pipeline run-all --input-dir input_reports/batch
```

如果你只想跑某一步，也可以单独执行：

```powershell
uv run audit-pipeline extract --input-dir input_reports/batch
uv run audit-pipeline analyze --input-dir input_reports/batch
uv run audit-pipeline aggregate --input-dir input_reports/batch
```

## 四、每一步到底做什么

### 第一步：原始提取 `extract`

这一阶段只做“原始提取”，不会改写正文。

处理逻辑：
- 读取 `.doc/.docx`
- 如果是 `.doc`，先自动转成 `.docx`
- 定位标题“审计发现的主要问题”
- 从该标题开始，截到下一个同级标题之前
- 输出 Markdown 和结构化 JSON

这一阶段的目标是：
- 确保你后续看到的原文是可审计、可追溯的
- 不让模型直接参与原文提取，避免内容被改写

### 第二步：单报告分析 `analyze`

这一阶段是在“原始提取结果”的基础上分析每份报告。

主要做这些事情：
- 识别每个子标题
- 提取该子标题下的正文
- 判断问题类型
- 打风险标签
- 生成候选审计规则
- 如果配置了模型，则调用模型做增强分析
- 完整保留 `reasoning_details`、`reasoning_text`、`raw_events`

当前重点覆盖的问题领域包括：
- 资产资源管理
- 合同租赁管理
- 财务报销管理
- 补贴补偿管理
- 劳务用工管理
- 采购管理
- 工程项目管理
- 内控执行
- 会计处理

### 第三步：汇总分析 `aggregate`

这一阶段把所有单报告分析结果合并，生成总分析报告。

目前会输出：
- 汇总长文分析报告（以模型文字分析为主）
- 核心频次统计（问题类型、子标题、规则候选）
- 汇总分析结构化结果（供后续程序复用）

## 五、产出物说明

这是最重要的一节。

### 1. `outputs/extracted_md/`

这一层是“原始提取结果”。

常见文件：
- `xxx.md`
- `xxx.json`

其中：

`xxx.md`
- 这是从原报告里截出来的“审计发现的主要问题”整章原文
- 基本不改写，只是转成 Markdown 便于查看
- 适合人工快速复核

`xxx.json`
- 这是同一份提取结果的结构化版本
- 包含：
  - 来源文件名
  - 起始锚点
  - 结束锚点
  - 原始 Markdown
  - 每一个段落/标题 block
  - 编号信息
  - 层级提示
- 适合程序继续分析

如果你想确认“原始提取是否正确”，优先看这一层。

### 2. `outputs/per_report_analysis/`

这一层是“单份报告分析结果”。

常见文件：
- `xxx.md`
- `xxx.json`

其中：

`xxx.md`
- 适合人工阅读
- 现在以“分析主文”为主，而不是简单字段堆叠
- 一般包含：
  - 总体判断
  - 按问题类型的分析
  - 规则沉淀建议
  - 附录问题清单摘要
- 适合直接作为人工阅读版本

`xxx.json`
- 适合后续继续建模、统计和规则沉淀
- 每个子标题通常包含：
  - `title_raw`
  - `title_normalized`
  - `content_raw`
  - `problem_type`
  - `risk_tags`
  - `keywords`
  - `evidence_sentences`
  - `rule_candidates`
  - `confidence`
  - `llm`

如果这一份报告启用了模型分析，则 `llm` 里会有：
- `response_text`
- `reasoning_text`
- `reasoning_details`
- `raw_events`
- `usage`

如果你想看“某一份报告被分析成了什么样”，优先看这一层。

### 3. `outputs/aggregate/`

这一层是“所有报告汇总后的总结果”。

重点文件如下。

`aggregate_report.md`
- 总分析报告（新版建议以“模型长文分析”为主体）
- 适合直接打开阅读
- 重点看问题结构、根因、规则沉淀和实施建议

`aggregate_narrative.json`
- 汇总长文分析的模型原始输出
- 包含：
  - `response_text`
  - `reasoning_text`
  - `reasoning_details`
  - `raw_events`
  - `usage`
- 适合做模型审计、复盘和追溯

`aggregate_report.json`
- 总分析报告的结构化数据版本
- 适合后续系统继续消费

`subsection_frequency.csv`
- 各类子标题出现了多少次

`problem_type_frequency.csv`
- 各类问题类型出现了多少次

`rule_candidate_frequency.csv`
- 候选规则出现次数统计

`aggregate_narrative.json`（新增）
- 汇总长文分析的模型原始输出与追溯信息
- 可包含 `response_text`、`reasoning_text`、`reasoning_details`、`raw_events`

如果你想看“所有报告的总体规律和审计建议”，优先看这一层。

### 4. `outputs/cache/`

这一层是中间缓存，不一定是最终要看的。

常见内容：
- `.doc` 转换出来的 `.docx`
- 临时中间文件

主要作用：
- 提高重复运行速度
- 便于排查文档转换问题

### 5. `outputs/cache/llm_raw/`

这一层非常重要，保存大模型原始输出。

每个 `json` 文件里会包含：
- 每个子标题的模型响应正文
- `reasoning_text`
- `reasoning_details`
- `raw_events`
- `usage`

这一层适合做：
- 模型效果审计
- 规则提炼复盘
- 提示词调整
- 问题追踪

如果你想确认“模型到底是怎么回答的、思考内容有没有保留”，就看这里。

## 六、推荐你怎么看结果

### 场景 1：先检查提取对不对

先看：
- `outputs/extracted_md/*.md`

如果发现章节截错了，再去看：
- `outputs/extracted_md/*.json`

### 场景 2：想看某一份报告分析得怎么样

先看：
- `outputs/per_report_analysis/*.md`

如果要做程序处理或想看更细字段，再看：
- `outputs/per_report_analysis/*.json`

### 场景 3：想看总体统计

先看：
- `outputs/aggregate/aggregate_report.md`

然后按需要看：
- `outputs/aggregate/subsection_frequency.csv`
- `outputs/aggregate/problem_type_frequency.csv`
- `outputs/aggregate/rule_candidate_frequency.csv`
- `outputs/aggregate/aggregate_narrative.json`

### 场景 4：想核对模型原始输出

看：
- `outputs/cache/llm_raw/*.json`

## 七、当前样例已经生成了什么

当前样例已经实际生成了以下文件：

- `outputs/extracted_md/14_江阴市利港街兴利社区股份经济合作社_11_11.md`
- `outputs/extracted_md/14_江阴市利港街兴利社区股份经济合作社_11_11.json`
- `outputs/per_report_analysis/14_江阴市利港街兴利社区股份经济合作社_11_11.md`
- `outputs/per_report_analysis/14_江阴市利港街兴利社区股份经济合作社_11_11.json`
- `outputs/aggregate/aggregate_report.md`
- `outputs/aggregate/aggregate_report.json`
- `outputs/aggregate/aggregate_narrative.json`（启用汇总长文分析时）
- `outputs/cache/llm_raw/14_江阴市利港街兴利社区股份经济合作社_11_11.json`

所以你现在如果要看结果，建议顺序是：

1. 先打开 `outputs/extracted_md/14_江阴市利港街兴利社区股份经济合作社_11_11.md`
   看原始提取是否准确。
2. 再打开 `outputs/per_report_analysis/14_江阴市利港街兴利社区股份经济合作社_11_11.md`
   看单报告分析是否符合预期。
3. 最后打开 `outputs/aggregate/aggregate_report.md`
   看汇总长文分析与实施建议。

## 八、常见问题

### 1. 为什么跑得比较慢

如果启用了模型分析，程序会按子标题逐条调用模型。
一份报告如果子标题很多，耗时会明显增加。

### 2. 为什么有时候会出现 429

这是模型服务端限流。
当前程序已经带自动重试，但批量跑很多报告时仍建议控制并发和批次大小。

### 3. 为什么 `outputs/cache/` 里有一些临时文件

这是文档转换过程中产生的中间文件。
它们主要用于缓存和排错，不是最终成果。

### 4. 如果后续放很多报告，应该怎么做

直接把文件放到：

- `input_reports/batch/`

然后执行：

```powershell
uv run audit-pipeline run-all --input-dir input_reports/batch
```

## 九、当前结论

如果你只记住一句话：

- `extracted_md` 看原文提取
- `per_report_analysis` 看单报告分析
- `aggregate` 看总报告和各种统计表
- `cache/llm_raw` 看模型原始输出和思考内容
