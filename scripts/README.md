# 脚本用法说明

这份说明聚焦最近常用的 4 个脚本，适合直接复制命令运行。

默认 Python 环境：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe'
```

## 1. 按二级标题拆分主要问题到多 Sheet Excel

脚本：
- `scripts/extract_major_issues_to_xlsx.py`

用途：
- 从 `outputs/extracted_md/` 里的 Markdown 中提取“三、审计发现的主要问题”
- 按二级标题自动创建不同 sheet
- 合并明显同类标题
- 对标题和正文做脱敏
- 保留 `source_file_name` 原始来源

默认运行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_major_issues_to_xlsx.py'
```

指定输入目录和输出文件：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_major_issues_to_xlsx.py' `
  --input-dir 'F:\3work\1审计处理汇总\outputs\extracted_md' `
  --output 'F:\3work\1审计处理汇总\outputs\major_issues_by_sheet_v7.xlsx'
```

输出：
- 一个 `.xlsx` 文件
- 每个二级标题一个 sheet
- 列为 `source_file_name`、`item_full_title`、`item_content`

## 2. 提取所有“财务管理方面”问题到 CSV

脚本：
- `scripts/extract_financial_management_to_csv.py`

用途：
- 从 `outputs/extracted_md/` 中提取所有“财务管理方面”的三级问题
- 汇总成单个 CSV

默认运行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_financial_management_to_csv.py'
```

指定输入目录和输出文件：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_financial_management_to_csv.py' `
  --input-dir 'F:\3work\1审计处理汇总\outputs\extracted_md' `
  --output-csv 'F:\3work\1审计处理汇总\outputs\financial_management_items_v2.csv'
```

输出：
- 一个 `.csv` 文件
- 列为 `source_file_name`、`item_full_title`、`item_content`

## 3. 用大模型提取“财务管理方面”的要素/规则

脚本：
- `scripts/extract_financial_elements_with_llm.py`

用途：
- 读取财务管理问题 CSV
- 对每行 `item_content` 做高覆盖率要素/规则提取
- 在结果中新增 `extracted_elements_rules`

前置条件：
- `.env` 中已配置 OpenAI 兼容模型参数

默认运行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_financial_elements_with_llm.py'
```

先抽样 20 行试跑：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_financial_elements_with_llm.py' `
  --limit 20
```

覆盖已有输出：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\extract_financial_elements_with_llm.py' `
  --overwrite
```

输出：
- 结果 CSV：新增 `extracted_elements_rules`
- 推理日志 JSONL：保存每行模型原始响应与推理细节

## 4. 用大模型给多 Sheet 主要问题做技术规则分类

脚本：
- `scripts/classify_technical_rules_from_xlsx.py`

用途：
- 读取 `major_issues_by_sheet_v7.xlsx` 这类多 sheet Excel
- 逐行结合 `item_full_title + item_content` 做技术规则分类
- 参考 `prompts/technical_rule_memory/` 里的记忆文档，而不是直接吃人工示例表
- 输出为多个 CSV，并支持断点续跑、失败重试、批处理和并发

默认运行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py'
```

推荐正式跑法：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --input-xlsx 'F:\3work\1审计处理汇总\input_reports\xlsx\major_issues_by_sheet_v7.xlsx' `
  --memory-dir 'F:\3work\1审计处理汇总\prompts\technical_rule_memory' `
  --max-workers 4 `
  --batch-size 6 `
  --max-retries 3
```

只跑某个 sheet：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --sheet '财务管理方面'
```

只跑某些行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --sheet '财务管理方面' `
  --rows '2,5,8'
```

只重跑失败项：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --only-failed
```

只重跑分类为空的行：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --rerun-empty
```

抽样试跑前 20 条：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' `
  'F:\3work\1审计处理汇总\scripts\classify_technical_rules_from_xlsx.py' `
  --limit 20
```

输出目录内容：
- 每个 sheet 一个 CSV
- `_progress.json`：断点进度
- `_llm_results.jsonl`：每行结果日志
- `_failed_rows.jsonl`：失败行日志

新增列：
- `技术规则分类`
- `分类置信度`
- `分类依据摘要`

## 推荐运行顺序

如果你是从提取到分类完整走一遍，推荐顺序：

1. `extract_major_issues_to_xlsx.py`
2. `extract_financial_management_to_csv.py`
3. `extract_financial_elements_with_llm.py`
4. `classify_technical_rules_from_xlsx.py`

## 查看脚本内帮助

每个脚本都支持：

```powershell
& 'F:\3work\1审计处理汇总\.venv\Scripts\python.exe' '脚本路径.py' --help
```
