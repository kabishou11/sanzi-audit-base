from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any

from src.config import load_config
from src.llm.client import OpenAICompatibleClient


LOGGER = logging.getLogger("extract_financial_elements_with_llm")
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = ROOT / "outputs" / "financial_management_items.csv"
DEFAULT_OUTPUT_CSV = ROOT / "outputs" / "financial_management_items_with_elements.csv"
DEFAULT_REASONING_JSONL = ROOT / "outputs" / "logs" / "financial_management_items_reasoning.jsonl"
OUTPUT_COLUMN = "extracted_elements_rules"

SYSTEM_PROMPT = """你是审计问题原文的规则化提取助手。

任务目标：
对输入的审计原文进行“要素/规则提取”。

硬性要求：
1. 必须最大化提取，不要漏掉任何可提取的要素、条件、动作、对象、流程、依据缺失点、单据缺失点、审批缺失点、时间信息、频次信息、责任主体、金额相关约束表述、对比关系、异常表现。
2. 只能基于原文提取，不能补充原文没有的信息，不能猜测，不能改写原文含义。
3. 不要做总结，不要归纳，不要合并不同意思的句子，不要下结论，不要评价严重程度。
4. 尽量保留原文中的关键措辞、限定词、条件词、否定词、数字表达、时间表达。
5. 如果原文包含多条并列问题、多个例子、多个凭证缺失点，必须全部拆开列出。
6. 输出必须是纯文本，不要使用 JSON，不要使用 Markdown 表格。
7. 每一条提取结果单独一行，建议使用“1. 2. 3.”编号。
8. 每一条都应尽量贴近原文，做“要素/规则层面的拆解”，不是摘要。
9. 若原文为空或没有可提取内容，输出“未提取到有效要素”。

提取侧重点：
- 事项/行为
- 主体/对象
- 时间
- 金额或数量表达
- 制度依据、标准、决议、合同、审批、签字、验收、台账、附件、送货单、签收记录、图片、考核记录等材料是否缺失
- 流程是否不合规
- 标准是否不明确
- 发放、支付、报销、外包、考核、救助、补助等动作及其约束
- 原文中出现的“无、未、缺少、不一致、不充分、流于形式、后补”等异常表述

输出风格：
- 只输出提取结果本身
- 不要写“以下是提取结果”
- 不要分章节
- 不要漏项
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Use MiniMax/OpenAI-compatible model to extract element/rule text from item_content."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=DEFAULT_INPUT_CSV,
        help="Input CSV path containing item_content column.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Output CSV path with extracted text appended as a new column.",
    )
    parser.add_argument(
        "--reasoning-jsonl",
        type=Path,
        default=DEFAULT_REASONING_JSONL,
        help="JSONL file storing reasoning/raw response for each processed row.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N rows for sampling/debugging.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they already exist.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def resolve_output_path(path: Path, overwrite: bool) -> Path:
    if overwrite or not path.exists():
        return path

    suffix = path.suffix
    stem = path.stem
    index = 2
    while True:
        candidate = path.with_name(f"{stem}_v{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def build_user_prompt(source_file_name: str, item_full_title: str, item_content: str) -> str:
    return f"""请对下面这条审计问题原文做“要素/规则提取”。

来源文件：{source_file_name}
问题标题：{item_full_title}

原文如下：
{item_content}

请严格按照系统要求输出，最大化提取，不要遗漏，不要改变原文意思，不要做总结整合。"""


def load_rows(path: Path, limit: int | None) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                rows = [dict(row) for row in reader]
            LOGGER.info("Loaded CSV with encoding: %s", encoding)
            if limit is not None:
                return rows[:limit]
            return rows
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    raise UnicodeDecodeError(
        "csv",
        b"",
        0,
        1,
        f"Unable to decode CSV file {path}. Last error: {last_error}",
    )


def append_reasoning_log(
    handle: Any,
    *,
    row_index: int,
    row: dict[str, str],
    extracted_text: str,
    llm_result: Any,
) -> None:
    payload = {
        "row_index": row_index,
        "source_file_name": row.get("source_file_name", ""),
        "item_full_title": row.get("item_full_title", ""),
        "item_content": row.get("item_content", ""),
        "extracted_elements_rules": extracted_text,
        "model": llm_result.model,
        "response_text": llm_result.response_text,
        "reasoning_text": llm_result.reasoning_text,
        "reasoning_details": llm_result.reasoning_details,
        "raw_events": llm_result.raw_events,
        "usage": llm_result.usage,
    }
    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()
    config = load_config()
    configure_logging(config.log_level)

    input_csv = args.input_csv.resolve()
    output_csv = resolve_output_path(args.output_csv.resolve(), args.overwrite)
    reasoning_jsonl = resolve_output_path(args.reasoning_jsonl.resolve(), args.overwrite)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")
    if not config.openai_api_key:
        raise ValueError("OPENAI_API_KEY is required in .env")

    ensure_parent(output_csv)
    ensure_parent(reasoning_jsonl)

    rows = load_rows(input_csv, args.limit)
    LOGGER.info("Loaded %s rows from %s", len(rows), input_csv)
    LOGGER.info("Output CSV: %s", output_csv)
    LOGGER.info("Reasoning JSONL: %s", reasoning_jsonl)
    LOGGER.info("Model: %s", config.openai_model)
    LOGGER.info("Reasoning split enabled: %s", config.openai_reasoning_split)

    client = OpenAICompatibleClient(
        api_key=config.openai_api_key,
        base_url=config.openai_base_url,
        default_model=config.openai_model,
        timeout=config.openai_timeout,
        max_retries=config.openai_max_retries,
    )

    processed_rows: list[dict[str, str]] = []
    with reasoning_jsonl.open("w", encoding="utf-8") as reasoning_handle:
        for index, row in enumerate(rows, start=1):
            item_content = (row.get("item_content") or "").strip()
            if not item_content:
                extracted_text = "未提取到有效要素"
                output_row = dict(row)
                output_row[OUTPUT_COLUMN] = extracted_text
                processed_rows.append(output_row)
                append_reasoning_log(
                    reasoning_handle,
                    row_index=index,
                    row=row,
                    extracted_text=extracted_text,
                    llm_result=type(
                        "EmptyResult",
                        (),
                        {
                            "model": config.openai_model,
                            "response_text": extracted_text,
                            "reasoning_text": "",
                            "reasoning_details": [],
                            "raw_events": [],
                            "usage": {},
                        },
                    )(),
                )
                continue

            LOGGER.info(
                "Processing row %s/%s | %s | %s",
                index,
                len(rows),
                row.get("source_file_name", ""),
                row.get("item_full_title", ""),
            )
            llm_result = client.stream_chat(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": build_user_prompt(
                            row.get("source_file_name", ""),
                            row.get("item_full_title", ""),
                            item_content,
                        ),
                    },
                ],
                reasoning_split=config.openai_reasoning_split,
                extra_body={"reasoning_split": config.openai_reasoning_split},
                stream=True,
            )
            extracted_text = (llm_result.response_text or "").strip() or "未提取到有效要素"

            output_row = dict(row)
            output_row[OUTPUT_COLUMN] = extracted_text
            processed_rows.append(output_row)
            append_reasoning_log(
                reasoning_handle,
                row_index=index,
                row=row,
                extracted_text=extracted_text,
                llm_result=llm_result,
            )

    fieldnames = list(processed_rows[0].keys()) if processed_rows else [OUTPUT_COLUMN]
    with output_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_rows)

    LOGGER.info("Wrote %s processed rows to %s", len(processed_rows), output_csv)


if __name__ == "__main__":
    main()
