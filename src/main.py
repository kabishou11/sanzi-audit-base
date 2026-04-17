from __future__ import annotations

import argparse
import csv
import json
import logging
import re
from pathlib import Path
from typing import Callable, Any

from src.config import AppConfig, load_config
from src.llm.client import LLMResponse, OpenAICompatibleClient
from src.llm.prompts import build_aggregate_narrative_messages
from src.logging_setup import configure_logging
from src.pipeline.aggregate import aggregate_report_analyses
from src.pipeline.analyze import analyze_report
from src.pipeline.documents import scan_report_files
from src.pipeline.extract import SectionNotFoundError, extract_section_from_file

logger = logging.getLogger(__name__)


def _run_extract(config: AppConfig) -> int:
    logger.info("Step extract started")
    logger.info("Input directory: %s", config.input_dir)
    files = scan_report_files(config.input_dir)
    if not files:
        logger.warning("No .doc/.docx files found in %s", config.input_dir)
        return 0

    success_count = 0
    for path in files:
        try:
            result = extract_section_from_file(path, config.cache_dir, converter=config.doc_converter)
        except SectionNotFoundError as exc:
            logger.error("Target section not found in %s: %s", path.name, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            logger.exception("Extraction failed for %s: %s", path.name, exc)
            continue

        payload = {
            "report_id": result.report_id,
            "source_file": result.source_path.name,
            "source_path": str(result.source_path),
            "paragraph_count": result.paragraph_count,
            "conversion": result.conversion.to_dict(),
            "section_title": result.section.section_title,
            "start_anchor": result.section.start_anchor.to_dict(),
            "end_anchor": result.section.end_anchor.to_dict() if result.section.end_anchor else None,
            "raw_markdown": result.section.raw_markdown,
            "blocks": [block.to_dict() for block in result.section.blocks],
            "extraction_method": "rule_based",
        }
        _write_text(config.extracted_dir / f"{result.report_id}.md", f"{result.section.raw_markdown}\n")
        _write_json(config.extracted_dir / f"{result.report_id}.json", payload)
        success_count += 1
        logger.info("Extracted %s", path.name)

    logger.info("Extraction finished: %s/%s reports succeeded", success_count, len(files))
    return 0


def _run_analyze(config: AppConfig) -> int:
    logger.info("Step analyze started")
    extracted_files = sorted(config.extracted_dir.glob("*.json"))
    if not extracted_files:
        logger.warning("No extracted JSON files found in %s", config.extracted_dir)
        return 0

    llm_client = _build_llm_client(config)
    for path in extracted_files:
        extracted_report = _read_json(path)
        analysis = analyze_report(
            extracted_report,
            llm_client=llm_client,
            model=config.openai_model if llm_client else None,
            reasoning_split=config.openai_reasoning_split,
        )
        report_id = analysis["report_id"]
        _persist_llm_raw_outputs(config, report_id, analysis)
        _write_json(config.per_report_dir / f"{report_id}.json", analysis)
        _write_text(config.per_report_dir / f"{report_id}.md", _build_report_analysis_markdown(analysis))
        logger.info("Analyzed %s", report_id)
    return 0


def _run_aggregate(config: AppConfig) -> int:
    logger.info("Step aggregate started")
    analysis_files = sorted(config.per_report_dir.glob("*.json"))
    if not analysis_files:
        logger.warning("No per-report analysis JSON files found in %s", config.per_report_dir)
        return 0

    analyses = [_read_json(path) for path in analysis_files]
    aggregate = aggregate_report_analyses(analyses)
    narrative = _build_aggregate_narrative(config, analyses, aggregate)
    aggregate["narrative_report"] = narrative["response_text"]
    _cleanup_aggregate_exports(config.aggregate_dir, keep_csv_names=set(aggregate.get("csv_exports", {}).keys()))
    _write_json(config.aggregate_dir / "aggregate_report.json", aggregate)
    _write_text(config.aggregate_dir / "aggregate_report.md", narrative["response_text"])
    _write_json(config.aggregate_dir / "aggregate_narrative.json", narrative)
    for name, rows in aggregate.get("csv_exports", {}).items():
        _write_csv(config.aggregate_dir / f"{name}.csv", rows)
    logger.info("Aggregate analysis written to %s", config.aggregate_dir)
    return 0


def _run_all(config: AppConfig) -> int:
    for step in (_run_extract, _run_analyze, _run_aggregate):
        exit_code = step(config)
        if exit_code != 0:
            return exit_code
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="audit-pipeline",
        description="Sanzi audit report extraction and analysis pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("extract", "Run extraction stage."),
        ("analyze", "Run per-report analysis stage."),
        ("aggregate", "Run aggregate analysis stage."),
        ("run-all", "Run extract, analyze, aggregate sequentially."),
    ):
        subparser = subparsers.add_parser(name, help=help_text)
        subparser.add_argument("--input-dir", default=None, help="Override input directory for this run.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    config = load_config()
    if args.input_dir:
        config.input_dir = Path(args.input_dir).resolve()
    configure_logging(config.log_level)
    config.ensure_output_dirs()

    logger.info("Configuration loaded from .env/environment")
    logger.info("OPENAI model: %s", config.openai_model)
    logger.info("Reasoning split enabled: %s", config.openai_reasoning_split)
    logger.info("Prompt version: %s", config.openai_prompt_version)

    handlers: dict[str, Callable[[AppConfig], int]] = {
        "extract": _run_extract,
        "analyze": _run_analyze,
        "aggregate": _run_aggregate,
        "run-all": _run_all,
    }
    return handlers[args.command](config)


def _build_llm_client(config: AppConfig) -> OpenAICompatibleClient | None:
    if not config.openai_api_key:
        logger.warning("OPENAI_API_KEY is empty; analyze stage will use rule-based analysis only.")
        return None
    return OpenAICompatibleClient(
        api_key=config.openai_api_key,
        base_url=config.openai_base_url,
        default_model=config.openai_model,
        timeout=config.openai_timeout,
        max_retries=config.openai_max_retries,
    )


def _build_aggregate_narrative(
    config: AppConfig,
    analyses: list[dict[str, Any]],
    aggregate: dict[str, Any],
) -> dict[str, Any]:
    llm_client = _build_llm_client(config)
    fallback_text = aggregate.get("markdown_report", "# 审计问题汇总分析报告\n\n暂无内容。\n")
    if not config.aggregate_narrative_enabled:
        return {
            "model": None,
            "response_text": fallback_text,
            "reasoning_text": "",
            "reasoning_details": [],
            "raw_events": [],
            "usage": {},
            "source": "fallback_disabled",
        }
    if llm_client is None:
        return {
            "model": None,
            "response_text": fallback_text,
            "reasoning_text": "",
            "reasoning_details": [],
            "raw_events": [],
            "usage": {},
            "source": "fallback_no_llm",
        }

    messages = build_aggregate_narrative_messages(
        aggregate_result=aggregate,
        report_analyses=analyses,
        prompt_version=config.openai_prompt_version,
        max_sample_subsections=config.aggregate_narrative_max_subsections,
        target_length_hint=config.aggregate_narrative_target_length,
    )
    llm_result = llm_client.stream_chat(
        messages=messages,
        model=config.openai_model,
        reasoning_split=config.openai_reasoning_split,
        stream=True,
    )
    response_text = llm_result.response_text.strip() or fallback_text
    return {
        "model": llm_result.model,
        "response_text": response_text,
        "reasoning_text": llm_result.reasoning_text,
        "reasoning_details": llm_result.reasoning_details,
        "raw_events": llm_result.raw_events,
        "usage": llm_result.usage,
        "source": "llm",
    }


def _persist_llm_raw_outputs(config: AppConfig, report_id: str, analysis: dict[str, Any]) -> None:
    raw_payload: list[dict[str, Any]] = []
    for subsection in analysis.get("subsections", []):
        llm_payload = subsection.get("llm")
        if not llm_payload:
            continue
        raw_payload.append(
            {
                "report_id": report_id,
                "subsection_id": subsection.get("subsection_id"),
                "title_raw": subsection.get("title_raw"),
                "model": llm_payload.get("model"),
                "response_text": llm_payload.get("response_text"),
                "reasoning_text": llm_payload.get("reasoning_text"),
                "reasoning_details": llm_payload.get("reasoning_details"),
                "raw_events": llm_payload.get("raw_events"),
                "usage": llm_payload.get("usage"),
            }
        )
    if raw_payload:
        _write_json(config.llm_raw_dir / f"{report_id}.json", {"items": raw_payload})


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = {
                key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                for key, value in row.items()
            }
            writer.writerow(normalized)


def _cleanup_aggregate_exports(aggregate_dir: Path, keep_csv_names: set[str]) -> None:
    for path in aggregate_dir.glob("*.csv"):
        if path.stem not in keep_csv_names:
            path.unlink(missing_ok=True)


def _build_report_analysis_markdown(analysis: dict[str, Any]) -> str:
    subsections = analysis.get("subsections", [])
    summary = analysis.get("report_level_summary", {})
    problem_type_distribution = summary.get("problem_type_distribution", {})
    sorted_problem_types = sorted(
        problem_type_distribution.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    top_problem_text = "、".join(f"{name}（{count}项）" for name, count in sorted_problem_types[:4]) or "暂无明显集中类型"
    title_samples = "、".join(
        subsection.get("title_normalized", subsection.get("title_raw", ""))
        for subsection in subsections[:6]
        if subsection.get("title_normalized") or subsection.get("title_raw")
    ) or "暂无"

    grouped_subsections: dict[str, list[dict[str, Any]]] = {}
    for subsection in subsections:
        grouped_subsections.setdefault(subsection.get("problem_type", "其他"), []).append(subsection)

    lines = [
        "# 单报告分析报告",
        "",
        f"**来源文件**：{analysis.get('source_file', '')}",
        f"**章节范围**：{analysis.get('section_title', '')}",
        f"**识别问题数**：{summary.get('subsection_count', 0)}",
        "",
        "## 一、总体判断",
        "",
        f"本报告围绕“{analysis.get('section_title', '')}”章节展开分析，共识别出 {summary.get('subsection_count', 0)} 个问题子标题，"
        f"主要集中在 {top_problem_text} 等领域。整体上看，该报告反映出的风险并非单点失误，而是集中体现为制度执行不严、"
        "合同约束弱化、附件与依据支撑不足、以及采购与支付环节的内控刚性不够。",
        "",
        f"从问题标题分布看，高频或代表性的关注点包括：{title_samples}。这些问题在表现形式上各不相同，但底层逻辑较为一致，"
        "即多数问题都发生在“规则已经存在、但执行和复核没有真正落地”的环节。",
        "",
        "## 二、按问题类型的分析",
    ]

    for problem_type, items in sorted(grouped_subsections.items(), key=lambda item: len(item[1]), reverse=True):
        representative_titles = "、".join(
            item.get("title_normalized", item.get("title_raw", ""))
            for item in items[:4]
            if item.get("title_normalized") or item.get("title_raw")
        ) or "暂无"
        representative_evidence = []
        for item in items:
            evidence = item.get("evidence_sentences", [])
            if evidence:
                representative_evidence.append(_clean_report_sentence(str(evidence[0])))
            if len(representative_evidence) >= 2:
                break
        evidence_text = "；".join(text for text in representative_evidence if text) or "该类型问题在原文中有多处体现。"

        lines.extend(
            [
                f"### {problem_type}",
                "",
                f"该类型共识别 {len(items)} 项，代表性问题包括：{representative_titles}。",
                "",
                f"从文本证据看，{evidence_text}",
                "",
                "这些问题说明该领域不仅存在单项操作偏差，更反映出相应流程缺少稳定的审批、复核、台账或验收约束。"
                "若后续做规则沉淀，应优先把这一类型中反复出现的判定信号固化下来，用于批量识别相似问题。",
                "",
            ]
        )

    lines.extend(
        [
            "## 三、规则沉淀建议",
            "",
            "从本报告提取结果来看，适合优先沉淀为规则的方向主要包括：",
            "",
        ]
    )

    seen_rules: set[str] = set()
    rule_lines: list[str] = []
    for subsection in subsections:
        for rule in subsection.get("rule_candidates", []):
            rule_name = str(rule.get("rule_name", "")).strip()
            if not rule_name or rule_name in seen_rules:
                continue
            seen_rules.add(rule_name)
            rule_logic = _clean_report_sentence(str(rule.get("rule_logic", "")))
            trigger_keywords = ", ".join(str(item) for item in rule.get("trigger_keywords", [])[:5])
            rule_lines.append(f"- `{rule_name}`：{rule_logic} 触发词示例：{trigger_keywords}")
    lines.extend(rule_lines[:10] or ["- 暂无可展示的规则候选。"])

    lines.extend(
        [
            "",
            "## 四、附录：问题清单摘要",
            "",
            "| 子标题 | 问题类型 | 风险标签 | 候选规则数 |",
            "| --- | --- | --- | --- |",
        ]
    )
    for subsection in subsections:
        title = subsection.get("title_normalized", subsection.get("title_raw", "未命名子标题"))
        problem_type = subsection.get("problem_type", "其他")
        risk_tags = "、".join(subsection.get("risk_tags", [])[:3]) or "-"
        rule_count = len(subsection.get("rule_candidates", []))
        lines.append(f"| {title} | {problem_type} | {risk_tags} | {rule_count} |")

    return "\n".join(lines).strip() + "\n"


def _clean_report_sentence(text: str) -> str:
    cleaned = re.sub(r"\d{1,3}(,\d{3})*(\.\d+)?(元|万元|亿元|亩|棵|人|月|年|天|次)?", "相关金额或数量", text)
    cleaned = re.sub(r"\d{4}[./年-]\d{1,2}([./月-]\d{1,2})?[日]?", "相关时间", cleaned)
    cleaned = re.sub(r"(相关金额或数量)+", "相关金额或数量", cleaned)
    cleaned = re.sub(r"(相关时间)+", "相关时间", cleaned)
    cleaned = cleaned.replace("相关金额或数量相关金额或数量", "相关金额或数量")
    cleaned = re.sub(r"\s+", "", cleaned).strip("；。")
    return cleaned[:180] + ("…" if len(cleaned) > 180 else "")


if __name__ == "__main__":
    raise SystemExit(main())
