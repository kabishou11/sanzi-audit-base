from __future__ import annotations

import re
from collections import Counter
from typing import Any

from src.pipeline.analyze import normalize_title

_RE_AMOUNT = re.compile(r"^\d{1,3}(,\d{3})*(\.\d+)?(元|万元|亿元|%)?(/(亩|人|棵|年|月|天|次|项|户))?$")
_RE_DATE = re.compile(
    r"(^\d{4}([./-]\d{1,2}){1,2}$|^\d{4}年\d{1,2}月(\d{1,2}日)?$|^\d{1,2}月\d{1,2}日$)"
)
_RE_NUM_FRAG = re.compile(r"^\d+([.,/:-]\d+)+$")


def _contains_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def _is_noise_keyword(token: str) -> bool:
    cleaned = token.strip()
    if not cleaned or len(cleaned) <= 1:
        return True
    if _RE_AMOUNT.match(cleaned):
        return True
    if _RE_DATE.match(cleaned):
        return True
    if _RE_NUM_FRAG.match(cleaned):
        return True
    if not _contains_chinese(cleaned) and any(ch.isdigit() for ch in cleaned):
        return True
    if re.fullmatch(r"[0-9,./%\-*()（）]+", cleaned):
        return True
    return False


def _clean_keywords(keywords: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in keywords:
        token = str(raw).strip()
        if _is_noise_keyword(token):
            continue
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _iter_subsections(report_analyses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for report in report_analyses:
        report_id = report.get("report_id", "")
        source_file = report.get("source_file", "")
        for subsection in report.get("subsections", []):
            rows.append(
                {
                    "report_id": report_id,
                    "source_file": source_file,
                    "title_raw": subsection.get("title_raw", ""),
                    "title_normalized": normalize_title(subsection.get("title_normalized") or subsection.get("title_raw", "")),
                    "problem_type": subsection.get("problem_type", "其他"),
                    "keywords": _clean_keywords(subsection.get("keywords", [])),
                    "rule_candidates": subsection.get("rule_candidates", []),
                    "confidence": subsection.get("confidence", 0.0),
                }
            )
    return rows


def _sorted_counter(counter: Counter, key_name: str) -> list[dict[str, Any]]:
    return [{key_name: key, "count": value} for key, value in counter.most_common()]


def build_rule_frequency(subsection_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter()
    for row in subsection_rows:
        for rule in row.get("rule_candidates", []):
            name = rule.get("rule_name", "").strip()
            if name:
                counter[name] += 1
    return _sorted_counter(counter, "rule_name")


def aggregate_report_analyses(report_analyses: list[dict[str, Any]]) -> dict[str, Any]:
    subsection_rows = _iter_subsections(report_analyses)
    problem_type_counter = Counter()
    subsection_counter = Counter()
    clean_keyword_counter = Counter()

    for row in subsection_rows:
        problem_type_counter[row["problem_type"] or "其他"] += 1
        subsection_counter[row["title_normalized"] or "未命名子标题"] += 1
        for keyword in row["keywords"]:
            clean_keyword_counter[keyword] += 1

    rule_frequency = build_rule_frequency(subsection_rows)

    summary = {
        "report_count": len(report_analyses),
        "subsection_count": len(subsection_rows),
        "subsection_frequency": _sorted_counter(subsection_counter, "title_normalized"),
        "problem_type_frequency": _sorted_counter(problem_type_counter, "problem_type"),
        "rule_candidate_frequency": rule_frequency,
        "narrative_materials": {
            "top_problem_types": _sorted_counter(problem_type_counter, "problem_type")[:10],
            "top_subsections": _sorted_counter(subsection_counter, "title_normalized")[:20],
            "top_rules": rule_frequency[:20],
            "top_clean_keywords": _sorted_counter(clean_keyword_counter, "keyword")[:40],
            "sample_titles": [row["title_raw"] for row in subsection_rows[:80] if row.get("title_raw")],
            "sample_problem_types": [row["problem_type"] for row in subsection_rows[:80] if row.get("problem_type")],
        },
    }
    summary["csv_exports"] = build_csv_exports(summary)
    summary["markdown_report"] = build_aggregate_markdown(summary)
    return summary


def build_csv_exports(aggregate_result: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return {
        "subsection_frequency": aggregate_result.get("subsection_frequency", []),
        "problem_type_frequency": aggregate_result.get("problem_type_frequency", []),
        "rule_candidate_frequency": aggregate_result.get("rule_candidate_frequency", []),
    }


def _md_table(rows: list[dict[str, Any]], columns: list[str], limit: int = 20) -> str:
    if not rows:
        return "_No data_\n"
    head = "| " + " | ".join(columns) + " |\n"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |\n"
    body_lines = []
    for row in rows[:limit]:
        values = []
        for col in columns:
            val = row.get(col, "")
            if isinstance(val, list):
                val = ", ".join(str(item) for item in val)
            values.append(str(val))
        body_lines.append("| " + " | ".join(values) + " |")
    return head + divider + "\n".join(body_lines) + "\n"


def build_aggregate_markdown(aggregate_result: dict[str, Any]) -> str:
    top_problem_types = aggregate_result.get("problem_type_frequency", [])[:3]
    top_subsections = aggregate_result.get("subsection_frequency", [])[:5]
    top_rules = aggregate_result.get("rule_candidate_frequency", [])[:5]

    problem_type_summary = "、".join(
        f"{item.get('problem_type', '其他')}({item.get('count', 0)})" for item in top_problem_types
    ) or "暂无"
    subsection_summary = "、".join(
        f"{item.get('title_normalized', '未命名子标题')}({item.get('count', 0)})" for item in top_subsections
    ) or "暂无"
    rule_summary = "、".join(
        f"{item.get('rule_name', '未命名规则')}({item.get('count', 0)})" for item in top_rules
    ) or "暂无"

    lines = [
        "# 审计问题汇总分析报告",
        "",
        f"- 报告总数: {aggregate_result.get('report_count', 0)}",
        f"- 子标题总数: {aggregate_result.get('subsection_count', 0)}",
        "",
        "## 汇总摘要（用于 Narrative 生成前置）",
        "",
        f"本批次问题主要集中在：{problem_type_summary}。",
        f"高频子标题包括：{subsection_summary}。",
        f"高频候选规则包括：{rule_summary}。",
        "该摘要用于后续模型长文分析的前置材料，核心统计见附录。",
        "",
        "## 附录：核心统计",
        "",
        "### 子标题频次 Top 20",
        _md_table(aggregate_result.get("subsection_frequency", []), ["title_normalized", "count"], limit=20),
        "### 问题类型频次",
        _md_table(aggregate_result.get("problem_type_frequency", []), ["problem_type", "count"], limit=20),
        "### 规则候选频次",
        _md_table(aggregate_result.get("rule_candidate_frequency", []), ["rule_name", "count"], limit=30),
    ]
    return "\n".join(lines).strip() + "\n"
