from __future__ import annotations

import json
from typing import Any

SYSTEM_PROMPT = (
    "你是一名审计分析助手。你必须基于提供的原始文本进行结构化分析，不得改写原文事实，"
    "不得虚构法规、金额、主体或时间。你关注三资专项审计中常见的资产资源管理、合同租赁、"
    "财务报销、工程采购、补贴补偿、劳务用工、内控执行等问题。输出必须是 JSON。"
)

AGGREGATE_NARRATIVE_SYSTEM_PROMPT = (
    "你是一名高级审计研究员，负责撰写三资专项审计的汇总分析报告。"
    "你必须基于输入材料进行归纳，不得虚构事实、法规、主体、金额或时间。"
    "你输出的是面向管理者和审计人员的中文长文分析，不是统计表罗列。"
)


def build_subsection_analysis_messages(
    *,
    report_meta: dict[str, Any],
    subsection: dict[str, Any],
) -> list[dict[str, str]]:
    schema = {
        "problem_type": "string",
        "risk_tags": ["string"],
        "keywords": ["string"],
        "evidence_sentences": ["string"],
        "title_aliases": ["string"],
        "rule_candidates": [
            {
                "rule_name": "string",
                "rule_logic": "string",
                "trigger_keywords": ["string"],
                "example_evidence": "string",
            }
        ],
        "confidence": 0.0,
    }

    user_payload = {
        "task": "分析单个审计子标题内容并提炼可复用规则",
        "constraints": [
            "只能依据给定文本",
            "evidence_sentences 必须是原文摘录",
            "problem_type 优先从 资产资源管理、合同租赁管理、财务报销管理、补贴补偿管理、劳务用工管理、采购管理、工程项目管理、内控执行、会计处理、其他 中选择",
            "risk_tags 优先提取 审批缺失、依据不足、合同不规范、未按约执行、重复支付、台账缺失、标准不一致、超范围支付、验收缺失、分拆采购、收入确认不当 等标签",
            "rule_candidates 要写成可复用的审计判定规则，不要写空泛总结",
            "若信息不足，返回空数组或低置信度",
            "仅输出 JSON，不要输出其他说明",
        ],
        "report_meta": report_meta,
        "subsection": subsection,
        "output_schema": schema,
    }

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def build_aggregate_narrative_messages(
    *,
    aggregate_result: dict[str, Any],
    report_analyses: list[dict[str, Any]],
    prompt_version: str = "v1",
    max_sample_subsections: int = 120,
    target_length_hint: int = 4500,
) -> list[dict[str, str]]:
    """
    Build messages for long-form aggregate narrative generation.

    The model is asked to produce a substantial Chinese report-style narrative
    with limited table/statistics emphasis.
    """

    samples = _collect_subsection_samples(report_analyses, max_samples=max_sample_subsections)
    compact_aggregate = {
        "report_count": aggregate_result.get("report_count", 0),
        "subsection_count": aggregate_result.get("subsection_count", 0),
        "problem_type_frequency": aggregate_result.get("problem_type_frequency", []),
        "subsection_frequency": aggregate_result.get("subsection_frequency", []),
        "rule_candidate_frequency": aggregate_result.get("rule_candidate_frequency", []),
    }
    user_payload = {
        "task": "生成三资专项审计汇总长文分析报告",
        "prompt_version": prompt_version,
        "target_audience": ["审计管理人员", "项目负责人", "规则建模人员"],
        "writing_goals": [
            "以分析性叙述为主，避免大段统计表罗列",
            "解释共性问题、差异模式、高风险场景、制度根因",
            "给出可落地的规则沉淀建议和模型结合建议",
            "提供后续批量审计可执行的优化路径",
        ],
        "constraints": [
            "不得虚构事实，不得引入输入中不存在的金额、主体、时间和法规",
            "尽量避免罗列大量具体金额数字，可用“多起”“高频”“反复出现”等表达",
            "除必要摘要外，不要输出表格",
            "输出为中文 Markdown 长文，建议长度不少于 target_length_hint 字符",
        ],
        "section_outline": [
            "一、总体判断与关键结论",
            "二、问题结构画像（按问题类型展开）",
            "三、高风险模式与触发路径",
            "四、跨报告共性根因分析",
            "五、可沉淀规则库设计建议",
            "六、大模型与规则协同落地建议",
            "七、优先级路线图（短期/中期）",
        ],
        "target_length_hint": target_length_hint,
        "aggregate_summary": compact_aggregate,
        "sample_subsections": samples,
    }
    return [
        {"role": "system", "content": AGGREGATE_NARRATIVE_SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _collect_subsection_samples(
    report_analyses: list[dict[str, Any]],
    *,
    max_samples: int,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for report in report_analyses:
        report_id = report.get("report_id", "")
        source_file = report.get("source_file", "")
        for subsection in report.get("subsections", []):
            if len(samples) >= max_samples:
                return samples
            samples.append(
                {
                    "report_id": report_id,
                    "source_file": source_file,
                    "title_raw": subsection.get("title_raw", ""),
                    "title_normalized": subsection.get("title_normalized", ""),
                    "problem_type": subsection.get("problem_type", "其他"),
                    "risk_tags": subsection.get("risk_tags", []),
                    "rule_candidates": subsection.get("rule_candidates", []),
                    "evidence_sentences": subsection.get("evidence_sentences", [])[:3],
                    "content_excerpt": _trim_text(str(subsection.get("content_raw", "")), limit=500),
                }
            )
    return samples


def _trim_text(text: str, *, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."
