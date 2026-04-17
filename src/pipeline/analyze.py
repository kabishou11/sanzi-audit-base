from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

from src.llm.client import OpenAICompatibleClient
from src.llm.prompts import build_subsection_analysis_messages

_TITLE_PATTERNS = [
    re.compile(r"^\s*[一二三四五六七八九十]{1,3}[、.．]\s*(.+)$"),
    re.compile(r"^\s*[（(][一二三四五六七八九十]{1,3}[)）]\s*(.+)$"),
    re.compile(r"^\s*\d{1,3}[、.．]\s*(.+)$"),
]

_SPLIT_PATTERN = re.compile(r"[，。；：、,\s]+")

_STOP_WORDS = {
    "相关",
    "问题",
    "情况",
    "进行",
    "存在",
    "部分",
    "以及",
    "未",
    "不",
    "和",
    "及",
    "的",
}

_PROBLEM_TYPE_RULES: dict[str, set[str]] = {
    "资产资源管理": {"资产", "资源", "台账", "折旧", "盘点", "固定资产", "设备", "租金", "土地"},
    "合同租赁管理": {"合同", "租赁", "租金", "签订", "条款", "履约", "约定"},
    "财务报销管理": {"报销", "财务", "会计", "账务", "票据", "凭证", "收支", "科目"},
    "补贴补偿管理": {"补贴", "补偿", "青苗", "一次性", "标准", "征地"},
    "劳务用工管理": {"用工", "工资", "考勤", "奖金", "绩效", "劳务", "派工", "勤杂"},
    "采购管理": {"采购", "招标", "比价", "供应商", "分拆", "拆分", "收发存"},
    "工程项目管理": {"工程", "项目", "结算", "验收", "施工", "预算", "审定", "评估"},
    "内控执行": {"审批", "授权", "流程", "制度", "复核", "监督", "会议纪要", "依据"},
    "会计处理": {"权责发生制", "确认收入", "会计科目", "核算", "入账"},
}

_RISK_TAG_RULES: dict[str, tuple[str, ...]] = {
    "审批缺失": ("审批", "审核", "流程不规范", "无审批"),
    "依据不足": ("无依据", "依据不充分", "无明细", "无协议", "无测量", "无清单"),
    "合同不规范": ("合同条款不规范", "合同签订", "合同不规范", "签订日早于"),
    "未按约执行": ("未按合同约定", "未按要求", "未按约定", "未按权责发生制"),
    "重复支付": ("重复支付", "重复结算", "重复采购"),
    "台账缺失": ("台账", "无收发存", "无入库单", "无领用记录"),
    "标准不一致": ("标准不一致", "金额不一致", "差异较大"),
    "验收缺失": ("无验收", "无验收记录", "无维修记录"),
    "分拆采购": ("拆分采购", "规避流程", "分拆"),
}


@dataclass(slots=True)
class Subsection:
    subsection_id: str
    title_raw: str
    title_normalized: str
    blocks: list[dict[str, Any]]
    content_raw: str


def normalize_title(title: str) -> str:
    text = (title or "").strip()
    for pattern in _TITLE_PATTERNS:
        matched = pattern.match(text)
        if matched:
            text = matched.group(1).strip()
            break
    text = re.sub(r"\s+", "", text)
    return text


def is_heading_like(block: dict[str, Any]) -> bool:
    block_type = str(block.get("type", "")).lower()
    text = str(block.get("text", "")).strip()
    if not text:
        return False
    if block_type == "heading":
        return True
    if block.get("numbering"):
        return True
    return any(pattern.match(text) for pattern in _TITLE_PATTERNS)


def split_subsections(blocks: list[dict[str, Any]]) -> list[Subsection]:
    subsections: list[Subsection] = []
    current_title = "未命名子标题"
    current_blocks: list[dict[str, Any]] = []
    sid = 1

    for block in blocks:
        text = str(block.get("text", "")).strip()
        if not text:
            continue
        if is_heading_like(block):
            if current_blocks:
                content_raw = "\n".join(str(item.get("text", "")).strip() for item in current_blocks if item.get("text"))
                subsections.append(
                    Subsection(
                        subsection_id=f"s{sid}",
                        title_raw=current_title,
                        title_normalized=normalize_title(current_title),
                        blocks=current_blocks,
                        content_raw=content_raw.strip(),
                    )
                )
                sid += 1
                current_blocks = []
            current_title = text
            continue
        current_blocks.append(block)

    if current_blocks:
        content_raw = "\n".join(str(item.get("text", "")).strip() for item in current_blocks if item.get("text"))
        subsections.append(
            Subsection(
                subsection_id=f"s{sid}",
                title_raw=current_title,
                title_normalized=normalize_title(current_title),
                blocks=current_blocks,
                content_raw=content_raw.strip(),
            )
        )

    if not subsections and blocks:
        content_raw = "\n".join(str(item.get("text", "")).strip() for item in blocks if item.get("text"))
        subsections.append(
            Subsection(
                subsection_id="s1",
                title_raw="未命名子标题",
                title_normalized="未命名子标题",
                blocks=blocks,
                content_raw=content_raw.strip(),
            )
        )
    return subsections


def _extract_keywords(title: str, content: str, limit: int = 12) -> list[str]:
    candidates = _SPLIT_PATTERN.split(f"{title} {content}")
    normalized: list[str] = []
    for token in candidates:
        token = token.strip()
        if not token:
            continue
        if len(token) < 2:
            continue
        if token in _STOP_WORDS:
            continue
        normalized.append(token)
    counter = Counter(normalized)
    return [item for item, _ in counter.most_common(limit)]


def _detect_problem_type(keywords: list[str], title: str) -> str:
    title_text = normalize_title(title)
    score = Counter()
    for problem_type, hints in _PROBLEM_TYPE_RULES.items():
        for token in keywords:
            if token in hints:
                score[problem_type] += 2
            elif any(h in token for h in hints):
                score[problem_type] += 1
        for hint in hints:
            if hint in title_text:
                score[problem_type] += 2
    if not score:
        return "其他"
    return score.most_common(1)[0][0]


def _pick_evidence_sentences(content: str, limit: int = 3) -> list[str]:
    sentences = [s.strip() for s in re.split(r"[。；\n]+", content) if s.strip()]
    return sentences[:limit]


def _detect_risk_tags(title: str, content: str, problem_type: str) -> list[str]:
    source = f"{title}\n{content}"
    tags = [tag for tag, hints in _RISK_TAG_RULES.items() if any(hint in source for hint in hints)]
    if not tags:
        tags = [problem_type]
    return tags


def _rule_candidates(problem_type: str, title: str, content: str, keywords: list[str], evidence_sentences: list[str]) -> list[dict[str, Any]]:
    if not keywords and not title:
        return []
    top_keywords = keywords[:4] if keywords else [normalize_title(title)]
    evidence = evidence_sentences[0] if evidence_sentences else ""
    normalized_title = normalize_title(title)
    if "租金" in content or "租赁" in title:
        rule_name = "租赁合同与租金执行检查"
        rule_logic = "若出现租赁合同、租金、欠缴、未按合同约定收取等表述，则判定为租赁合同执行异常。"
    elif "补贴" in title or "补偿" in title:
        rule_name = "补贴补偿依据与标准检查"
        rule_logic = "若补贴补偿缺少面积、单价、协议、测量或标准前后不一致，则判定为补贴补偿异常。"
    elif "工资" in content or "考勤" in content or "用工" in title:
        rule_name = "劳务用工支付与考核检查"
        rule_logic = "若工资、奖金、绩效、考勤、派工、考核记录缺失或与合同不一致，则判定为劳务用工管理异常。"
    elif "采购" in title or "供应商" in content:
        rule_name = "采购流程合规性检查"
        rule_logic = "若采购存在比价缺失、拆分采购、固定供应商或收发存记录缺失，则判定为采购管理异常。"
    elif "工程" in title or "结算" in content or "审定" in content:
        rule_name = "工程项目结算支付检查"
        rule_logic = "若工程项目出现评估、审定、结算、付款金额不一致或验收资料不足，则判定为工程项目异常。"
    else:
        rule_name = f"{problem_type}规则检查"
        rule_logic = f"若文本同时出现关键词 {', '.join(top_keywords)}，则判定为{problem_type}疑似问题。"
    return [
        {
            "rule_name": rule_name,
            "rule_logic": rule_logic,
            "trigger_keywords": top_keywords,
            "example_evidence": evidence,
            "source_title": normalized_title,
        }
    ]


def _safe_parse_json(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        return {}
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", stripped, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            return {}
    obj = re.search(r"(\{.*\})", stripped, re.DOTALL)
    if obj:
        try:
            return json.loads(obj.group(1))
        except json.JSONDecodeError:
            return {}
    return {}


def analyze_subsection_rule_based(subsection: Subsection) -> dict[str, Any]:
    keywords = _extract_keywords(subsection.title_normalized, subsection.content_raw)
    problem_type = _detect_problem_type(keywords, subsection.title_raw)
    evidence = _pick_evidence_sentences(subsection.content_raw)
    risk_tags = _detect_risk_tags(subsection.title_raw, subsection.content_raw, problem_type)
    rules = _rule_candidates(problem_type, subsection.title_raw, subsection.content_raw, keywords, evidence)
    return {
        "subsection_id": subsection.subsection_id,
        "title_raw": subsection.title_raw,
        "title_normalized": subsection.title_normalized,
        "content_raw": subsection.content_raw,
        "keywords": keywords,
        "problem_type": problem_type,
        "risk_tags": risk_tags,
        "evidence_sentences": evidence,
        "title_aliases": [subsection.title_normalized],
        "rule_candidates": rules,
        "confidence": 0.62,
        "analysis_source": "rule_based",
    }


def analyze_subsection_with_llm(
    *,
    subsection: Subsection,
    report_meta: dict[str, Any],
    llm_client: OpenAICompatibleClient,
    model: str | None = None,
    reasoning_split: bool = True,
) -> dict[str, Any]:
    base = analyze_subsection_rule_based(subsection)
    messages = build_subsection_analysis_messages(
        report_meta=report_meta,
        subsection={
            "subsection_id": subsection.subsection_id,
            "title_raw": subsection.title_raw,
            "title_normalized": subsection.title_normalized,
            "content_raw": subsection.content_raw,
        },
    )
    llm_result = llm_client.stream_chat(
        messages=messages,
        model=model,
        reasoning_split=reasoning_split,
        stream=True,
    )
    parsed = _safe_parse_json(llm_result.response_text)

    merged = dict(base)
    for key in (
        "problem_type",
        "risk_tags",
        "keywords",
        "evidence_sentences",
        "title_aliases",
        "rule_candidates",
        "confidence",
    ):
        if key in parsed and parsed[key] not in (None, "", []):
            merged[key] = parsed[key]
    merged["analysis_source"] = "llm_enhanced"
    merged["llm"] = {
        "model": llm_result.model,
        "response_text": llm_result.response_text,
        "reasoning_text": llm_result.reasoning_text,
        "reasoning_details": llm_result.reasoning_details,
        "raw_events": llm_result.raw_events,
        "usage": llm_result.usage,
    }
    return merged


def summarize_report_level(subsections: list[dict[str, Any]]) -> dict[str, Any]:
    problem_type_counter = Counter(item.get("problem_type", "其他") for item in subsections)
    rule_count = sum(len(item.get("rule_candidates", [])) for item in subsections)
    return {
        "subsection_count": len(subsections),
        "problem_type_distribution": dict(problem_type_counter),
        "rule_candidate_count": rule_count,
    }


def analyze_report(
    extracted_report: dict[str, Any],
    *,
    llm_client: OpenAICompatibleClient | None = None,
    model: str | None = None,
    reasoning_split: bool = True,
) -> dict[str, Any]:
    blocks = extracted_report.get("blocks", [])
    if not isinstance(blocks, list):
        raise ValueError("extracted_report.blocks must be a list")

    subsections_raw = split_subsections(blocks)
    report_meta = {
        "report_id": extracted_report.get("report_id", ""),
        "source_file": extracted_report.get("source_file", ""),
        "section_title": extracted_report.get("section_title", ""),
    }

    analyzed_subsections: list[dict[str, Any]] = []
    for subsection in subsections_raw:
        if llm_client is None:
            analyzed = analyze_subsection_rule_based(subsection)
        else:
            analyzed = analyze_subsection_with_llm(
                subsection=subsection,
                report_meta=report_meta,
                llm_client=llm_client,
                model=model,
                reasoning_split=reasoning_split,
            )
        analyzed_subsections.append(analyzed)

    return {
        "report_id": report_meta["report_id"],
        "source_file": report_meta["source_file"],
        "section_title": report_meta["section_title"],
        "subsections": analyzed_subsections,
        "report_level_summary": summarize_report_level(analyzed_subsections),
    }
