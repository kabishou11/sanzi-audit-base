from __future__ import annotations

from src.pipeline.aggregate import aggregate_report_analyses
from src.pipeline.analyze import normalize_title


def _sample_reports() -> list[dict]:
    return [
        {
            "report_id": "r1",
            "source_file": "a.docx",
            "subsections": [
                {
                    "title_raw": "一、资产管理不规范",
                    "title_normalized": "资产管理不规范",
                    "problem_type": "资产管理",
                    "keywords": ["资产", "台账", "盘点"],
                    "risk_tags": ["资产管理"],
                    "rule_candidates": [{"rule_name": "资产管理异常检查"}],
                    "confidence": 0.8,
                },
                {
                    "title_raw": "（一）资金收支管理薄弱",
                    "title_normalized": "资金收支管理薄弱",
                    "problem_type": "资金管理",
                    "keywords": ["资金", "收支", "票据", "23,640.72元", "2023.12/30", "000.00元"],
                    "risk_tags": ["资金管理"],
                    "rule_candidates": [{"rule_name": "资金管理异常检查"}],
                    "confidence": 0.7,
                },
            ],
        },
        {
            "report_id": "r2",
            "source_file": "b.docx",
            "subsections": [
                {
                    "title_raw": "1. 资产管理不规范",
                    "title_normalized": "资产管理不规范",
                    "problem_type": "资产管理",
                    "keywords": ["资产", "折旧"],
                    "risk_tags": ["资产管理"],
                    "rule_candidates": [{"rule_name": "资产管理异常检查"}],
                    "confidence": 0.9,
                }
            ],
        },
    ]


def test_normalize_title() -> None:
    assert normalize_title("一、资产管理不规范") == "资产管理不规范"
    assert normalize_title("（一） 资金收支管理薄弱") == "资金收支管理薄弱"
    assert normalize_title("1. 合同管理缺失") == "合同管理缺失"


def test_aggregate_report_analyses_counts() -> None:
    aggregate = aggregate_report_analyses(_sample_reports())
    assert aggregate["report_count"] == 2
    assert aggregate["subsection_count"] == 3

    subsection_top = aggregate["subsection_frequency"][0]
    assert subsection_top["title_normalized"] == "资产管理不规范"
    assert subsection_top["count"] == 2

    problem_counts = {row["problem_type"]: row["count"] for row in aggregate["problem_type_frequency"]}
    assert problem_counts["资产管理"] == 2
    assert problem_counts["资金管理"] == 1

    rule_counts = {row["rule_name"]: row["count"] for row in aggregate["rule_candidate_frequency"]}
    assert rule_counts["资产管理异常检查"] == 2
    assert rule_counts["资金管理异常检查"] == 1

    csv_keys = set(aggregate["csv_exports"].keys())
    assert csv_keys == {"subsection_frequency", "problem_type_frequency", "rule_candidate_frequency"}
    assert "keyword_frequency" not in aggregate

    clean_keywords = [item["keyword"] for item in aggregate["narrative_materials"]["top_clean_keywords"]]
    assert "资金" in clean_keywords
    assert "23,640.72元" not in clean_keywords
    assert "2023.12/30" not in clean_keywords
    assert "000.00元" not in clean_keywords


def test_aggregate_markdown_output() -> None:
    aggregate = aggregate_report_analyses(_sample_reports())
    markdown = aggregate["markdown_report"]
    assert "审计问题汇总分析报告" in markdown
    assert "汇总摘要（用于 Narrative 生成前置）" in markdown
    assert "附录：核心统计" in markdown
    assert "高频关键词 Top 30" not in markdown
