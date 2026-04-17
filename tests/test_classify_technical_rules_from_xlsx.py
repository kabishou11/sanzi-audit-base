from __future__ import annotations

import importlib.util
import sys
from collections import OrderedDict
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "classify_technical_rules_from_xlsx.py"
SPEC = importlib.util.spec_from_file_location("classify_technical_rules_from_xlsx", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_normalize_category_output_supports_plain_text_and_multiline() -> None:
    assert MODULE.normalize_category_output("缺流程要素") == "缺流程要素"
    assert MODULE.normalize_category_output("1. 缺流程要素\n2. 流程时间倒置") == "缺流程要素\n流程时间倒置"


def test_normalize_category_output_supports_json_payload() -> None:
    raw = '{"technical_rule_category":"财务做账不规范"}'
    assert MODULE.normalize_category_output(raw) == "财务做账不规范"


def test_safe_filename_replaces_invalid_characters() -> None:
    assert MODULE.safe_filename('合同管理方面:测试/样例') == "合同管理方面_测试_样例"


def test_parse_rows_filter_handles_blank_and_numbers() -> None:
    assert MODULE.parse_rows_filter("") == set()
    assert MODULE.parse_rows_filter("2, 5,8") == {2, 5, 8}


def test_normalize_category_labels_merges_aliases_and_deduplicates() -> None:
    labels = ["做账不规范", "财务做账不规范", "资料缺失", "附件缺失", "时序倒置"]
    assert MODULE.normalize_category_labels(labels) == "财务做账不规范\n附件内缺少内容\n流程时间倒置"


def test_parse_single_model_result_extracts_confidence_and_rationale() -> None:
    raw = """
    {
      "technical_rule_category": ["长期挂账", "做账不规范"],
      "confidence": "high",
      "rationale": "账项长期未清理，且账务处理逻辑不规范。"
    }
    """
    parsed = MODULE.parse_single_model_result(raw)
    assert parsed["technical_rule_category"] == "长期挂账未处理\n财务做账不规范"
    assert parsed["confidence"] == "高"
    assert parsed["rationale"] == "账项长期未清理，且账务处理逻辑不规范。"


def test_parse_batch_model_result_normalizes_each_row() -> None:
    raw = """
    [
      {
        "row_id": 1,
        "technical_rule_category": ["资料缺失"],
        "confidence": "中",
        "rationale": "附件虽有但关键内容缺失。"
      },
      {
        "row_id": 2,
        "technical_rule_category": ["做账不规范", "长期挂账"],
        "confidence": "low",
        "rationale": "同时存在挂账与账务处理问题。"
      }
    ]
    """
    parsed = MODULE.parse_batch_model_result(raw, task_count=2)
    assert parsed[1]["technical_rule_category"] == "附件内缺少内容"
    assert parsed[2]["technical_rule_category"] == "财务做账不规范\n长期挂账未处理"
    assert parsed[2]["confidence"] == "低"


def test_select_candidate_categories_prefers_relevant_memory_profiles() -> None:
    memory_bundle = MODULE.load_memory_bundle(
        Path(__file__).resolve().parents[1] / "prompts" / "technical_rule_memory"
    )
    candidates = MODULE.select_candidate_categories(
        sheet_title="财务管理方面",
        item_full_title="部分往来款长期挂账未清理",
        item_content="截至审计日，部分应付款已挂账5年以上仍未处理。",
        memory_bundle=memory_bundle,
    )
    assert candidates
    assert candidates[0] == "长期挂账未处理"


def test_collect_tasks_supports_rerun_empty() -> None:
    sheets = OrderedDict(
        {
            "财务管理方面": [
                {
                    "source_file_name": "a.docx",
                    "item_full_title": "标题1",
                    "item_content": "内容1",
                },
                {
                    "source_file_name": "b.docx",
                    "item_full_title": "标题2",
                    "item_content": "内容2",
                },
                {
                    "source_file_name": "c.docx",
                    "item_full_title": "标题3",
                    "item_content": "内容3",
                },
            ]
        }
    )
    state = {
        "sheets": {
            "财务管理方面": {
                "rows": {
                    "2": {"status": "done", "technical_rule_category": "缺流程要素"},
                    "3": {"status": "done", "technical_rule_category": ""},
                    "4": {"status": "failed", "technical_rule_category": ""},
                }
            }
        }
    }
    tasks, skipped_done = MODULE.collect_tasks(
        sheets=sheets,
        state=state,
        selected_sheets=set(),
        selected_rows=set(),
        only_failed=False,
        rerun_empty=True,
        force=False,
        limit=None,
    )
    assert [task.row_number for task in tasks] == [3, 4]
    assert skipped_done == 0
