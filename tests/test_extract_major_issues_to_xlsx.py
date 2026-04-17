from __future__ import annotations

import importlib.util
import shutil
import zipfile
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "extract_major_issues_to_xlsx.py"
SPEC = importlib.util.spec_from_file_location("extract_major_issues_to_xlsx", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_extract_major_issue_sections_groups_rows_by_level2_heading() -> None:
    markdown = "\n".join(
        [
            "三、审计发现的主要问题",
            "",
            "（一）资产资源管理方面",
            "",
            "1、存在账外资产",
            "",
            "这里是正文。",
            "",
            "（1）补充说明",
            "",
            "补充内容。",
            "",
            "（二）财务管理方面",
            "",
            "1、账务处理不规范",
            "",
            "第一段。",
            "",
            "第二段。",
            "",
            "四、审计建议",
        ]
    )

    result = MODULE.extract_major_issue_sections(markdown)

    assert list(result) == ["资产资源管理方面", "财务管理方面"]
    assert result["资产资源管理方面"][0]["item_full_title"] == "1、存在账外资产"
    assert result["资产资源管理方面"][0]["item_content"] == "这里是正文。\n\n（1）补充说明\n\n补充内容。"
    assert result["财务管理方面"][0]["item_content"] == "第一段。\n\n第二段。"


def test_extract_major_issue_sections_merges_same_level2_title_with_different_numbering() -> None:
    markdown = "\n".join(
        [
            "三、审计发现的主要问题",
            "",
            "（一）资产资源管理方面",
            "",
            "1、问题A",
            "",
            "正文A。",
            "",
            "（二）资产资源管理方面",
            "",
            "1、问题B",
            "",
            "正文B。",
            "",
            "四、审计建议",
        ]
    )

    result = MODULE.extract_major_issue_sections(markdown)

    assert list(result) == ["资产资源管理方面"]
    assert [item["item_full_title"] for item in result["资产资源管理方面"]] == ["1、问题A", "1、问题B"]


def test_normalize_level2_title_merges_obvious_similar_titles() -> None:
    assert MODULE.normalize_level2_title("（一）资产管理方面") == "资产资源管理方面"
    assert MODULE.normalize_level2_title("（二）资源管理方面") == "资产资源管理方面"
    assert MODULE.normalize_level2_title("（三）资产资源管理方面") == "资产资源管理方面"
    assert MODULE.normalize_level2_title("（一）工程管理") == "工程管理方面"


def test_mask_sensitive_text_redacts_person_org_and_place() -> None:
    text = (
        "无锡市厚鑫机械厂与王美芬签订合同，"
        "由江阴市顾山镇红豆村股份经济合作社收取租金1000元。"
    )

    masked = MODULE.mask_sensitive_text(text)

    assert "无锡市厚鑫机械厂" not in masked
    assert "王美芬" not in masked
    assert "江阴市顾山镇红豆村股份经济合作社" not in masked
    assert "1000元" in masked
    assert "xxx" in masked


def test_mask_sensitive_text_redacts_listed_names_before_year_and_amount() -> None:
    text = (
        "截止目前，仍有个别租户租金暂未收取，总金额44.11万元，包括："
        "刘传永2025年租金29.28万元、xxx2025年租金11.71万元、浦春英2025年租金3.12万元。"
    )

    masked = MODULE.mask_sensitive_text(text)

    assert "刘传永" not in masked
    assert "浦春英" not in masked
    assert "29.28万元" in masked
    assert "11.71万元" in masked
    assert "3.12万元" in masked


def test_write_xlsx_creates_multiple_sheets() -> None:
    temp_dir = Path(__file__).resolve().parent / "_tmp_major_issues_test"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output_path = temp_dir / "major_issues.xlsx"
    rows_by_sheet = {
        "资产资源管理方面": [
            {
                "source_file_name": "无锡市厚鑫机械厂_a.md",
                "item_full_title": "1、王美芬承租问题",
                "item_content": "江阴市顾山镇红豆村股份经济合作社向王美芬收取1000元",
            }
        ],
        "财务管理方面": [
            {
                "source_file_name": "b.md",
                "item_full_title": "1、问题B",
                "item_content": "正文B",
            }
        ],
    }

    MODULE.write_xlsx(rows_by_sheet, output_path)

    with zipfile.ZipFile(output_path) as zf:
        workbook_xml = zf.read("xl/workbook.xml").decode("utf-8")
        sheet1_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")

    assert "资产资源管理方面" in workbook_xml
    assert "财务管理方面" in workbook_xml
    assert "source_file_name" in sheet1_xml

    shutil.rmtree(temp_dir)


def test_collect_rows_by_sheet_masks_exported_fields() -> None:
    temp_dir = Path(__file__).resolve().parent / "_tmp_collect_rows_test"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    md_path = temp_dir / "无锡市厚鑫机械厂_a.md"
    md_path.write_text(
        "\n".join(
            [
                "三、审计发现的主要问题",
                "",
                "（一）资产管理方面",
                "",
                "1、王美芬承租问题",
                "",
                "江阴市顾山镇红豆村股份经济合作社向王美芬收取1000元。",
            ]
        ),
        encoding="utf-8",
    )

    rows_by_sheet = MODULE.collect_rows_by_sheet(temp_dir)
    row = rows_by_sheet["资产资源管理方面"][0]

    assert row["source_file_name"] == "无锡市厚鑫机械厂_a.md"
    assert "王美芬" not in row["item_full_title"]
    assert "江阴市顾山镇红豆村股份经济合作社" not in row["item_content"]
    assert "1000元" in row["item_content"]
    assert "xxx" in row["item_full_title"]
    assert "xxx" in row["item_content"]

    shutil.rmtree(temp_dir)
