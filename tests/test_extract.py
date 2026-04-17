from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.documents import scan_report_files
from src.pipeline.extract import SectionNotFoundError, extract_section_from_paragraphs
from src.pipeline.models import DocumentParagraph


def _to_paragraphs(lines: list[str]) -> list[DocumentParagraph]:
    return [DocumentParagraph(index=i, text=line) for i, line in enumerate(lines)]


def test_extract_section_until_next_same_level_heading() -> None:
    paragraphs = _to_paragraphs(
        [
            "一、基本情况",
            "二、审计发现的主要问题",
            "（一）资产管理不规范",
            "1. 未建立资产台账。",
            "（二）合同管理不到位",
            "三、审计建议",
            "后续内容",
        ]
    )
    section = extract_section_from_paragraphs(paragraphs)

    assert section.start_anchor.index == 1
    assert section.end_anchor is not None
    assert section.end_anchor.index == 5
    assert [block.text for block in section.blocks] == [
        "二、审计发现的主要问题",
        "（一）资产管理不规范",
        "1. 未建立资产台账。",
        "（二）合同管理不到位",
    ]
    assert "三、审计建议" not in section.raw_markdown


def test_extract_section_with_numbered_target_heading() -> None:
    paragraphs = _to_paragraphs(
        [
            "二、专项资金使用情况",
            "三、审计发现的主要问题",
            "（一）报销凭证不完整",
            "（二）合同审批滞后",
            "四、审计处理意见",
        ]
    )
    section = extract_section_from_paragraphs(paragraphs)

    assert section.start_anchor.text == "三、审计发现的主要问题"
    assert section.end_anchor is not None
    assert section.end_anchor.text == "四、审计处理意见"


def test_extract_section_without_same_level_end_heading() -> None:
    paragraphs = _to_paragraphs(
        [
            "审计发现的主要问题",
            "（一）预算执行偏差较大",
            "整改建议如下",
        ]
    )
    section = extract_section_from_paragraphs(paragraphs)

    assert section.end_anchor is None
    assert len(section.blocks) == 3


def test_extract_section_not_found() -> None:
    paragraphs = _to_paragraphs(["一、基本情况", "二、工作开展情况"])
    with pytest.raises(SectionNotFoundError):
        extract_section_from_paragraphs(paragraphs)


def test_scan_report_files_filters_extensions_and_lock_files(tmp_path: Path) -> None:
    (tmp_path / "a.doc").write_text("x", encoding="utf-8")
    (tmp_path / "b.docx").write_text("x", encoding="utf-8")
    (tmp_path / "~$b.docx").write_text("x", encoding="utf-8")
    (tmp_path / "c.txt").write_text("x", encoding="utf-8")

    files = scan_report_files(tmp_path)

    assert [path.name for path in files] == ["a.doc", "b.docx"]
