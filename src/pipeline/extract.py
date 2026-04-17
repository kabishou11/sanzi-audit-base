from __future__ import annotations

from pathlib import Path

from src.pipeline.documents import load_document_paragraphs
from src.pipeline.models import (
    DocumentExtractionResult,
    DocumentParagraph,
    ExtractedBlock,
    SectionAnchor,
    SectionExtraction,
)
from src.utils.text import (
    clean_line,
    detect_heading_level,
    extract_numbering,
    is_list_item,
    is_probable_heading,
    make_report_id,
    normalize_for_match,
)


TARGET_SECTION_TITLE = "审计发现的主要问题"


class SectionNotFoundError(ValueError):
    """Raised when target section heading cannot be located."""


def extract_section_from_file(
    source_path: str | Path,
    cache_dir: str | Path,
    section_title: str = TARGET_SECTION_TITLE,
    converter: str = "auto",
) -> DocumentExtractionResult:
    source = Path(source_path)
    conversion, paragraphs = load_document_paragraphs(source, cache_dir, converter=converter)
    section = extract_section_from_paragraphs(paragraphs, section_title=section_title)
    return DocumentExtractionResult(
        report_id=make_report_id(source),
        source_path=source,
        conversion=conversion,
        paragraph_count=len(paragraphs),
        section=section,
    )


def extract_section_from_paragraphs(
    paragraphs: list[DocumentParagraph],
    section_title: str = TARGET_SECTION_TITLE,
) -> SectionExtraction:
    if not paragraphs:
        raise SectionNotFoundError("The document has no text paragraphs.")

    start_position = _find_section_start_position(paragraphs, section_title=section_title)
    start_paragraph = paragraphs[start_position]
    start_level = detect_heading_level(start_paragraph.text) or 1

    end_position, end_anchor = _find_section_end_position(
        paragraphs=paragraphs,
        start_position=start_position,
        start_level=start_level,
    )

    selected_paragraphs = paragraphs[start_position:end_position]
    blocks = _build_blocks(selected_paragraphs)
    raw_markdown = "\n\n".join(block.text for block in blocks if block.text)

    return SectionExtraction(
        section_title=section_title,
        start_anchor=SectionAnchor(
            index=start_paragraph.index,
            text=start_paragraph.text,
            level=start_level,
        ),
        end_anchor=end_anchor,
        raw_markdown=raw_markdown,
        blocks=blocks,
    )


def _find_section_start_position(paragraphs: list[DocumentParagraph], section_title: str) -> int:
    normalized_title = normalize_for_match(section_title)
    for idx, paragraph in enumerate(paragraphs):
        normalized_paragraph = normalize_for_match(paragraph.text)
        if normalized_title and normalized_title in normalized_paragraph:
            return idx
    raise SectionNotFoundError(f"Section heading not found: {section_title}")


def _find_section_end_position(
    paragraphs: list[DocumentParagraph],
    start_position: int,
    start_level: int,
) -> tuple[int, SectionAnchor | None]:
    for idx in range(start_position + 1, len(paragraphs)):
        text = clean_line(paragraphs[idx].text)
        if not text:
            continue
        if not is_probable_heading(text):
            continue
        level = detect_heading_level(text)
        if level is not None and level == start_level:
            return idx, SectionAnchor(index=paragraphs[idx].index, text=text, level=level)
        if level is None and start_level == 1 and _is_unnumbered_top_heading(text):
            return idx, SectionAnchor(index=paragraphs[idx].index, text=text, level=None)
    return len(paragraphs), None


def _build_blocks(paragraphs: list[DocumentParagraph]) -> list[ExtractedBlock]:
    blocks: list[ExtractedBlock] = []
    for paragraph in paragraphs:
        text = clean_line(paragraph.text)
        level = detect_heading_level(text)
        if level is not None:
            block_type = "heading"
        elif is_list_item(text):
            block_type = "list_item"
        else:
            block_type = "paragraph"
        blocks.append(
            ExtractedBlock(
                index=paragraph.index,
                type=block_type,
                text=text,
                normalized_text=normalize_for_match(text),
                numbering=extract_numbering(text),
                level_hint=level,
            )
        )
    return blocks


def _is_unnumbered_top_heading(text: str) -> bool:
    if len(text) > 24:
        return False
    if text.endswith(("如下", "如下：", "如下:")):
        return False
    # Exclude common sub-heading markers and list prefixes.
    if text.startswith(("（", "(", "-", "*")):
        return False
    if text[:1].isdigit():
        return False
    return True
