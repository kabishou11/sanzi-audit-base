from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


BlockType = Literal["heading", "paragraph", "list_item"]
ConversionStatus = Literal["no_conversion", "converted", "failed"]


@dataclass(slots=True)
class DocumentParagraph:
    index: int
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "text": self.text,
        }


@dataclass(slots=True)
class ConversionResult:
    source_path: Path
    effective_path: Path
    status: ConversionStatus
    method: str | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_path": str(self.source_path),
            "effective_path": str(self.effective_path),
            "status": self.status,
            "method": self.method,
            "message": self.message,
        }


@dataclass(slots=True)
class SectionAnchor:
    index: int
    text: str
    level: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "text": self.text,
            "level": self.level,
        }


@dataclass(slots=True)
class ExtractedBlock:
    index: int
    type: BlockType
    text: str
    normalized_text: str
    numbering: str | None = None
    level_hint: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "type": self.type,
            "text": self.text,
            "normalized_text": self.normalized_text,
            "numbering": self.numbering,
            "level_hint": self.level_hint,
        }


@dataclass(slots=True)
class SectionExtraction:
    section_title: str
    start_anchor: SectionAnchor
    end_anchor: SectionAnchor | None
    raw_markdown: str
    blocks: list[ExtractedBlock]

    def to_dict(self) -> dict[str, Any]:
        return {
            "section_title": self.section_title,
            "start_anchor": self.start_anchor.to_dict(),
            "end_anchor": self.end_anchor.to_dict() if self.end_anchor else None,
            "raw_markdown": self.raw_markdown,
            "blocks": [block.to_dict() for block in self.blocks],
        }


@dataclass(slots=True)
class DocumentExtractionResult:
    report_id: str
    source_path: Path
    conversion: ConversionResult
    paragraph_count: int
    section: SectionExtraction

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "source_path": str(self.source_path),
            "conversion": self.conversion.to_dict(),
            "paragraph_count": self.paragraph_count,
            "section": self.section.to_dict(),
        }
