from __future__ import annotations

import hashlib
import re
from pathlib import Path


_WHITESPACE_RE = re.compile(r"[\s\u3000]+")

_RE_LEVEL_1 = re.compile(r"^[一二三四五六七八九十百千]+[、.．]\s*")
_RE_CHAPTER = re.compile(r"^第[一二三四五六七八九十百千0-9]+[章节部分编]\s*")
_RE_LEVEL_2 = re.compile(r"^[（(][一二三四五六七八九十百千]+[)）][、.．]?\s*")
_RE_LEVEL_3 = re.compile(r"^(\d{1,3}[、.．)]\s*|[（(]\d{1,3}[)）][、.．]?\s*)")
_RE_LEVEL_4 = re.compile(r"^[A-Za-z][、.．)]\s*")

_RE_NUMBERING = re.compile(
    r"^("
    r"[一二三四五六七八九十百千]+[、.．]|"
    r"第[一二三四五六七八九十百千0-9]+[章节部分编]|"
    r"[（(][一二三四五六七八九十百千]+[)）][、.．]?|"
    r"\d{1,3}[、.．)]|"
    r"[（(]\d{1,3}[)）][、.．]?|"
    r"[A-Za-z][、.．)]"
    r")\s*"
)

_RE_LIST_ITEM = re.compile(
    r"^("
    r"[-*]\s+|"
    r"\d{1,3}[、.．)]\s*|"
    r"[（(]\d{1,3}[)）]\s*|"
    r"[（(][一二三四五六七八九十百千]+[)）]\s*"
    r")"
)

_HEADING_PUNCTUATION = "。！？；，,!?"


def clean_line(text: str | None) -> str:
    if text is None:
        return ""
    normalized = text.replace("\u00A0", " ").replace("\u3000", " ")
    return normalized.strip()


def normalize_for_match(text: str | None) -> str:
    return _WHITESPACE_RE.sub("", clean_line(text))


def extract_numbering(text: str | None) -> str | None:
    line = clean_line(text)
    if not line:
        return None
    match = _RE_NUMBERING.match(line)
    if not match:
        return None
    return match.group(0).strip()


def detect_heading_level(text: str | None) -> int | None:
    line = clean_line(text)
    if not line:
        return None
    if _RE_LEVEL_1.match(line) or _RE_CHAPTER.match(line):
        return 1
    if _RE_LEVEL_2.match(line):
        return 2
    if _RE_LEVEL_3.match(line):
        return 3
    if _RE_LEVEL_4.match(line):
        return 4
    return None


def is_short_heading_candidate(text: str | None) -> bool:
    line = clean_line(text)
    if not line:
        return False
    if len(line) > 30:
        return False
    if any(char in line for char in _HEADING_PUNCTUATION):
        return False
    return True


def is_probable_heading(text: str | None) -> bool:
    if detect_heading_level(text) is not None:
        return True
    return is_short_heading_candidate(text)


def is_list_item(text: str | None) -> bool:
    line = clean_line(text)
    if not line:
        return False
    level = detect_heading_level(line)
    if level is not None and level <= 2:
        return False
    return bool(_RE_LIST_ITEM.match(line))


def make_report_id(path: str | Path) -> str:
    source = Path(path)
    stem = source.stem.strip().lower()
    slug = re.sub(r"[^\w]+", "_", stem, flags=re.UNICODE).strip("_")
    slug = re.sub(r"_+", "_", slug)
    if slug:
        return slug
    digest = hashlib.md5(str(source).encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return f"report_{digest}"
