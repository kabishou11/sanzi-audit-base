from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXTRACTED_MD_DIR = ROOT / "outputs" / "extracted_md"
OUTPUT_CSV = ROOT / "outputs" / "financial_management_items.csv"

LEVEL1_RE = re.compile(r"^\s*[一二三四五六七八九十百千]+[、.．]\s*")
LEVEL2_RE = re.compile(r"^\s*[（(][一二三四五六七八九十百千]+[)）]\s*")
LEVEL3_RE = re.compile(r"^\s*(\d{1,2})[、.．]\s*(?!\d)(.+?)\s*$")


def iter_md_files(directory: Path) -> list[Path]:
    files = [path for path in directory.glob("*.md") if path.is_file()]
    files.sort()
    return files


def extract_financial_items(markdown_text: str) -> list[dict[str, str]]:
    lines = [line.rstrip() for line in markdown_text.splitlines()]
    in_finance_section = False
    current_item: dict[str, str] | None = None
    results: list[dict[str, str]] = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if current_item and current_item["item_content"]:
                current_item["item_content"] += "\n"
            continue

        if LEVEL2_RE.match(line):
            if in_finance_section and current_item is not None:
                current_item["item_content"] = current_item["item_content"].strip()
                results.append(current_item)
                current_item = None

            in_finance_section = "财务管理方面" in line
            continue

        if not in_finance_section:
            continue

        if LEVEL1_RE.match(line):
            if current_item is not None:
                current_item["item_content"] = current_item["item_content"].strip()
                results.append(current_item)
            break

        level3_match = LEVEL3_RE.match(line)
        if level3_match:
            if current_item is not None:
                current_item["item_content"] = current_item["item_content"].strip()
                results.append(current_item)
            current_item = {
                "item_no": level3_match.group(1),
                "item_title": level3_match.group(2).strip(),
                "item_full_title": line,
                "item_content": "",
            }
            continue

        if current_item is not None:
            if current_item["item_content"]:
                current_item["item_content"] += "\n"
            current_item["item_content"] += line

    if in_finance_section and current_item is not None:
        current_item["item_content"] = current_item["item_content"].strip()
        results.append(current_item)

    return results


def main() -> None:
    rows: list[dict[str, str]] = []
    for md_path in iter_md_files(EXTRACTED_MD_DIR):
        text = md_path.read_text(encoding="utf-8")
        items = extract_financial_items(text)
        for item in items:
            rows.append(
                {
                    "source_file_name": md_path.name,
                    "item_full_title": item["item_full_title"],
                    "item_content": item["item_content"],
                }
            )

    output_path = OUTPUT_CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = output_path.open("w", encoding="utf-8-sig", newline="")
    except PermissionError:
        output_path = OUTPUT_CSV.with_name(f"{OUTPUT_CSV.stem}_v2{OUTPUT_CSV.suffix}")
        handle = output_path.open("w", encoding="utf-8-sig", newline="")

    with handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source_file_name",
                "item_full_title",
                "item_content",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
