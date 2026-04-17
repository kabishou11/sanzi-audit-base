from __future__ import annotations

import argparse
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


def resolve_output_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    index = 2
    while True:
        candidate = path.with_name(f"{stem}_v{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从提取后的 Markdown 中汇总所有“财务管理方面”问题，导出为 CSV。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "常用示例：\n"
            f"  python {Path(__file__).name}\n"
            f"  python {Path(__file__).name} --input-dir \"{EXTRACTED_MD_DIR}\"\n"
            f"  python {Path(__file__).name} --output-csv \"{OUTPUT_CSV.with_name('financial_management_items_v2.csv')}\""
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=EXTRACTED_MD_DIR,
        help=f"Markdown 输入目录，默认：{EXTRACTED_MD_DIR}",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=OUTPUT_CSV,
        help=f"输出 CSV 路径，默认：{OUTPUT_CSV}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows: list[dict[str, str]] = []
    input_dir = args.input_dir
    if not input_dir.exists():
        raise FileNotFoundError(f"输入目录不存在：{input_dir}")

    for md_path in iter_md_files(input_dir):
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

    output_path = args.output_csv
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = output_path.open("w", encoding="utf-8-sig", newline="")
    except PermissionError:
        output_path = resolve_output_path(output_path)
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
