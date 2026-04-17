from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT / "input_reports" / "xlsx" / "major_issues_by_sheet_v7_technical_rules_csv"
DEFAULT_OUTPUT_CSV = ROOT / "input_reports" / "xlsx" / "major_issues_by_sheet_v7_technical_rules_merged.csv"
SHEET_FILE_RE = re.compile(r"^\d+_(.+)\.csv$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="将一个目录下按 sheet 拆分的多个 CSV 合并成一个总 CSV。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "常用示例：\n"
            f"  python {Path(__file__).name}\n"
            f"  python {Path(__file__).name} --input-dir \"{DEFAULT_INPUT_DIR}\"\n"
            f"  python {Path(__file__).name} --output-csv \"{DEFAULT_OUTPUT_CSV}\""
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"待合并的 CSV 目录，默认：{DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"合并后的总 CSV 路径，默认：{DEFAULT_OUTPUT_CSV}",
    )
    return parser.parse_args()


def iter_sheet_csvs(input_dir: Path) -> list[Path]:
    files = [
        path
        for path in input_dir.glob("*.csv")
        if path.is_file() and not path.name.startswith("_")
    ]
    files.sort()
    return files


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


def infer_sheet_name(file_name: str) -> str:
    match = SHEET_FILE_RE.match(file_name)
    return match.group(1) if match else Path(file_name).stem


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def merge_sheet_csvs(input_dir: Path) -> tuple[list[str], list[dict[str, str]]]:
    merged_rows: list[dict[str, str]] = []
    base_fields: list[str] = []

    for csv_path in iter_sheet_csvs(input_dir):
        rows = load_rows(csv_path)
        if not rows:
            continue
        if not base_fields:
            base_fields = list(rows[0].keys())
        sheet_name = infer_sheet_name(csv_path.name)
        for row in rows:
            merged_row = {
                "sheet_name": sheet_name,
                "sheet_file_name": csv_path.name,
            }
            for field in base_fields:
                merged_row[field] = row.get(field, "")
            merged_rows.append(merged_row)

    fieldnames = ["sheet_name", "sheet_file_name", *base_fields]
    return fieldnames, merged_rows


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_csv = args.output_csv.resolve()

    if not input_dir.exists():
        raise FileNotFoundError(f"输入目录不存在：{input_dir}")

    fieldnames, rows = merge_sheet_csvs(input_dir)
    if not rows:
        raise ValueError(f"目录下未找到可合并的 CSV：{input_dir}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    final_output = resolve_output_path(output_csv)
    with final_output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows from {input_dir} to {final_output}")


if __name__ == "__main__":
    main()
