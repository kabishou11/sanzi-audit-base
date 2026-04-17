from __future__ import annotations

import argparse
import re
import zipfile
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT / "outputs" / "extracted_md"
DEFAULT_OUTPUT_XLSX = ROOT / "outputs" / "major_issues_by_sheet.xlsx"
TARGET_SECTION_TITLE = "审计发现的主要问题"

LEVEL1_RE = re.compile(r"^\s*[一二三四五六七八九十百千]+[、.．]\s*")
LEVEL2_RE = re.compile(r"^\s*[（(][一二三四五六七八九十百千]+[)）]\s*(.+?)\s*$")
LEVEL3_RE = re.compile(r"^\s*(\d{1,2})[、.．]\s*(?!\d)(.+?)\s*$")
INVALID_SHEET_CHARS_RE = re.compile(r"[:\\/?*\[\]]")
TITLE_CORE_RE = re.compile(r"\s+")
PLACE_RE = re.compile(r"[一-龥A-Za-z0-9]{2,30}(?:省|市|区|县|镇|乡|街道|村|社区)")
ORG_RE = re.compile(
    r"[一-龥A-Za-z0-9（）()·]{2,40}"
    r"(?:股份经济合作社|股份有限公司|有限责任公司|集团有限公司|有限公司|合作社|村民委员会|村委会|居委会|工作站|"
    r"服务部|经营部|事务所|商行|银行|物业管理有限公司|物业服务有限公司|建设工程有限公司|工程有限公司|工厂|公司|厂)"
)
PERSON_PAREN_RE = re.compile(r"(?<![一-龥])[一-龥]{2,4}(?=[（(])")
PERSON_CONTEXT_RE = re.compile(
    r"(?:(?<=与)|(?<=给)|(?<=向)|(?<=由)|(?<=支付)|(?<=收到)|(?<=收取)|(?<=承租)|(?<=补偿)|(?<=发放)|(?<=租给))"
    r"[一-龥]{2,4}"
)
PERSON_ACTION_RE = re.compile(r"(?<![一-龥])[一-龥]{2,4}(?=(?:承租|租赁|补偿|付款|收款|借款|问题))")
PERSON_AMOUNT_RE = re.compile(r"(?<![一-龥])[一-龥]{2,4}(?=(?:\d{4}年)?(?:租金|款项|款|费用|补贴|补偿|借款|金额))")
NON_PERSON_TOKENS = {
    "村民",
    "公司",
    "工厂",
    "合同",
    "项目",
    "工程",
    "金额",
    "期限",
    "面积",
    "其中",
    "上述",
    "部分",
    "合作社",
    "村委会",
    "居委会",
    "工作站",
}


def iter_md_files(directory: Path) -> list[Path]:
    files = [path for path in directory.glob("*.md") if path.is_file()]
    files.sort()
    return files


def normalize_level2_title(line: str) -> str:
    match = LEVEL2_RE.match(line.strip())
    title = match.group(1).strip() if match else line.strip()
    core = TITLE_CORE_RE.sub("", title)
    if core.endswith("方面"):
        core = core[:-2]

    if core in {"资产管理", "资源管理", "资产资源管理"}:
        return "资产资源管理方面"
    if core == "工程管理":
        return "工程管理方面"
    if core == "财务管理":
        return "财务管理方面"
    if core == "资金管理":
        return "资金管理方面"
    if core == "债务管理":
        return "债务管理方面"
    if core == "采购管理":
        return "采购管理方面"
    if core == "合同管理":
        return "合同管理方面"
    return title


def _mask_person_match(match: re.Match[str]) -> str:
    token = match.group(0)
    if token in NON_PERSON_TOKENS:
        return token
    return "xxx"


def mask_sensitive_text(text: str) -> str:
    masked = text
    masked = ORG_RE.sub("xxx", masked)
    masked = PLACE_RE.sub("xxx", masked)
    masked = PERSON_PAREN_RE.sub(_mask_person_match, masked)
    masked = PERSON_CONTEXT_RE.sub(_mask_person_match, masked)
    masked = PERSON_ACTION_RE.sub(_mask_person_match, masked)
    masked = PERSON_AMOUNT_RE.sub(_mask_person_match, masked)
    masked = re.sub(r"(?:xxx)+", "xxx", masked)
    return masked


def extract_major_issue_sections(markdown_text: str) -> dict[str, list[dict[str, str]]]:
    lines = [line.rstrip() for line in markdown_text.splitlines()]
    results: dict[str, list[dict[str, str]]] = defaultdict(list)

    in_target_section = False
    current_sheet_title: str | None = None
    current_item: dict[str, str] | None = None

    def flush_current_item() -> None:
        nonlocal current_item
        if current_sheet_title is None or current_item is None:
            return
        current_item["item_content"] = current_item["item_content"].strip()
        if current_item["item_full_title"] or current_item["item_content"]:
            results[current_sheet_title].append(current_item)
        current_item = None

    def ensure_current_item() -> dict[str, str]:
        nonlocal current_item
        if current_item is None:
            current_item = {
                "item_no": "",
                "item_title": "",
                "item_full_title": current_sheet_title or "",
                "item_content": "",
            }
        return current_item

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            if current_item and current_item["item_content"]:
                current_item["item_content"] += "\n"
            continue

        if not in_target_section:
            if TARGET_SECTION_TITLE in line:
                in_target_section = True
            continue

        level2_match = LEVEL2_RE.match(line)
        if level2_match:
            flush_current_item()
            current_sheet_title = normalize_level2_title(line)
            continue

        if current_sheet_title is None:
            continue

        if LEVEL1_RE.match(line):
            flush_current_item()
            break

        level3_match = LEVEL3_RE.match(line)
        if level3_match:
            flush_current_item()
            current_item = {
                "item_no": level3_match.group(1),
                "item_title": level3_match.group(2).strip(),
                "item_full_title": line,
                "item_content": "",
            }
            continue

        item = ensure_current_item()
        if item["item_content"]:
            item["item_content"] += "\n"
        item["item_content"] += line

    flush_current_item()
    return dict(results)


def collect_rows_by_sheet(md_dir: Path) -> dict[str, list[dict[str, str]]]:
    rows_by_sheet: dict[str, list[dict[str, str]]] = defaultdict(list)

    for md_path in iter_md_files(md_dir):
        text = md_path.read_text(encoding="utf-8")
        extracted = extract_major_issue_sections(text)
        for sheet_title, rows in extracted.items():
            for row in rows:
                rows_by_sheet[sheet_title].append(
                    {
                        "source_file_name": md_path.name,
                        "item_full_title": mask_sensitive_text(row["item_full_title"]),
                        "item_content": mask_sensitive_text(row["item_content"]),
                    }
                )

    return dict(rows_by_sheet)


def make_unique_sheet_names(sheet_titles: list[str]) -> dict[str, str]:
    used_names: set[str] = set()
    mapping: dict[str, str] = {}

    for title in sheet_titles:
        cleaned = INVALID_SHEET_CHARS_RE.sub(" ", title).strip().strip("'")
        cleaned = cleaned or "Sheet"
        base_name = cleaned[:31]
        candidate = base_name
        suffix = 1
        while candidate in used_names:
            suffix_text = f"_{suffix}"
            candidate = f"{base_name[:31 - len(suffix_text)]}{suffix_text}"
            suffix += 1
        used_names.add(candidate)
        mapping[title] = candidate

    return mapping


def write_xlsx(rows_by_sheet: dict[str, list[dict[str, str]]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ordered_titles = sorted(rows_by_sheet)
    sheet_name_map = make_unique_sheet_names(ordered_titles)

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", build_content_types(len(ordered_titles)))
        zf.writestr("_rels/.rels", build_root_rels())
        zf.writestr("xl/workbook.xml", build_workbook_xml(ordered_titles, sheet_name_map))
        zf.writestr("xl/_rels/workbook.xml.rels", build_workbook_rels(len(ordered_titles)))
        zf.writestr("xl/styles.xml", build_styles_xml())

        for idx, title in enumerate(ordered_titles, start=1):
            sheet_rows = rows_by_sheet[title]
            worksheet_xml = build_worksheet_xml(sheet_rows)
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", worksheet_xml)


def build_content_types(sheet_count: int) -> str:
    worksheet_overrides = "\n".join(
        f'  <Override PartName="/xl/worksheets/sheet{idx}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for idx in range(1, sheet_count + 1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">\n'
        '  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>\n'
        '  <Default Extension="xml" ContentType="application/xml"/>\n'
        '  <Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>\n'
        '  <Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>\n'
        f"{worksheet_overrides}\n"
        "</Types>"
    )


def build_root_rels() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
        '  <Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>\n'
        "</Relationships>"
    )


def build_workbook_xml(ordered_titles: list[str], sheet_name_map: dict[str, str]) -> str:
    sheets = "\n".join(
        f'    <sheet name="{xml_attr(sheet_name_map[title])}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, title in enumerate(ordered_titles, start=1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">\n'
        "  <sheets>\n"
        f"{sheets}\n"
        "  </sheets>\n"
        "</workbook>"
    )


def build_workbook_rels(sheet_count: int) -> str:
    relations = "\n".join(
        f'  <Relationship Id="rId{idx}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        f'Target="worksheets/sheet{idx}.xml"/>'
        for idx in range(1, sheet_count + 1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
        f"{relations}\n"
        "</Relationships>"
    )


def build_styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">\n'
        '  <fonts count="2">\n'
        '    <font><sz val="11"/><name val="Calibri"/></font>\n'
        '    <font><b/><sz val="11"/><name val="Calibri"/></font>\n'
        '  </fonts>\n'
        '  <fills count="2">\n'
        '    <fill><patternFill patternType="none"/></fill>\n'
        '    <fill><patternFill patternType="gray125"/></fill>\n'
        '  </fills>\n'
        '  <borders count="1">\n'
        '    <border><left/><right/><top/><bottom/><diagonal/></border>\n'
        '  </borders>\n'
        '  <cellStyleXfs count="1">\n'
        '    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>\n'
        '  </cellStyleXfs>\n'
        '  <cellXfs count="3">\n'
        '    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>\n'
        '    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1" '
        'applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>\n'
        '    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1">'
        '<alignment vertical="top" wrapText="1"/></xf>\n'
        '  </cellXfs>\n'
        '  <cellStyles count="1">\n'
        '    <cellStyle name="Normal" xfId="0" builtinId="0"/>\n'
        '  </cellStyles>\n'
        '</styleSheet>'
    )


def build_worksheet_xml(rows: list[dict[str, str]]) -> str:
    all_rows = [
        ["source_file_name", "item_full_title", "item_content"],
        *[
            [
                row["source_file_name"],
                row["item_full_title"],
                row["item_content"],
            ]
            for row in rows
        ],
    ]

    row_xml_parts: list[str] = []
    for row_idx, row_values in enumerate(all_rows, start=1):
        cell_style = 1 if row_idx == 1 else 2
        cells = []
        for col_idx, value in enumerate(row_values, start=1):
            cells.append(
                build_inline_string_cell(
                    ref=f"{column_name(col_idx)}{row_idx}",
                    value=value,
                    style_index=cell_style,
                )
            )
        row_xml_parts.append(f'    <row r="{row_idx}">{"".join(cells)}</row>')

    dimension_ref = f"A1:C{len(all_rows)}"
    sheet_data = "\n".join(row_xml_parts)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">\n'
        f'  <dimension ref="{dimension_ref}"/>\n'
        '  <sheetViews>\n'
        '    <sheetView workbookViewId="0">\n'
        '      <pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>\n'
        '      <selection pane="bottomLeft" activeCell="A2" sqref="A2"/>\n'
        '    </sheetView>\n'
        '  </sheetViews>\n'
        '  <sheetFormatPr defaultRowHeight="18"/>\n'
        '  <cols>\n'
        '    <col min="1" max="1" width="32" customWidth="1"/>\n'
        '    <col min="2" max="2" width="36" customWidth="1"/>\n'
        '    <col min="3" max="3" width="120" customWidth="1"/>\n'
        '  </cols>\n'
        '  <sheetData>\n'
        f"{sheet_data}\n"
        '  </sheetData>\n'
        '  <autoFilter ref="A1:C1"/>\n'
        '</worksheet>'
    )


def build_inline_string_cell(ref: str, value: str, style_index: int) -> str:
    escaped = xml_text(value)
    return f'<c r="{ref}" s="{style_index}" t="inlineStr"><is><t xml:space="preserve">{escaped}</t></is></c>'


def column_name(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def xml_attr(value: str) -> str:
    return escape(value, {'"': "&quot;"})


def xml_text(value: str) -> str:
    return escape(value)


def resolve_output_path(output_path: Path) -> Path:
    if not output_path.exists():
        return output_path
    stem = output_path.stem
    suffix = output_path.suffix
    counter = 2
    while True:
        candidate = output_path.with_name(f"{stem}_v{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按二级标题拆分“审计发现的主要问题”，输出为多 sheet 的 Excel 文件。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "常用示例：\n"
            f"  python {Path(__file__).name}\n"
            f"  python {Path(__file__).name} --input-dir \"{DEFAULT_INPUT_DIR}\"\n"
            f"  python {Path(__file__).name} --output \"{DEFAULT_OUTPUT_XLSX.with_name('major_issues_by_sheet_v7.xlsx')}\""
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Markdown 输入目录，默认：{DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_XLSX,
        help=f"输出 Excel 路径，默认：{DEFAULT_OUTPUT_XLSX}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir: Path = args.input_dir
    if not input_dir.exists():
        raise FileNotFoundError(f"输入目录不存在：{input_dir}")

    rows_by_sheet = collect_rows_by_sheet(input_dir)
    if not rows_by_sheet:
        raise ValueError(f"未在目录中找到可导出的内容：{input_dir}")

    output_path = resolve_output_path(args.output)
    write_xlsx(rows_by_sheet, output_path)

    total_rows = sum(len(rows) for rows in rows_by_sheet.values())
    print(f"Wrote {total_rows} rows across {len(rows_by_sheet)} sheets to {output_path}")


if __name__ == "__main__":
    main()
