from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import threading
import time
import zipfile
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

LOGGER = logging.getLogger("classify_technical_rules_from_xlsx")
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_config
from src.llm.client import OpenAICompatibleClient


DEFAULT_INPUT_XLSX = ROOT / "input_reports" / "xlsx" / "major_issues_by_sheet_v7.xlsx"
DEFAULT_MEMORY_DIR = ROOT / "prompts" / "technical_rule_memory"
OUTPUT_COLUMN = "技术规则分类"
CONFIDENCE_COLUMN = "分类置信度"
RATIONALE_COLUMN = "分类依据摘要"
XML_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "p": "http://schemas.openxmlformats.org/package/2006/relationships",
}
INVALID_FILENAME_RE = re.compile(r'[<>:"/\\|?*]+')
MAX_CATEGORY_LINES = 3
CANDIDATE_CATEGORY_LIMIT = 6
MEMORY_FILE_ORDER = [
    "README.md",
    "00_overview.md",
    "01_core_categories.md",
    "02_boundary_rules.md",
    "03_multilabel_and_fallback.md",
    "04_usage_strategy.md",
]
KNOWN_CATEGORY_ALIASES = {
    "流程缺失": "缺流程要素",
    "缺少流程要素": "缺流程要素",
    "缺流程": "缺流程要素",
    "资料附件缺失": "附件内缺少内容",
    "资料缺失": "附件内缺少内容",
    "附件不全": "附件内缺少内容",
    "附件缺失": "附件内缺少内容",
    "时序倒置": "流程时间倒置",
    "时间倒置": "流程时间倒置",
    "长期挂账": "长期挂账未处理",
    "挂账未处理": "长期挂账未处理",
    "做账不规范": "财务做账不规范",
    "账务处理不规范": "财务做账不规范",
    "合同内容问题": "合同内容存在问题",
    "合同条款问题": "合同内容存在问题",
    "内容不一致": "内容一致性问题",
    "一致性问题": "内容一致性问题",
    "业务重复": "业务高度重合",
    "重复支付": "业务高度重合",
}

SYSTEM_PROMPT = """你是“三资专项审计问题”的技术规则分类助手。

你的任务：
对每一条审计问题，根据 item_full_title 与 item_content，判断它在“技术规则层面”属于什么问题模式。

这里的“技术规则分类”强调的是：
1. 关注技术/规则层面的违规模式，而不是业务领域名称。
2. 同一底层模式应尽量归到同一类，例如：
   - 缺审批、缺决议、缺签字、缺比价、缺验收、缺台账、缺附件等，可归入“缺流程要素”或更精确的流程/资料缺失类；
   - 时间先后颠倒、签约晚于执行、验收早于确认，可归入“流程时间倒置”；
   - 长期挂账、长期未清理、往来款久拖未处理，可归入“长期挂账未处理”；
   - 会计科目使用错误、收支确认不规范，可归入“财务做账不规范”；
   - 合同缺条款、条款不清、签订主体不一致，可归入“合同内容存在问题”；
   - 内容、口径、台账、图斑、合同与实际不一致，可归入“内容一致性问题”。
3. 可以参考已有技术规则记忆中的判断特征，但不要被历史标签机械限制。
4. 如果一条内容同时明显符合多个技术规则分类，可以输出多行，每行一个分类。

输出要求：
1. 只输出分类结果，不要解释原因。
2. 每行一个分类，最多 3 行。
3. 分类名称尽量简洁，通常 4-10 个字。
4. 若可归入已有常见模式，优先复用稳定表述；如果现有分类都不贴切，可以输出更合适的新分类名。
5. 输出时必须使用 JSON。
"""


@dataclass(slots=True)
class RowTask:
    sheet_title: str
    row_number: int
    row_data: dict[str, str]


@dataclass(slots=True)
class CategoryProfile:
    name: str
    body: str
    keywords: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="逐行调用大模型，为多 sheet 审计问题 Excel 生成技术规则分类 CSV。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "常用示例：\n"
            f"  python {Path(__file__).name}\n"
            f"  python {Path(__file__).name} --max-workers 4 --batch-size 6\n"
            f"  python {Path(__file__).name} --sheet \"财务管理方面\" --limit 20\n"
            f"  python {Path(__file__).name} --only-failed\n"
            f"  python {Path(__file__).name} --rerun-empty"
        ),
    )
    parser.add_argument("--input-xlsx", type=Path, default=DEFAULT_INPUT_XLSX, help="输入 xlsx 路径。")
    parser.add_argument(
        "--memory-dir",
        type=Path,
        default=DEFAULT_MEMORY_DIR,
        help="技术规则记忆 md 目录路径。",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="输出目录。默认在输入文件同级目录下自动创建。",
    )
    parser.add_argument("--max-workers", type=int, default=4, help="并发线程数，默认 4。")
    parser.add_argument("--batch-size", type=int, default=1, help="批处理大小，默认 1 表示逐条。")
    parser.add_argument("--max-retries", type=int, default=3, help="单行失败重试次数，默认 3。")
    parser.add_argument("--only-failed", action="store_true", help="只重跑上次失败的行。")
    parser.add_argument("--rerun-empty", action="store_true", help="只重跑分类为空的行。")
    parser.add_argument("--force", action="store_true", help="忽略已有结果，整表重跑。")
    parser.add_argument(
        "--sheet",
        action="append",
        default=[],
        help="只处理指定 sheet，可重复传入多次。",
    )
    parser.add_argument(
        "--rows",
        type=str,
        default="",
        help="只处理指定 Excel 行号，逗号分隔，例如 2,5,8。建议与 --sheet 一起用。",
    )
    parser.add_argument("--limit", type=int, default=None, help="仅处理前 N 条待处理记录，便于抽样。")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def safe_filename(name: str) -> str:
    cleaned = INVALID_FILENAME_RE.sub("_", name).strip()
    return cleaned or "sheet"


def default_output_dir(input_xlsx: Path) -> Path:
    return input_xlsx.with_suffix("").with_name(f"{input_xlsx.stem}_technical_rules_csv")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def col_letters_to_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha())
    value = 0
    for ch in letters:
        value = value * 26 + (ord(ch.upper()) - 64)
    return value - 1


def cell_to_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(cell.itertext()).strip()
    value = cell.find("a:v", XML_NS)
    if value is None or value.text is None:
        return ""
    raw = value.text
    if cell_type == "s":
        index = int(raw)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""
    return raw


def load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    return ["".join(item.itertext()) for item in root.findall("a:si", XML_NS)]


def load_xlsx_sheets(path: Path) -> OrderedDict[str, list[dict[str, str]]]:
    sheets_data: OrderedDict[str, list[dict[str, str]]] = OrderedDict()
    with zipfile.ZipFile(path) as zf:
        shared_strings = load_shared_strings(zf)
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_root.findall("p:Relationship", XML_NS)
        }

        for sheet in workbook.find("a:sheets", XML_NS):
            title = sheet.attrib.get("name", "")
            rel_id = sheet.attrib.get(f"{{{XML_NS['r']}}}id", "")
            target = rel_map.get(rel_id)
            if not target:
                continue
            sheet_root = ET.fromstring(zf.read(f"xl/{target}"))
            sheet_data = sheet_root.find("a:sheetData", XML_NS)
            if sheet_data is None:
                sheets_data[title] = []
                continue

            rows: list[list[str]] = []
            for row in sheet_data.findall("a:row", XML_NS):
                values_map: dict[int, str] = {}
                max_idx = -1
                for cell in row.findall("a:c", XML_NS):
                    ref = cell.attrib.get("r", "")
                    idx = col_letters_to_index(ref)
                    values_map[idx] = cell_to_text(cell, shared_strings)
                    max_idx = max(max_idx, idx)
                row_values = [values_map.get(i, "") for i in range(max_idx + 1)]
                rows.append(row_values)

            if not rows:
                sheets_data[title] = []
                continue

            headers = rows[0]
            normalized_headers = [header.strip() for header in headers]
            data_rows: list[dict[str, str]] = []
            for row_values in rows[1:]:
                record = {
                    normalized_headers[i]: row_values[i] if i < len(row_values) else ""
                    for i in range(len(normalized_headers))
                    if normalized_headers[i]
                }
                data_rows.append(record)
            sheets_data[title] = data_rows

    return sheets_data


def parse_markdown_sections(text: str) -> OrderedDict[str, str]:
    sections: OrderedDict[str, list[str]] = OrderedDict()
    current = ""
    for line in text.splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            sections[current] = []
            continue
        if current:
            sections[current].append(line)
    return OrderedDict((title, "\n".join(lines).strip()) for title, lines in sections.items())


def extract_keywords_from_section(section_text: str) -> list[str]:
    keywords: list[str] = []
    in_block = False
    for raw_line in section_text.splitlines():
        line = raw_line.strip()
        if line.startswith("高频触发特征"):
            in_block = True
            continue
        if not line:
            continue
        if in_block and line.startswith("- "):
            keywords.append(line[2:].strip())
            continue
        if in_block and not line.startswith("- "):
            break
    return keywords


def load_memory_bundle(memory_dir: Path) -> dict[str, Any]:
    parts: list[str] = []
    used: set[str] = set()
    file_texts: dict[str, str] = {}
    for name in MEMORY_FILE_ORDER:
        path = memory_dir / name
        if path.exists():
            text = path.read_text(encoding="utf-8").strip()
            parts.append(text)
            file_texts[name] = text
            used.add(name)

    for path in sorted(memory_dir.glob("*.md")):
        if path.name in used:
            continue
        text = path.read_text(encoding="utf-8").strip()
        parts.append(text)
        file_texts[path.name] = text

    core_sections_raw = parse_markdown_sections(file_texts.get("01_core_categories.md", ""))
    core_profiles = [
        CategoryProfile(name=name, body=body, keywords=extract_keywords_from_section(body))
        for name, body in core_sections_raw.items()
    ]
    boundary_sections = parse_markdown_sections(file_texts.get("02_boundary_rules.md", ""))
    return {
        "full_text": "\n\n".join(part for part in parts if part),
        "overview": file_texts.get("00_overview.md", ""),
        "core_profiles": core_profiles,
        "boundary_sections": boundary_sections,
        "multilabel": file_texts.get("03_multilabel_and_fallback.md", ""),
        "usage": file_texts.get("04_usage_strategy.md", ""),
    }


def build_label_fingerprint(label: str) -> str:
    return re.sub(r"\s+", "", label).replace("分类", "")


def normalize_confidence(value: str) -> str:
    text = (value or "").strip().lower()
    if text in {"高", "high", "h", "1.0", "0.9", "0.8"}:
        return "高"
    if text in {"中", "medium", "med", "m", "0.7", "0.6", "0.5"}:
        return "中"
    if text in {"低", "low", "l", "0.4", "0.3", "0.2", "0.1"}:
        return "低"
    if "高" in text:
        return "高"
    if "低" in text:
        return "低"
    return "中"


def normalize_rationale(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    return cleaned[:80]


def normalize_match_text(text: str) -> str:
    return re.sub(r"[\s:：，,。.；;（）()、/\-]+", "", (text or "").lower())


def build_user_prompt(
    *,
    sheet_title: str,
    source_file_name: str,
    item_full_title: str,
    item_content: str,
    memory_block: str,
    candidate_categories: list[str],
) -> str:
    candidate_text = "、".join(candidate_categories) if candidate_categories else "无明确候选，可自行判断"
    return f"""下面是一条需要判断“技术规则分类”的审计问题，请结合标题与正文做判断。

当前所属sheet（业务类目，仅供参考）：{sheet_title}
来源文件：{source_file_name}
问题标题：{item_full_title}
问题内容：
{item_content}

下面是已经沉淀好的“技术规则分类模型记忆”，帮助你理解判断特征与边界：
{memory_block}

本条问题优先候选分类（仅供参考，不是硬限制）：
{candidate_text}

请现在输出 JSON，格式如下：
{{
  "technical_rule_category": ["分类1", "分类2"],
  "confidence": "高/中/低",
  "rationale": "20-60字的简短判断依据"
}}

要求：
1. 优先复用记忆中的稳定分类名。
2. 但不要被历史分类标签限制；如果这条问题明显更适合一个新的、更准确的技术规则分类，可以直接输出新分类名。
3. `technical_rule_category` 最多 3 个。
4. `rationale` 要简短，便于人工复核。
5. 不要输出 JSON 以外的任何文字。"""


def normalize_category_output(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("模型返回为空")

    cleaned = raw.replace("```", "").strip()
    if cleaned.startswith("{") and cleaned.endswith("}"):
        try:
            payload = json.loads(cleaned)
            category = str(payload.get("technical_rule_category", "")).strip()
            if category:
                cleaned = category
        except json.JSONDecodeError:
            pass

    lines: list[str] = []
    seen: set[str] = set()
    for line in cleaned.splitlines():
        item = line.strip().lstrip("-").strip()
        item = re.sub(r"^\d+[.、]\s*", "", item)
        item = item.replace("技术规则分类：", "").strip()
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            lines.append(item)
        if len(lines) >= MAX_CATEGORY_LINES:
            break

    if not lines:
        raise ValueError(f"模型返回无法解析为分类：{raw}")

    return "\n".join(lines)


def normalize_category_labels(labels: list[str] | str) -> str:
    if isinstance(labels, str):
        raw_items = labels.splitlines()
    else:
        raw_items = []
        for item in labels:
            raw_items.extend(str(item).splitlines())

    canonical_by_fingerprint = {
        build_label_fingerprint(value): value for value in set(KNOWN_CATEGORY_ALIASES.values())
    }
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        item = re.sub(r"^\d+[.、]\s*", "", str(raw).strip())
        if not item:
            continue
        item = KNOWN_CATEGORY_ALIASES.get(item, item)
        fingerprint = build_label_fingerprint(item)
        item = canonical_by_fingerprint.get(fingerprint, item)
        if item not in seen:
            seen.add(item)
            normalized.append(item)
        if len(normalized) >= MAX_CATEGORY_LINES:
            break
    return "\n".join(normalized)


def parse_json_payload(text: str) -> Any:
    cleaned = (text or "").strip()
    if not cleaned:
        raise ValueError("模型返回为空")
    start = min([idx for idx in [cleaned.find("{"), cleaned.find("[")] if idx != -1], default=-1)
    if start > 0:
        cleaned = cleaned[start:]
    end_obj = cleaned.rfind("}")
    end_arr = cleaned.rfind("]")
    end = max(end_obj, end_arr)
    if end != -1:
        cleaned = cleaned[: end + 1]
    return json.loads(cleaned)


def parse_single_model_result(text: str) -> dict[str, str]:
    payload = parse_json_payload(text)
    if isinstance(payload, list):
        payload = payload[0] if payload else {}
    if not isinstance(payload, dict):
        raise ValueError(f"模型返回不是对象：{text}")
    categories = normalize_category_labels(payload.get("technical_rule_category", []))
    if not categories:
        categories = normalize_category_output(payload.get("technical_rule_category", ""))
    return {
        "technical_rule_category": categories,
        "confidence": normalize_confidence(str(payload.get("confidence", "中"))),
        "rationale": normalize_rationale(str(payload.get("rationale", ""))),
    }


def score_category_candidate(text: str, profile: CategoryProfile) -> int:
    score = 0
    full_text = text.lower()
    normalized_text = normalize_match_text(text)
    if profile.name in text:
        score += 5
    for keyword in profile.keywords:
        normalized_keyword = normalize_match_text(keyword)
        if keyword and (
            keyword.lower() in full_text
            or (normalized_keyword and normalized_keyword in normalized_text)
        ):
            score += 3 if len(keyword) >= 4 else 1
    return score


def select_candidate_categories(
    *,
    sheet_title: str,
    item_full_title: str,
    item_content: str,
    memory_bundle: dict[str, Any],
    limit: int = CANDIDATE_CATEGORY_LIMIT,
) -> list[str]:
    text = f"{sheet_title}\n{item_full_title}\n{item_content}"
    scored: list[tuple[int, str]] = []
    for profile in memory_bundle["core_profiles"]:
        score = score_category_candidate(text, profile)
        scored.append((score, profile.name))
    scored.sort(key=lambda item: (-item[0], item[1]))
    positive = [name for score, name in scored if score > 0][:limit]
    if positive:
        return positive
    return [profile.name for profile in memory_bundle["core_profiles"][:limit]]


def build_memory_block_for_task(
    *,
    candidate_categories: list[str],
    memory_bundle: dict[str, Any],
) -> str:
    parts = [memory_bundle.get("overview", "").strip()]
    core_map = {profile.name: profile.body for profile in memory_bundle["core_profiles"]}
    for name in candidate_categories:
        body = core_map.get(name, "")
        if body:
            parts.append(f"## {name}\n{body}")
    for title, body in memory_bundle.get("boundary_sections", {}).items():
        if any(name in title for name in candidate_categories):
            parts.append(f"## {title}\n{body}")
    if len(candidate_categories) > 1:
        parts.append(memory_bundle.get("multilabel", "").strip())
    return "\n\n".join(part for part in parts if part)


def load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_progress(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def sheet_csv_path(output_dir: Path, sheet_index: int, sheet_title: str) -> Path:
    return output_dir / f"{sheet_index:02d}_{safe_filename(sheet_title)}.csv"


def write_sheet_csv(
    *,
    csv_path: Path,
    rows: list[dict[str, str]],
    row_state: dict[str, Any],
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["source_file_name", "item_full_title", "item_content", OUTPUT_COLUMN, CONFIDENCE_COLUMN, RATIONALE_COLUMN]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for idx, row in enumerate(rows, start=2):
            state = row_state.get(str(idx), {})
            category = state.get("technical_rule_category", "") if state.get("status") == "done" else ""
            confidence = state.get("confidence", "") if state.get("status") == "done" else ""
            rationale = state.get("rationale", "") if state.get("status") == "done" else ""
            writer.writerow(
                {
                    "source_file_name": row.get("source_file_name", ""),
                    "item_full_title": row.get("item_full_title", ""),
                    "item_content": row.get("item_content", ""),
                    OUTPUT_COLUMN: category,
                    CONFIDENCE_COLUMN: confidence,
                    RATIONALE_COLUMN: rationale,
                }
            )


def initialize_state(
    *,
    input_xlsx: Path,
    memory_dir: Path,
    output_dir: Path,
    sheets: OrderedDict[str, list[dict[str, str]]],
    existing: dict[str, Any],
) -> dict[str, Any]:
    state = existing or {
        "input_xlsx": str(input_xlsx),
        "memory_dir": str(memory_dir),
        "output_dir": str(output_dir),
        "created_at": now_iso(),
        "sheets": {},
    }
    state["memory_dir"] = str(memory_dir)
    state["last_run_started_at"] = now_iso()

    for index, (sheet_title, rows) in enumerate(sheets.items(), start=1):
        sheet_state = state.setdefault("sheets", {}).setdefault(sheet_title, {})
        sheet_state["sheet_index"] = index
        sheet_state["csv_path"] = str(sheet_csv_path(output_dir, index, sheet_title))
        sheet_state["row_count"] = len(rows)
        sheet_state.setdefault("rows", {})

    return state


def parse_rows_filter(raw: str) -> set[int]:
    if not raw.strip():
        return set()
    rows: set[int] = set()
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        rows.add(int(value))
    return rows


def collect_tasks(
    *,
    sheets: OrderedDict[str, list[dict[str, str]]],
    state: dict[str, Any],
    selected_sheets: set[str],
    selected_rows: set[int],
    only_failed: bool,
    rerun_empty: bool,
    force: bool,
    limit: int | None,
) -> tuple[list[RowTask], int]:
    tasks: list[RowTask] = []
    skipped_done = 0
    for sheet_title, rows in sheets.items():
        if selected_sheets and sheet_title not in selected_sheets:
            continue
        row_state = state["sheets"][sheet_title]["rows"]
        for row_number, row in enumerate(rows, start=2):
            if selected_rows and row_number not in selected_rows:
                continue
            current = row_state.get(str(row_number), {})
            status = current.get("status")
            if only_failed:
                if status != "failed":
                    continue
            elif rerun_empty:
                if (current.get("technical_rule_category") or "").strip():
                    continue
            elif not force and status == "done":
                skipped_done += 1
                continue
            tasks.append(RowTask(sheet_title=sheet_title, row_number=row_number, row_data=row))
            if limit is not None and len(tasks) >= limit:
                return tasks, skipped_done
    return tasks, skipped_done


class ClientPool:
    def __init__(self, config: Any) -> None:
        self._config = config
        self._local = threading.local()

    def get(self) -> OpenAICompatibleClient:
        client = getattr(self._local, "client", None)
        if client is None:
            client = OpenAICompatibleClient(
                api_key=self._config.openai_api_key,
                base_url=self._config.openai_base_url,
                default_model=self._config.openai_model,
                timeout=self._config.openai_timeout,
                max_retries=self._config.openai_max_retries,
            )
            self._local.client = client
        return client


def classify_task(
    task: RowTask,
    *,
    config: Any,
    memory_bundle: dict[str, Any],
    max_retries: int,
    client_pool: ClientPool,
) -> dict[str, Any]:
    last_error = ""
    candidate_categories = select_candidate_categories(
        sheet_title=task.sheet_title,
        item_full_title=task.row_data.get("item_full_title", ""),
        item_content=task.row_data.get("item_content", ""),
        memory_bundle=memory_bundle,
    )
    memory_block = build_memory_block_for_task(candidate_categories=candidate_categories, memory_bundle=memory_bundle)
    for attempt in range(1, max_retries + 1):
        try:
            client = client_pool.get()
            response = client.stream_chat(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": build_user_prompt(
                            sheet_title=task.sheet_title,
                            source_file_name=task.row_data.get("source_file_name", ""),
                            item_full_title=task.row_data.get("item_full_title", ""),
                            item_content=task.row_data.get("item_content", ""),
                            memory_block=memory_block,
                            candidate_categories=candidate_categories,
                        ),
                    },
                ],
                reasoning_split=config.openai_reasoning_split,
                extra_body={"reasoning_split": config.openai_reasoning_split},
                stream=False,
            )
            parsed = parse_single_model_result(response.response_text)
            return {
                "status": "done",
                "technical_rule_category": parsed["technical_rule_category"],
                "confidence": parsed["confidence"],
                "rationale": parsed["rationale"],
                "candidate_categories": candidate_categories,
                "attempts": attempt,
                "model": response.model,
                "usage": response.usage,
                "raw_response": response.response_text,
                "updated_at": now_iso(),
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(min(1.5 * attempt, 5))

    return {
        "status": "failed",
        "technical_rule_category": "",
        "confidence": "",
        "rationale": "",
        "attempts": max_retries,
        "error": last_error,
        "updated_at": now_iso(),
    }


def build_batch_user_prompt(
    *,
    tasks: list[RowTask],
    memory_bundle: dict[str, Any],
) -> str:
    all_candidates: list[str] = []
    row_blocks: list[str] = []
    for idx, task in enumerate(tasks, start=1):
        candidates = select_candidate_categories(
            sheet_title=task.sheet_title,
            item_full_title=task.row_data.get("item_full_title", ""),
            item_content=task.row_data.get("item_content", ""),
            memory_bundle=memory_bundle,
        )
        all_candidates.extend(candidates)
        row_blocks.append(
            "\n".join(
                [
                    f"row_id: {idx}",
                    f"sheet: {task.sheet_title}",
                    f"source_file_name: {task.row_data.get('source_file_name', '')}",
                    f"item_full_title: {task.row_data.get('item_full_title', '')}",
                    "item_content:",
                    task.row_data.get("item_content", ""),
                    f"candidate_categories: {'、'.join(candidates)}",
                ]
            )
        )
    merged_candidates = list(dict.fromkeys(all_candidates))[: CANDIDATE_CATEGORY_LIMIT + 2]
    memory_block = build_memory_block_for_task(candidate_categories=merged_candidates, memory_bundle=memory_bundle)
    return f"""下面有多条审计问题，请逐条输出 JSON 数组结果。

技术规则记忆：
{memory_block}

待判断数据：
{'\n\n'.join(row_blocks)}

请输出 JSON 数组，格式如下：
[
  {{
    "row_id": 1,
    "technical_rule_category": ["分类1", "分类2"],
    "confidence": "高/中/低",
    "rationale": "20-60字简短判断依据"
  }}
]

要求：
1. 按 row_id 一一返回，不要遗漏。
2. 优先复用已有稳定分类名，但允许输出更贴切的新分类名。
3. 除 JSON 外不要输出任何文字。"""


def parse_batch_model_result(text: str, task_count: int) -> dict[int, dict[str, str]]:
    payload = parse_json_payload(text)
    if not isinstance(payload, list):
        raise ValueError(f"批处理返回不是数组：{text}")
    result_map: dict[int, dict[str, str]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        row_id = int(item.get("row_id", 0))
        if row_id <= 0:
            continue
        result_map[row_id] = {
            "technical_rule_category": normalize_category_labels(item.get("technical_rule_category", [])),
            "confidence": normalize_confidence(str(item.get("confidence", "中"))),
            "rationale": normalize_rationale(str(item.get("rationale", ""))),
        }
    if len(result_map) != task_count:
        raise ValueError(f"批处理结果数量不匹配，期望 {task_count} 实得 {len(result_map)}")
    return result_map


def classify_batch_tasks(
    tasks: list[RowTask],
    *,
    config: Any,
    memory_bundle: dict[str, Any],
    max_retries: int,
    client_pool: ClientPool,
) -> dict[int, dict[str, Any]]:
    last_error = ""
    prompt = build_batch_user_prompt(tasks=tasks, memory_bundle=memory_bundle)
    for attempt in range(1, max_retries + 1):
        try:
            client = client_pool.get()
            response = client.stream_chat(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                reasoning_split=config.openai_reasoning_split,
                extra_body={"reasoning_split": config.openai_reasoning_split},
                stream=False,
            )
            parsed = parse_batch_model_result(response.response_text, len(tasks))
            return {
                idx: {
                    "status": "done",
                    "technical_rule_category": parsed[idx]["technical_rule_category"],
                    "confidence": parsed[idx]["confidence"],
                    "rationale": parsed[idx]["rationale"],
                    "attempts": attempt,
                    "model": response.model,
                    "usage": response.usage,
                    "raw_response": response.response_text,
                    "updated_at": now_iso(),
                }
                for idx in parsed
            }
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(min(1.5 * attempt, 5))

    if len(tasks) > 1:
        fallback: dict[int, dict[str, Any]] = {}
        for idx, task in enumerate(tasks, start=1):
            result = classify_task(
                task,
                config=config,
                memory_bundle=memory_bundle,
                max_retries=max_retries,
                client_pool=client_pool,
            )
            result["error"] = last_error if result.get("status") != "done" else result.get("error", "")
            fallback[idx] = result
        return fallback

    return {
        1: {
            "status": "failed",
            "technical_rule_category": "",
            "confidence": "",
            "rationale": "",
            "attempts": max_retries,
            "error": last_error,
            "updated_at": now_iso(),
        }
    }


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def print_progress(
    *,
    completed: int,
    total: int,
    success: int,
    failed: int,
    skipped: int,
    current: str = "",
) -> None:
    message = (
        f"\r进度 {completed}/{total} | 成功 {success} | 失败 {failed} | 已跳过 {skipped}"
    )
    if current:
        message += f" | 当前 {current}"
    print(message, end="", flush=True)


def chunk_tasks(tasks: list[RowTask], batch_size: int) -> list[list[RowTask]]:
    size = max(1, batch_size)
    return [tasks[i : i + size] for i in range(0, len(tasks), size)]


def main() -> None:
    args = parse_args()
    config = load_config()
    configure_logging(config.log_level)

    if not config.openai_api_key:
        raise ValueError("OPENAI_API_KEY 缺失，请先在 .env 中配置。")

    input_xlsx = args.input_xlsx.resolve()
    memory_dir = args.memory_dir.resolve()
    output_dir = (args.output_dir.resolve() if args.output_dir else default_output_dir(input_xlsx))
    progress_path = output_dir / "_progress.json"
    results_jsonl = output_dir / "_llm_results.jsonl"
    failed_jsonl = output_dir / "_failed_rows.jsonl"

    if not input_xlsx.exists():
        raise FileNotFoundError(f"输入 xlsx 不存在：{input_xlsx}")
    if not memory_dir.exists():
        raise FileNotFoundError(f"技术规则记忆目录不存在：{memory_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    sheets = load_xlsx_sheets(input_xlsx)
    memory_bundle = load_memory_bundle(memory_dir)
    existing_state = load_progress(progress_path)
    state = initialize_state(
        input_xlsx=input_xlsx,
        memory_dir=memory_dir,
        output_dir=output_dir,
        sheets=sheets,
        existing=existing_state,
    )
    state["model"] = config.openai_model
    state["max_workers"] = args.max_workers
    state["max_retries"] = args.max_retries
    state["batch_size"] = args.batch_size

    for sheet_title, rows in sheets.items():
        csv_path = Path(state["sheets"][sheet_title]["csv_path"])
        write_sheet_csv(csv_path=csv_path, rows=rows, row_state=state["sheets"][sheet_title]["rows"])
    save_progress(progress_path, state)

    selected_sheets = set(args.sheet)
    selected_rows = parse_rows_filter(args.rows)
    tasks, skipped_done = collect_tasks(
        sheets=sheets,
        state=state,
        selected_sheets=selected_sheets,
        selected_rows=selected_rows,
        only_failed=args.only_failed,
        rerun_empty=args.rerun_empty,
        force=args.force,
        limit=args.limit,
    )

    total = len(tasks)
    if total == 0:
        print("没有需要处理的行，可能都已完成。")
        return

    LOGGER.info("输入文件：%s", input_xlsx)
    LOGGER.info("记忆目录：%s", memory_dir)
    LOGGER.info("输出目录：%s", output_dir)
    LOGGER.info("总 sheet 数：%s", len(sheets))
    LOGGER.info("待处理行数：%s", total)
    LOGGER.info("已跳过已完成行：%s", skipped_done)
    LOGGER.info("并发数：%s", args.max_workers)
    LOGGER.info("批处理大小：%s", args.batch_size)
    LOGGER.info("模型：%s", config.openai_model)

    success = 0
    failed = 0
    completed = 0
    client_pool = ClientPool(config)

    task_groups = chunk_tasks(tasks, args.batch_size)

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        future_map = {}
        for group in task_groups:
            if len(group) == 1:
                future = executor.submit(
                    classify_task,
                    group[0],
                    config=config,
                    memory_bundle=memory_bundle,
                    max_retries=max(1, args.max_retries),
                    client_pool=client_pool,
                )
            else:
                future = executor.submit(
                    classify_batch_tasks,
                    group,
                    config=config,
                    memory_bundle=memory_bundle,
                    max_retries=max(1, args.max_retries),
                    client_pool=client_pool,
                )
            future_map[future] = group

        for future in as_completed(future_map):
            group = future_map[future]
            try:
                batch_result = future.result()
            except Exception as exc:  # noqa: BLE001
                batch_result = {
                    idx + 1: {
                        "status": "failed",
                        "technical_rule_category": "",
                        "confidence": "",
                        "rationale": "",
                        "attempts": args.max_retries,
                        "error": str(exc),
                        "updated_at": now_iso(),
                    }
                    for idx in range(len(group))
                }

            for idx, task in enumerate(group, start=1):
                result = batch_result[idx] if len(group) > 1 else batch_result
                current_label = f"{task.sheet_title} / 第{task.row_number}行"
                row_state = state["sheets"][task.sheet_title]["rows"]
                row_state[str(task.row_number)] = result

                csv_path = Path(state["sheets"][task.sheet_title]["csv_path"])
                write_sheet_csv(csv_path=csv_path, rows=sheets[task.sheet_title], row_state=row_state)
                save_progress(progress_path, state)

                payload = {
                    "sheet_title": task.sheet_title,
                    "row_number": task.row_number,
                    "source_file_name": task.row_data.get("source_file_name", ""),
                    "item_full_title": task.row_data.get("item_full_title", ""),
                    "status": result.get("status"),
                    OUTPUT_COLUMN: result.get("technical_rule_category", ""),
                    CONFIDENCE_COLUMN: result.get("confidence", ""),
                    RATIONALE_COLUMN: result.get("rationale", ""),
                    "attempts": result.get("attempts", 0),
                    "error": result.get("error", ""),
                    "updated_at": result.get("updated_at", now_iso()),
                }
                append_jsonl(results_jsonl, payload)

                completed += 1
                if result.get("status") == "done":
                    success += 1
                else:
                    failed += 1
                    append_jsonl(failed_jsonl, payload)

                print_progress(
                    completed=completed,
                    total=total,
                    success=success,
                    failed=failed,
                    skipped=skipped_done,
                    current=current_label,
                )

    print()
    print(f"处理完成：成功 {success}，失败 {failed}，跳过 {skipped_done}")
    LOGGER.info("处理完成：成功 %s，失败 %s，跳过 %s", success, failed, skipped_done)
    LOGGER.info("结果目录：%s", output_dir)
    LOGGER.info("进度文件：%s", progress_path)


if __name__ == "__main__":
    main()
