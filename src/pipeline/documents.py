from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from src.pipeline.models import ConversionResult, DocumentParagraph
from src.utils.text import clean_line


SUPPORTED_EXTENSIONS = {".doc", ".docx"}


class DocumentProcessingError(RuntimeError):
    """Raised when a report file cannot be converted or parsed."""


def scan_report_files(input_dir: str | Path) -> list[Path]:
    root = Path(input_dir)
    if not root.exists():
        return []
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("~$"):
            continue
        if path.suffix.lower() in SUPPORTED_EXTENSIONS:
            files.append(path)
    files.sort()
    return files


def resolve_effective_docx_path(
    source_path: str | Path,
    cache_dir: str | Path,
    converter: str = "auto",
) -> ConversionResult:
    source = Path(source_path)
    if not source.exists():
        raise DocumentProcessingError(f"Source document does not exist: {source}")

    ext = source.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise DocumentProcessingError(f"Unsupported document extension: {source.suffix}")

    if ext == ".docx":
        return ConversionResult(
            source_path=source,
            effective_path=source,
            status="no_conversion",
            method="native_docx",
        )

    cache_root = Path(cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    output_path = _build_converted_output_path(source, cache_root)

    if output_path.exists() and output_path.stat().st_mtime >= source.stat().st_mtime:
        return ConversionResult(
            source_path=source,
            effective_path=output_path,
            status="converted",
            method="cached",
        )

    methods = _resolve_converter_methods(converter)
    failure_messages: list[str] = []

    for method in methods:
        if method == "powershell_word":
            ok, message = _convert_with_powershell_word(source, output_path)
        elif method == "win32com":
            ok, message = _convert_with_win32com(source, output_path)
        elif method == "soffice":
            ok, message = _convert_with_soffice(source, output_path, cache_root)
        else:
            continue

        if ok:
            return ConversionResult(
                source_path=source,
                effective_path=output_path,
                status="converted",
                method=method,
                message=message,
            )
        failure_messages.append(f"{method}: {message}")

    return ConversionResult(
        source_path=source,
        effective_path=source,
        status="failed",
        method=",".join(methods),
        message=" | ".join(failure_messages) if failure_messages else "No converter available.",
    )


def load_document_paragraphs(
    source_path: str | Path,
    cache_dir: str | Path,
    converter: str = "auto",
) -> tuple[ConversionResult, list[DocumentParagraph]]:
    conversion = resolve_effective_docx_path(source_path, cache_dir, converter=converter)
    if conversion.status == "failed":
        raise DocumentProcessingError(conversion.message or "Failed to convert .doc to .docx.")
    paragraphs = read_docx_paragraphs(conversion.effective_path)
    return conversion, paragraphs


def read_docx_paragraphs(docx_path: str | Path) -> list[DocumentParagraph]:
    path = Path(docx_path)
    if not path.exists():
        raise DocumentProcessingError(f"DOCX file does not exist: {path}")

    try:
        with zipfile.ZipFile(path) as archive:
            xml_bytes = archive.read("word/document.xml")
    except KeyError as exc:
        raise DocumentProcessingError(f"Missing word/document.xml in: {path}") from exc
    except zipfile.BadZipFile as exc:
        raise DocumentProcessingError(f"Invalid DOCX archive: {path}") from exc

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DocumentProcessingError(f"Invalid XML in document.xml: {path}") from exc

    paragraphs: list[DocumentParagraph] = []
    paragraph_index = 0
    for paragraph in root.iter():
        if _local_name(paragraph.tag) != "p":
            continue
        text = _extract_paragraph_text(paragraph)
        cleaned = clean_line(text)
        if not cleaned:
            continue
        paragraphs.append(DocumentParagraph(index=paragraph_index, text=cleaned))
        paragraph_index += 1
    return paragraphs


def _extract_paragraph_text(element: ET.Element) -> str:
    segments: list[str] = []
    for node in element.iter():
        local = _local_name(node.tag)
        if local == "t" and node.text:
            segments.append(node.text)
        elif local == "tab":
            segments.append("\t")
        elif local in {"br", "cr"}:
            segments.append("\n")
    return "".join(segments)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _resolve_converter_methods(converter: str) -> list[str]:
    normalized = (converter or "auto").strip().lower()
    if normalized == "auto":
        return ["powershell_word", "win32com", "soffice"]
    if normalized in {"powershell_word", "win32com", "soffice"}:
        return [normalized]
    if normalized in {"none", "disabled"}:
        return []
    return ["powershell_word", "win32com", "soffice"]


def _build_converted_output_path(source: Path, cache_root: Path) -> Path:
    digest = hashlib.md5(str(source.resolve()).encode("utf-8"), usedforsecurity=False).hexdigest()[:10]
    return cache_root / f"{source.stem}.{digest}.docx"


def _convert_with_win32com(source: Path, target: Path) -> tuple[bool, str]:
    try:
        import win32com.client  # type: ignore[import-not-found]
    except ImportError:
        return False, "win32com is not installed."

    word = None
    document = None
    try:
        word = win32com.client.DispatchEx("Word.Application")  # type: ignore[attr-defined]
        word.Visible = False
        document = word.Documents.Open(str(source.resolve()))
        document.SaveAs(str(target.resolve()), FileFormat=16)
        document.Close(False)
        word.Quit()
        return True, "Converted via Microsoft Word COM."
    except Exception as exc:  # noqa: BLE001
        return False, f"Word COM conversion failed: {exc}"
    finally:
        if document is not None:
            try:
                document.Close(False)
            except Exception:  # noqa: BLE001
                pass
        if word is not None:
            try:
                word.Quit()
            except Exception:  # noqa: BLE001
                pass


def _convert_with_powershell_word(source: Path, target: Path) -> tuple[bool, str]:
    src_literal = str(source.resolve()).replace("'", "''")
    dst_literal = str(target.resolve()).replace("'", "''")
    script = f"""
$ErrorActionPreference = 'Stop'
$src = '{src_literal}'
$dst = '{dst_literal}'
$word = $null
$doc = $null
try {{
    $word = New-Object -ComObject Word.Application
    $word.Visible = $false
    $word.DisplayAlerts = 0
    $doc = $word.Documents.Open($src, [ref]$false, [ref]$true)
    $doc.SaveAs2($dst, 16)
    Write-Output 'OK'
}} finally {{
    if ($doc) {{
        try {{ $doc.Close([ref]$false) | Out-Null }} catch {{}}
    }}
    if ($word) {{
        try {{ $word.Quit() | Out-Null }} catch {{}}
    }}
}}
""".strip()
    process = subprocess.run(
        ["powershell", "-NoProfile", "-Command", script],
        capture_output=True,
        check=False,
    )
    if target.exists():
        return True, "Converted via PowerShell Word COM."

    stderr = process.stderr.decode("utf-8", errors="ignore").strip()
    stdout = process.stdout.decode("utf-8", errors="ignore").strip()
    details = stderr or stdout or "Unknown PowerShell conversion failure."
    return False, f"PowerShell Word COM conversion failed: {details}"


def _convert_with_soffice(source: Path, target: Path, output_dir: Path) -> tuple[bool, str]:
    soffice = shutil.which("soffice")
    if not soffice:
        return False, "LibreOffice soffice not found in PATH."

    process = subprocess.run(
        [
            soffice,
            "--headless",
            "--convert-to",
            "docx",
            "--outdir",
            str(output_dir),
            str(source),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        stderr = process.stderr.strip() or process.stdout.strip()
        return False, f"soffice conversion failed: {stderr}"

    default_output = output_dir / f"{source.stem}.docx"
    if not default_output.exists():
        return False, "soffice returned success but output .docx not found."

    default_output.replace(target)
    return True, "Converted via LibreOffice soffice."
