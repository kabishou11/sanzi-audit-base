from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _to_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _to_int(raw: str | None, default: int) -> int:
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _to_float(raw: str | None, default: float) -> float:
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(slots=True)
class AppConfig:
    openai_api_key: str
    openai_base_url: str | None
    openai_model: str
    openai_reasoning_split: bool
    openai_timeout: int
    openai_max_retries: int
    openai_prompt_version: str
    aggregate_narrative_enabled: bool
    aggregate_narrative_max_subsections: int
    aggregate_narrative_target_length: int
    aggregate_narrative_temperature: float
    input_dir: Path
    output_dir: Path
    log_level: str
    doc_converter: str
    keep_intermediate: bool

    @property
    def extracted_dir(self) -> Path:
        return self.output_dir / "extracted_md"

    @property
    def per_report_dir(self) -> Path:
        return self.output_dir / "per_report_analysis"

    @property
    def aggregate_dir(self) -> Path:
        return self.output_dir / "aggregate"

    @property
    def cache_dir(self) -> Path:
        return self.output_dir / "cache"

    @property
    def logs_dir(self) -> Path:
        return self.output_dir / "logs"

    @property
    def llm_raw_dir(self) -> Path:
        return self.cache_dir / "llm_raw"

    def ensure_output_dirs(self) -> None:
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self.per_report_dir.mkdir(parents=True, exist_ok=True)
        self.aggregate_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.llm_raw_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


def load_config() -> AppConfig:
    load_dotenv(override=False)

    return AppConfig(
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_base_url=os.getenv("OPENAI_BASE_URL", "").strip() or None,
        openai_model=os.getenv("OPENAI_MODEL", "MiniMax-M2.7").strip(),
        openai_reasoning_split=_to_bool(os.getenv("OPENAI_REASONING_SPLIT"), True),
        openai_timeout=_to_int(os.getenv("OPENAI_TIMEOUT"), 120),
        openai_max_retries=_to_int(os.getenv("OPENAI_MAX_RETRIES"), 3),
        openai_prompt_version=os.getenv("OPENAI_PROMPT_VERSION", "v1").strip(),
        aggregate_narrative_enabled=_to_bool(os.getenv("AGGREGATE_NARRATIVE_ENABLED"), True),
        aggregate_narrative_max_subsections=_to_int(os.getenv("AGGREGATE_NARRATIVE_MAX_SUBSECTIONS"), 120),
        aggregate_narrative_target_length=_to_int(os.getenv("AGGREGATE_NARRATIVE_TARGET_LENGTH"), 4500),
        aggregate_narrative_temperature=_to_float(os.getenv("AGGREGATE_NARRATIVE_TEMPERATURE"), 0.3),
        input_dir=Path(os.getenv("INPUT_DIR", "input_reports/sample")).resolve(),
        output_dir=Path(os.getenv("OUTPUT_DIR", "outputs")).resolve(),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        doc_converter=os.getenv("DOC_CONVERTER", "auto").strip(),
        keep_intermediate=_to_bool(os.getenv("KEEP_INTERMEDIATE"), True),
    )
