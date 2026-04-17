from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, list, dict)):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    try:
        return json.loads(json.dumps(value, default=lambda x: getattr(x, "__dict__", str(x))))
    except Exception:
        return str(value)


def _extract_text_from_content_delta(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
                continue
            if hasattr(item, "text") and getattr(item, "text"):
                parts.append(str(getattr(item, "text")))
        return "".join(parts)
    return str(content)


def _extract_reasoning_text(reasoning_details: Any) -> str:
    if not reasoning_details:
        return ""
    if isinstance(reasoning_details, dict):
        return str(reasoning_details.get("text", ""))

    text_parts: list[str] = []
    for detail in reasoning_details:
        if isinstance(detail, str):
            text_parts.append(detail)
            continue
        if isinstance(detail, dict):
            if "text" in detail and detail["text"]:
                text_parts.append(str(detail["text"]))
            continue
        if hasattr(detail, "text") and getattr(detail, "text"):
            text_parts.append(str(getattr(detail, "text")))
    return "".join(text_parts)


def _merge_stream_text(current: str, incoming: str) -> str:
    if not incoming:
        return current
    if not current:
        return incoming
    # Some providers stream cumulative text, others stream deltas.
    if incoming.startswith(current):
        return incoming
    if current.endswith(incoming):
        return current
    return f"{current}{incoming}"


@dataclass(slots=True)
class LLMResponse:
    model: str
    response_text: str
    reasoning_text: str
    reasoning_details: list[Any] = field(default_factory=list)
    raw_events: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, Any] = field(default_factory=dict)


class OpenAICompatibleClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str | None = None,
        default_model: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "openai package is required for OpenAICompatibleClient. "
                "Install it before running LLM analysis."
            ) from exc

        self.default_model = default_model
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
        )

    def stream_chat(
        self,
        *,
        messages: list[dict[str, str]],
        model: str | None = None,
        extra_body: dict[str, Any] | None = None,
        reasoning_split: bool = True,
        stream: bool = True,
        include_raw_events: bool = True,
    ) -> LLMResponse:
        selected_model = model or self.default_model
        if not selected_model:
            raise ValueError("model is required when default_model is not configured")

        payload_extra_body = dict(extra_body or {})
        payload_extra_body.setdefault("reasoning_split", reasoning_split)

        response_text = ""
        reasoning_text = ""
        reasoning_details_all: list[Any] = []
        raw_events: list[dict[str, Any]] = []
        usage: dict[str, Any] = {}

        stream_obj = self._client.chat.completions.create(
            model=selected_model,
            messages=messages,
            extra_body=payload_extra_body,
            stream=stream,
        )

        if not stream:
            return self._build_nonstream_response(selected_model, stream_obj)

        try:
            for chunk in stream_obj:
                chunk_json = _to_jsonable(chunk)
                if include_raw_events and isinstance(chunk_json, dict):
                    raw_events.append(chunk_json)

                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                delta = getattr(choices[0], "delta", None)
                if delta is None:
                    continue

                content_piece = _extract_text_from_content_delta(getattr(delta, "content", None))
                response_text = _merge_stream_text(response_text, content_piece)

                details = getattr(delta, "reasoning_details", None)
                if details:
                    details_jsonable = _to_jsonable(details)
                    if isinstance(details_jsonable, list):
                        reasoning_details_all.extend(details_jsonable)
                    else:
                        reasoning_details_all.append(details_jsonable)
                    reasoning_piece = _extract_reasoning_text(details)
                    reasoning_text = _merge_stream_text(reasoning_text, reasoning_piece)

                if not details and hasattr(delta, "reasoning_content"):
                    fallback_piece = str(getattr(delta, "reasoning_content") or "")
                    reasoning_text = _merge_stream_text(reasoning_text, fallback_piece)
        except Exception as exc:  # noqa: BLE001
            fallback_response = self._client.chat.completions.create(
                model=selected_model,
                messages=messages,
                extra_body=payload_extra_body,
                stream=False,
            )
            fallback = self._build_nonstream_response(selected_model, fallback_response)
            fallback.raw_events.append(
                {
                    "fallback_reason": str(exc),
                    "fallback_mode": "nonstream_after_stream_failure",
                }
            )
            return fallback

        if raw_events:
            tail_usage = raw_events[-1].get("usage", {})
            if isinstance(tail_usage, dict):
                usage = tail_usage

        return LLMResponse(
            model=selected_model,
            response_text=response_text,
            reasoning_text=reasoning_text,
            reasoning_details=reasoning_details_all,
            raw_events=raw_events,
            usage=usage,
        )

    def _build_nonstream_response(self, selected_model: str, response_obj: Any) -> LLMResponse:
        data = _to_jsonable(response_obj)
        message = (
            data.get("choices", [{}])[0].get("message", {})
            if isinstance(data, dict)
            else {}
        )
        response_text = str(message.get("content", ""))
        reasoning_details = message.get("reasoning_details", []) if isinstance(message, dict) else []
        reasoning_text = _extract_reasoning_text(reasoning_details)
        if not reasoning_text and isinstance(message, dict):
            reasoning_text = str(message.get("reasoning_content", "") or "")
        usage = data.get("usage", {}) if isinstance(data, dict) else {}
        return LLMResponse(
            model=selected_model,
            response_text=response_text,
            reasoning_text=reasoning_text,
            reasoning_details=reasoning_details if isinstance(reasoning_details, list) else [reasoning_details],
            raw_events=[data] if isinstance(data, dict) else [],
            usage=usage if isinstance(usage, dict) else {},
        )
