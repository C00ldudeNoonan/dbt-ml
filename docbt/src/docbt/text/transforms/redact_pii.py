"""Redact PII from a text column and (optionally) emit detected spans.

YAML:

    transform:
      type: python
      module: docbt.text.transforms.redact_pii
      options:
        text_field: body                  # source column (required)
        output_field: body_redacted       # redacted text (default: text_field, i.e. in-place)
        entities_field: pii_entities      # optional: JSON array of detected spans
        entities: [PHONE_NUMBER, EMAIL_ADDRESS, US_SSN]  # optional allow-list
        replacement: "[{type}]"           # default; "{type}" substituted at runtime
        score_threshold: 0.4              # default Presidio threshold
        spacy_model: en_core_web_sm
        language: en

First-time setup:
    python -m spacy download en_core_web_sm
"""
from __future__ import annotations

import json

import polars as pl

from ...transforms import TransformContext
from ..pii import redact_pii
from ._helpers import require_text_column, upstream_df


def run(deps: dict[str, pl.DataFrame], ctx: TransformContext) -> pl.DataFrame:
    df = upstream_df(deps)
    text_field = ctx.options.get("text_field", "text")
    out_field = ctx.options.get("output_field", text_field)
    entities_field = ctx.options.get("entities_field")
    entities = ctx.options.get("entities")
    replacement = ctx.options.get("replacement", "[{type}]")
    score_threshold = float(ctx.options.get("score_threshold", 0.4))
    spacy_model = ctx.options.get("spacy_model", "en_core_web_sm")
    language = ctx.options.get("language", "en")

    require_text_column(df, text_field)

    redacted_texts: list[str] = []
    detected_entities: list[str] = []
    for t in df[text_field].to_list():
        redacted, entities_found = redact_pii(
            t or "",
            entities=entities,
            language=language,
            score_threshold=score_threshold,
            replacement=replacement,
            spacy_model=spacy_model,
        )
        redacted_texts.append(redacted)
        detected_entities.append(
            json.dumps([e.to_dict() for e in entities_found])
        )

    out = df.with_columns(pl.Series(out_field, redacted_texts))
    if entities_field:
        out = out.with_columns(pl.Series(entities_field, detected_entities))
    return out
