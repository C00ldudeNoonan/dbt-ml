"""Standard text-preprocessing primitives.

Importable directly:

    from docbt.text import count_tokens, detect_language, text_stats

Or referenced from YAML as built-in transforms:

    transform:
      type: python
      module: docbt.text.transforms.text_stats
      options:
        text_field: body
"""
from .dedup import minhash_signature, near_duplicates
from .encoding import clean_encoding
from .language import detect_language
from .pii import PIIEntity, PIIError, detect_pii, redact_pii
from .stats import text_stats
from .tokens import count_tokens

__all__ = [
    "PIIEntity",
    "PIIError",
    "clean_encoding",
    "count_tokens",
    "detect_language",
    "detect_pii",
    "minhash_signature",
    "near_duplicates",
    "redact_pii",
    "text_stats",
]
