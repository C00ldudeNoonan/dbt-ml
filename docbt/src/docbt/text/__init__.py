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
from .stats import text_stats
from .tokens import count_tokens

__all__ = [
    "clean_encoding",
    "count_tokens",
    "detect_language",
    "minhash_signature",
    "near_duplicates",
    "text_stats",
]
