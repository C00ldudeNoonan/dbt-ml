from __future__ import annotations

from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from .base import BaseBackend, ExtractionResult
from .registry import register


@register
class HtmlBackend(BaseBackend):
    """Read .html files via BeautifulSoup.

    Options:
        text_field:        Field name for the plain-text body (default "text").
        include_text:      Emit body text with tags stripped (default True).
        selectors:         dict of {field_name: css_selector}. First match's text
                           per selector is emitted; missing selectors yield None
                           with a warning.
        include_meta:      Emit a `meta` dict of <meta> name→content pairs.
        include_opengraph: Emit `og` dict of OpenGraph properties (og:*).
        include_links:     Emit `links` as a list of href strings.
        parser:            "html.parser" (default, stdlib) or "lxml" if installed.
    """

    def name(self) -> str:
        return "html"

    def supported_formats(self) -> list[str]:
        return [".html", ".htm"]

    def extract(self, path: Path, options: dict[str, Any]) -> ExtractionResult:
        parser = options.get("parser", "html.parser")
        soup = BeautifulSoup(path.read_text(), parser)

        warnings: list[str] = []
        fields: dict[str, Any] = {}

        if options.get("include_text", True):
            text_field = options.get("text_field", "text")
            body = soup.body or soup
            fields[text_field] = body.get_text(separator="\n", strip=True)

        selectors = options.get("selectors") or {}
        for field_name, selector in selectors.items():
            match = soup.select_one(selector)
            if match is None:
                warnings.append(
                    f"selector {selector!r} for field '{field_name}' matched nothing"
                )
                fields[field_name] = None
            else:
                fields[field_name] = match.get_text(strip=True)

        if options.get("include_meta", False):
            meta: dict[str, str] = {}
            for tag in soup.find_all("meta"):
                key = tag.get("name") or tag.get("property")
                content = tag.get("content")
                if key and content:
                    meta[str(key)] = str(content)
            fields["meta"] = meta

        if options.get("include_opengraph", False):
            og: dict[str, str] = {}
            for tag in soup.find_all("meta"):
                prop = tag.get("property")
                if prop and isinstance(prop, str) and prop.startswith("og:"):
                    og[prop[3:]] = str(tag.get("content") or "")
            fields["og"] = og

        if options.get("include_links", False):
            fields["links"] = [
                str(a.get("href"))
                for a in soup.find_all("a", href=True)
            ]

        return ExtractionResult(fields=fields, warnings=warnings)
