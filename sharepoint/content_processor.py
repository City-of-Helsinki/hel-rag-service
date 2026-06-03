"""
HTML-to-Markdown converter for SharePoint page content.

SharePoint pages arrive as raw HTML strings (web part ``innerHtml`` fragments
assembled by :meth:`~sharepoint_client.SharePointClient.get_page_content_html`).
This module converts those strings to structured Markdown using docling's
HTML pipeline, producing output consistent with what the file crawler
generates for DOCX/PDF/PPTX documents.
"""

from __future__ import annotations

import logging
import tempfile
import os

from docling.document_converter import DocumentConverter, InputFormat
from docling.document_converter import ConversionResult

logger = logging.getLogger(__name__)


class HtmlToMarkdownConverter:
    """Converts raw HTML strings to Markdown using docling's HTML pipeline.

    Intended for SharePoint site-page content where the Graph API returns
    HTML fragments rather than downloadable files.  For binary files (PDF,
    DOCX, etc.) use :class:`~sharepoint_crawler.SharePointCrawler` directly.

    A single :class:`~docling.document_converter.DocumentConverter` instance
    is reused across calls to avoid repeated model-loading overhead.
    """

    def __init__(self) -> None:
        # Initialise once — converter setup can be expensive
        self._converter = DocumentConverter(
            allowed_formats=[InputFormat.HTML],
        )

    def convert(self, html: str, source_label: str = "page") -> str:
        """Convert an HTML string to Markdown.

        Docling requires a file path as input, so the HTML is written to a
        temporary file, converted, and the file is deleted afterwards.

        Args:
            html: Raw HTML string to convert.
            source_label: Human-readable label used in log messages to
                identify which page or source is being processed.

        Returns:
            Markdown string produced by docling, or an empty string if the
            input is blank or conversion fails.
        """
        if not html or not html.strip():
            logger.warning("Empty HTML received for '%s', skipping.", source_label)
            return ""

        # Write to a temp file — docling requires a file path, not a string
        with tempfile.NamedTemporaryFile(
            suffix=".html", mode="w", encoding="utf-8", delete=False
        ) as tmp:
            tmp.write(html)
            tmp_path = tmp.name

        try:
            result: ConversionResult = self._converter.convert(tmp_path)
            markdown = result.document.export_to_markdown()
            logger.debug(
                "Converted '%s': %d chars of Markdown.", source_label, len(markdown)
            )
            return markdown
        except Exception as exc:
            logger.error("docling conversion failed for '%s': %s", source_label, exc)
            return ""
        finally:
            os.unlink(tmp_path)

