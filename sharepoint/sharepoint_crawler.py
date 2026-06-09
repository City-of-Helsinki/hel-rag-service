"""
SharePoint crawler: fetches all pages and files from a SharePoint site
and converts them to Markdown documents.

File conversion is delegated to :class:`MarkItDownDocumentConverter`
(see ``document_converter.py``).
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

from sharepoint_client import SharePointClient
from content_processor import HtmlToMarkdownConverter
from document_converter import BaseDocumentConverter, MarkItDownDocumentConverter

logger = logging.getLogger(__name__)

_DEFAULT_EXTENSIONS = {".pdf", ".docx", ".pptx", ".xlsx"}

# Override via env: SHAREPOINT_FILE_EXTENSIONS=".pdf,.docx"
# Falls back to _DEFAULT_EXTENSIONS when the variable is unset or empty.
def _load_extensions() -> set[str]:
    raw = os.environ.get("SHAREPOINT_FILE_EXTENSIONS", "").strip()
    if not raw:
        return _DEFAULT_EXTENSIONS
    return {ext.strip().lower() for ext in raw.split(",") if ext.strip()}

SUPPORTED_FILE_EXTENSIONS: set[str] = _load_extensions()


def slugify(text: str | None) -> str:
    """Convert a title or filename stem to a URL/filesystem-safe slug.

    Strips non-word characters, collapses whitespace and hyphens, and
    lowercases the result.  Returns ``"untitled"`` for empty or ``None`` input.
    """
    if not text:
        return "untitled"
    text = re.sub(r"[^\w\s-]", "", text.lower())
    return re.sub(r"[\s_-]+", "-", text).strip("-") or "untitled"


class SharePointCrawler:
    """Crawls a SharePoint site and produces Markdown files for all content."""

    def __init__(
        self,
        client: SharePointClient,
        output_dir: Path,
        file_extensions: set[str] | None = None,
        page_paths: list[str] | None = None,
        converter: BaseDocumentConverter | None = None,
    ) -> None:
        """Initialise the crawler.

        Args:
            client: Authenticated :class:`~sharepoint_client.SharePointClient`
                used for all Graph API calls.
            output_dir: Root output directory; ``pages/`` and ``files/``
                sub-directories are created automatically.  Accepts both
                ``str`` and ``Path``.
            file_extensions: Set of file extensions to process (e.g., {".pdf", ".docx"}).
                Defaults to SUPPORTED_FILE_EXTENSIONS if not provided.
            page_paths: Optional list of page paths to filter. If provided, only
                pages matching these paths will be crawled. Omit to crawl all pages.
            converter: Document converter backend to use for binary files.
                Defaults to :class:`~document_converter.MarkItDownDocumentConverter`.
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_extensions = file_extensions if file_extensions is not None else SUPPORTED_FILE_EXTENSIONS
        self.page_paths = page_paths
        self._converter: BaseDocumentConverter = converter or MarkItDownDocumentConverter()
        self._html_converter = HtmlToMarkdownConverter()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _save_metadata(md_path: Path, metadata: dict) -> None:
        """Write a JSON sidecar file next to a Markdown document.

        The file uses the same stem as ``md_path`` with a ``.json`` extension,
        e.g. ``my-document.md`` → ``my-document.json``.

        Args:
            md_path: Path of the already-saved Markdown file.
            metadata: Dict to serialise; typically contains ``name``,
                ``webUrl``, ``lastModifiedDateTime``, and ``filePath``.
        """
        json_path = md_path.with_suffix(".json")
        json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.debug("Saved metadata: %s", json_path)

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    def crawl_pages(self) -> int:
        """Fetch every SitePage and write it as ``<output_dir>/pages/<slug>.md``.

        A JSON sidecar with page metadata is written alongside each Markdown
        file.  Pages without an ``"id"`` field or with empty content are
        skipped.  When two pages produce the same slug the page ID is appended
        to prevent overwrites.
        
        If ``page_paths`` was provided to the constructor, only pages matching
        those paths will be crawled.

        Returns:
            Number of pages successfully saved.
        """
        # Retrieve pages - filtered or all
        if self.page_paths:
            pages = self.client.get_pages_by_paths(self.page_paths)
            logger.info("Filtered to %d page(s) matching configured paths.", len(pages))
        else:
            pages = self.client.get_all_pages()
        saved = 0

        for page in pages:
            # Ensure page_id, skip if missing
            page_id: str | None = page.get("id")
            if not page_id:
                logger.warning("Skipping page with missing 'id': %s", page)
                continue
            title: str = page.get("title") or page.get("name") or page_id

            # Fetch raw HTML fragments assembled from the page's web parts
            try:
                html = self.client.get_page_content_html(page_id)
            except Exception:
                logger.exception("Failed to fetch page '%s' (%s).", title, page_id)
                continue

            if not html:
                logger.warning("Empty content for page '%s', skipping.", title)
                continue

            # Convert HTML to Markdown via MarkItDown
            text = self._html_converter.convert(html, source_label=title)
            if not text:
                logger.warning("Markdown conversion produced no output for page '%s', skipping.", title)
                continue

            # Resolve output path; append page ID on collision to prevent overwrites
            slug = slugify(title)
            out_path = self.output_dir / "pages" / f"{slug}.md"
            if out_path.exists():
                # Avoid overwriting when multiple pages share the same slug
                out_path = out_path.with_name(f"{slug}-{page_id}.md")
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Write Markdown and JSON sidecar
            out_path.write_text(text, encoding="utf-8")
            self._save_metadata(out_path, {
                "name": page.get("name", ""),
                "webUrl": page.get("webUrl", ""),
                "lastModifiedDateTime": page.get("lastModifiedDateTime", ""),
                "filePath": "",
            })
            logger.info("Saved page: %s", out_path)
            saved += 1

        logger.info("Pages: %d / %d converted.", saved, len(pages))
        return saved

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    def _convert_file(self, raw: bytes, filename: str) -> str | None:
        """Convert raw file bytes to Markdown via the configured converter backend.

        Args:
            raw: Raw file content.  Empty bytes return ``None``.
            filename: Original filename including extension; the converter uses
                the extension to pick the correct parser (e.g. ``"report.pdf"``).

        Returns:
            Cleaned Markdown string, or ``None`` on empty input or conversion failure.
        """
        return self._converter.convert_bytes(raw, filename)

    def crawl_files(self) -> int:
        """Download all supported files from every drive and convert to Markdown.

        Supported extensions are controlled by the ``file_extensions`` parameter
        passed to the constructor.  Each file is saved to
        ``<output_dir>/files/<slug>.md`` with a JSON sidecar.  Files missing a
        ``"driveId"`` are skipped.  Existing files are overwritten.

        Returns:
            Number of files successfully saved.
        """
        # Retrieve all matching files across every document library
        items = self.client.get_all_files(extensions=self.file_extensions)
        saved = 0

        for item in items:
            name: str = item.get("name", item.get("id", "unknown"))
            # driveId is injected by get_all_files; skip if somehow absent
            drive_id: str | None = item.get("driveId")
            if not drive_id:
                logger.warning("Skipping item '%s': missing 'driveId'.", name)
                continue

            # Download raw bytes from the drive
            try:
                raw = self.client.download_drive_item(drive_id, item["id"])
            except Exception:
                logger.exception("Failed to download '%s'.", name)
                continue

            # Convert to Markdown via MarkItDown
            markdown = self._convert_file(raw, name)
            if not markdown:
                continue

            # Resolve output path; overwrite if already exists
            stem = slugify(Path(name).stem)
            out_path = self.output_dir / "files" / f"{stem}.md"
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Write Markdown and JSON sidecar
            out_path.write_text(markdown, encoding="utf-8")
            self._save_metadata(out_path, self.client.extract_file_metadata(item))
            logger.info("Saved file: %s → %s", name, out_path)
            saved += 1

        logger.info("Files: %d / %d converted.", saved, len(items))
        return saved

    # ------------------------------------------------------------------
    # Full crawl
    # ------------------------------------------------------------------

    def crawl_all(self) -> dict[str, int]:
        """Run a full crawl of all site pages and drive files.

        Returns:
            ``{"pages": <int>, "files": <int>}`` with counts of saved items.
        """
        logger.info("Starting full SharePoint crawl → %s", self.output_dir)
        results = {
            "pages": self.crawl_pages(),
            "files": self.crawl_files(),
        }
        logger.info("Crawl complete: %s", results)
        return results

