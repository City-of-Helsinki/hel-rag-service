"""
Document converter abstractions for SharePoint file processing.

Provides a conversion layer using MarkItDown as the backend, with shared
post-processing logic (NaN / artefact cleanup) defined in
:class:`BaseDocumentConverter`.

Usage::

    converter = MarkItDownDocumentConverter()
    markdown = converter.convert_bytes(raw_bytes, "report.xlsx")
"""

from __future__ import annotations

import abc
import io
import logging
import re
import tempfile
import os

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class BaseDocumentConverter(abc.ABC):
    """Abstract base for file-to-Markdown converters.

    Sub-classes must implement :meth:`_convert_bytes_impl`.  The public
    :meth:`convert_bytes` method wraps that with shared post-processing
    such as NaN / empty-cell cleanup.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert_bytes(self, raw: bytes, filename: str) -> str | None:
        """Convert *raw* file bytes to cleaned Markdown.

        Args:
            raw: Raw file content.
            filename: Original filename including extension (e.g. ``"data.xlsx"``).

        Returns:
            Cleaned Markdown string, or ``None`` on empty input / failure.
        """
        if not raw:
            logger.warning("Skipping empty file content for '%s'.", filename)
            return None

        markdown = self._convert_bytes_impl(raw, filename)
        if not markdown:
            return None

        return self._post_process(markdown)

    # ------------------------------------------------------------------
    # Abstract implementation
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def _convert_bytes_impl(self, raw: bytes, filename: str) -> str | None:
        """Backend-specific conversion.  Must be overridden by sub-classes.

        Args:
            raw: Non-empty raw file bytes.
            filename: Original filename with extension.

        Returns:
            Raw Markdown string (before shared post-processing), or ``None``.
        """

    # ------------------------------------------------------------------
    # Shared post-processing
    # ------------------------------------------------------------------

    def _post_process(self, content: str) -> str:
        """Apply shared Markdown cleanup steps.

        Currently handles:
        - NaN / None / empty-cell artefacts from spreadsheet conversion
        - Trailing whitespace / redundant blank lines in table rows
        - Consecutive empty table cells (``| | | |``) collapsed to single cell
        """
        content = self._clean_nan(content)
        content = self._clean_empty_table_columns(content)
        return content

    @staticmethod
    def _clean_nan(content: str) -> str:
        """Replace NaN / None cell values left by pandas-based converters."""
        # Full-cell matches (surrounded by pipes and optional spaces)
        for token in ("nan", "NaN", "None"):
            content = content.replace(f"| {token} |", "| |")
            content = content.replace(f"|{token}|", "||")
            # Edge: token at start / end of row
            content = content.replace(f"| {token}\n", "| \n")
            content = content.replace(f"{token} |", " |")
        return content

    @staticmethod
    def _clean_empty_table_columns(content: str) -> str:
        """Collapse runs of empty Markdown table cells on a single row.

        Rows like ``| Heading | | | | |`` become ``| Heading |``.
        Separator rows (``| --- |``) are left untouched.
        """
        lines = content.splitlines(keepends=True)
        result = []
        for line in lines:
            stripped = line.rstrip()
            # Only process lines that look like a table row (start & end with |)
            # and are NOT separator rows
            if (
                stripped.startswith("|")
                and stripped.endswith("|")
                and not re.match(r"^\|[-| :]+\|$", stripped)
            ):
                # Remove trailing empty cells: | content | | | | -> | content |
                cleaned = re.sub(r"(\| *)+$", "|", stripped)
                # Ensure it still ends with |
                if not cleaned.endswith("|"):
                    cleaned += "|"
                result.append(cleaned + "\n")
            else:
                result.append(line)
        return "".join(result)


# ---------------------------------------------------------------------------
# MarkItDown backend
# ---------------------------------------------------------------------------

class MarkItDownDocumentConverter(BaseDocumentConverter):
    """Converts files to Markdown using
    `markitdown <https://github.com/microsoft/markitdown>`_.

    Excel files are pre-processed with pandas to trim empty columns and remove
    NaN values before conversion — mirroring the logic in ``convert_to_md.py``.
    """

    def __init__(self) -> None:
        from markitdown import MarkItDown
        self._converter = MarkItDown()

    def _convert_bytes_impl(self, raw: bytes, filename: str) -> str | None:
        ext = os.path.splitext(filename)[1].lower()
        try:
            if ext in (".xlsx", ".xls"):
                return self._convert_excel(raw, filename)
            else:
                return self._convert_generic(raw, filename)
        except Exception:
            logger.exception("MarkItDown failed to convert '%s'.", filename)
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _convert_generic(self, raw: bytes, filename: str) -> str | None:
        """Write bytes to a temp file and convert with MarkItDown."""
        suffix = os.path.splitext(filename)[1] or ".bin"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        try:
            result = self._converter.convert(tmp_path)
            return result.text_content
        finally:
            os.unlink(tmp_path)

    def _convert_excel(self, raw: bytes, filename: str) -> str | None:
        """Pre-process Excel bytes with pandas, then convert with MarkItDown."""
        try:
            import pandas as pd
        except ImportError:
            logger.warning("pandas not installed — converting Excel without pre-processing.")
            return self._convert_generic(raw, filename)

        try:
            excel_file = pd.ExcelFile(io.BytesIO(raw))
        except Exception:
            logger.exception("Could not open Excel file '%s' with pandas.", filename)
            return self._convert_generic(raw, filename)

        cleaned_sheets: dict[str, "pd.DataFrame"] = {}
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(io.BytesIO(raw), sheet_name=sheet_name)
            df = df.fillna("")
            df = self._trim_excel_columns(df)
            df = df.loc[(df != "").any(axis=1)]  # drop fully empty rows
            # Fix column names
            df.columns = [
                "" if pd.isna(c) or str(c).strip() == "nan" else str(c).strip()
                for c in df.columns
            ]
            cleaned_sheets[sheet_name] = df

        # Write cleaned workbook to a temp file for MarkItDown
        suffix = os.path.splitext(filename)[1] or ".xlsx"
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(tmp_fd)
        try:
            with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
                for sheet_name, df in cleaned_sheets.items():
                    df.fillna("").replace([None], "").to_excel(
                        writer, sheet_name=sheet_name, index=False
                    )
            result = self._converter.convert(tmp_path)
            return result.text_content
        finally:
            os.unlink(tmp_path)

    @staticmethod
    def _trim_excel_columns(df: "pd.DataFrame") -> "pd.DataFrame":
        """Remove trailing columns that are sparsely populated.

        Mirrors the ``smart_column_detection`` logic from ``convert_to_md.py``:
        computes average filled-cell count per row and drops columns that are
        used by fewer than 50 % of the average.
        """
        if df.empty:
            return df

        max_columns = min(50, len(df.columns))
        column_usage = [0] * max_columns
        row_lengths = []

        for _, row in df.iterrows():
            cells = [str(c) if c != "" else "" for c in row[:max_columns]]
            length = sum(1 for c in cells if c.strip())
            row_lengths.append(length)
            for i in range(min(length, max_columns)):
                column_usage[i] += 1

        if not row_lengths:
            return df

        avg = sum(row_lengths) / len(row_lengths)
        cutoff = max(1, int(avg * 0.5))

        last_valid = 0
        for i in range(max_columns):
            if column_usage[i] >= cutoff:
                last_valid = i + 1

        return df.iloc[:, : max(last_valid, 1)]

