"""
Content conversion service for converting HTML and PDF files to Markdown.

There's implemented two classes:
- HTMLSanitizer: Sanitizes and fixes malformed HTML content. Primarily removes backslashes and if needed, uses html5lib for parsing and fixing HTML structure.
- MarkdownConverter: Converts sanitized HTML and PDF files to Markdown format and cleans up the content for further processing.
"""

import os
import re
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET

import html5lib
from bs4 import BeautifulSoup
from docling.document_converter import DocumentConverter

from app.core import get_logger

logger = get_logger(__name__)


class HTMLSanitizer:
    """Service for sanitizing and fixing malformed HTML."""

    def __init__(self):
        """Initialize HTML sanitizer with html5lib parser."""
        self.parser = html5lib.HTMLParser(
            tree=html5lib.getTreeBuilder("etree"), namespaceHTMLElements=False
        )

    def _remove_backslashes(self, html_content: str) -> str:
        """
        Remove all backslashes from HTML content.

        Args:
            html_content: HTML content with potential backslashes

        Returns:
            HTML content without backslashes
        """
        return html_content.replace("\\", "")

    def _is_valid_html(self, html_content: str) -> bool:
        """
        Check if HTML is well-formed and valid.

        Args:
            html_content: HTML content to validate

        Returns:
            True if HTML is valid, False otherwise
        """
        try:
            # Try parsing with BeautifulSoup
            soup = BeautifulSoup(html_content, "html.parser")
            # If we can convert it back to string without errors, it's valid
            _ = str(soup)

            # Additional check: look for common malformation indicators
            # Check for unclosed tags or obvious malformations
            if "</>" in html_content or "< >" in html_content:
                return False

            logger.info("HTML validation successful")
            return True
        except Exception as e:
            logger.debug(f"HTML validation failed: {e}")
            return False

    def _sanitize_with_library(self, html_content: str) -> str:
        """
        Sanitize HTML using external libraries.

        Args:
            html_content: HTML content to sanitize

        Returns:
            Sanitized HTML string
        """
        try:
            # Delete image tags to avoid conversion issues
            soup = BeautifulSoup(html_content, "html.parser")
            for img in soup.find_all("img"):
                img.decompose()
            html_content = str(soup)

            # Parse HTML with html5lib (most lenient parser)
            doc = self.parser.parse(html_content)

            # Serialize back to HTML string
            sanitized = ET.tostring(doc, encoding="unicode", method="html")

            logger.info("HTML library sanitization completed successfully")
            return sanitized

        except Exception as e:
            logger.error(f"Error in library sanitization: {e}")
            # Return original content if sanitization fails
            return html_content

    def sanitize(self, html_content: str) -> str:
        """
        Sanitize and fix malformed HTML.

        First removes backslashes, then validates HTML.
        Only uses external library sanitization if HTML is still invalid.

        Args:
            html_content: Raw HTML content to sanitize

        Returns:
            Sanitized and well-formed HTML string
        """
        if not html_content or not html_content.strip():
            logger.warning("Empty HTML content provided for sanitization")
            return ""

        try:
            # Step 1: Remove all backslashes
            logger.info("Removing backslashes from HTML")
            cleaned_html = self._remove_backslashes(html_content)

            # Step 2: Check if HTML is now valid
            if self._is_valid_html(cleaned_html):
                logger.info("HTML is valid after backslash removal")
                # Remove image tags for cleaner conversion
                soup = BeautifulSoup(cleaned_html, "html.parser")
                for img in soup.find_all("img"):
                    img.decompose()
                return str(soup)

            # Step 3: If still invalid, use external library sanitization
            logger.info("HTML still invalid, using library sanitization")
            return self._sanitize_with_library(cleaned_html)

        except Exception as e:
            logger.error(f"Error sanitizing HTML: {e}")
            # Return original content if sanitization fails
            return html_content


class MarkdownConverter:
    """Service for converting HTML to Markdown."""

    def __init__(self):
        """Initialize Markdown converter."""
        self.converter = DocumentConverter()
        self.sanitizer = HTMLSanitizer()

    def convert(self, html_content: str, preserve_structure: bool = True) -> str:
        """
        Convert HTML content to Markdown.

        Args:
            html_content: HTML content to convert
            preserve_structure: Whether to preserve document structure

        Returns:
            Markdown formatted text
        """
        if not html_content or not html_content.strip():
            logger.warning("Empty HTML content provided for conversion")
            return ""

        try:
            # First sanitize the HTML
            sanitized_html = self.sanitizer.sanitize(html_content)

            # Create a temporary file of sanitized HTML for conversion
            temp_fd = None
            temp_path = None
            try:
                # Create temporary file with .html extension
                temp_fd, temp_path = tempfile.mkstemp(suffix=".html", text=True)

                # Write sanitized HTML to temporary file
                with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                    f.write(sanitized_html)
                    temp_fd = None  # File descriptor is now closed

                # Convert to Markdown using Docling
                result = self.converter.convert(temp_path)

                # Extract markdown content from Docling result
                markdown_text = result.document.export_to_markdown()

                # Clean up the markdown
                markdown_text = self._clean_markdown(markdown_text)

                if not self._validate_markdown(markdown_text):
                    logger.warning("Converted Markdown is invalid")
                    return ""

                logger.debug(
                    f"HTML to Markdown conversion completed. Length: {len(markdown_text)} chars"
                )
                return markdown_text

            finally:
                # Clean up temporary file
                if temp_fd is not None:
                    try:
                        os.close(temp_fd)
                    except Exception:
                        pass
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                        logger.debug(f"Temporary file {temp_path} cleaned up")
                    except Exception as e:
                        logger.warning(f"Failed to clean up temporary file {temp_path}: {e}")

        except Exception as e:
            logger.error(f"Error converting HTML to Markdown: {e}")
            # Return empty string on conversion failure
            return ""

    def _clean_markdown(self, markdown_text: str) -> str:
        """
        Clean and normalize Markdown text.

        Args:
            markdown_text: Raw Markdown text

        Returns:
            Cleaned Markdown text
        """
        if not markdown_text:
            return ""

        # Remove excessive blank lines (more than 2 consecutive)
        cleaned = re.sub(r"\n{3,}", "\n\n", markdown_text)

        # Remove trailing whitespace from lines
        cleaned = "\n".join(line.rstrip() for line in cleaned.split("\n"))

        # Ensure document ends with single newline
        cleaned = cleaned.strip() + "\n"

        return cleaned

    def _validate_markdown(self, markdown_text: str) -> bool:
        """
        Validate Markdown text to ensure it is not empty or malformed.

        Args:
            markdown_text: Markdown text to validate
        Returns:
            True if valid, False otherwise
        """
        try:
            if not markdown_text or not markdown_text.strip():
                logger.warning("Converted Markdown text is empty or whitespace only")
                return False
            if "<html" in markdown_text.lower() or "</html>" in markdown_text.lower():
                logger.warning("Converted Markdown text contains HTML tags, indicating malformed content")
                return False
            # Check if text is printable
            if not all(c.isprintable() or c.isspace() for c in markdown_text):
                logger.warning("Converted Markdown text contains non-printable characters")
                return False
            return True
        except Exception as e:
            logger.error(f"Error validating Markdown text: {e}")
            return False

    def convert_attachment_file(self, file_path: Path) -> str:
        """
        Convert attachment file to Markdown.

        Supports PDF, DOCX, and other formats supported by Docling.

        Args:
            file_path: Path to attachment file

        Returns:
            Markdown formatted text, or empty string on failure
        """
        if not file_path or not file_path.exists():
            logger.warning(f"Attachment file does not exist: {file_path}")
            return ""

        try:
            logger.info(f"Converting attachment file to Markdown: {file_path}")

            # Convert using Docling
            result = self.converter.convert(str(file_path))

            # Extract markdown content from Docling result
            markdown_text = result.document.export_to_markdown()

            # Clean up the markdown
            markdown_text = self._clean_markdown(markdown_text)

            if not self._validate_markdown(markdown_text):
                logger.warning(f"Converted Markdown from attachment {file_path} is invalid")
                return ""

            logger.info(
                f"Attachment conversion completed. Length: {len(markdown_text)} chars"
            )
            return markdown_text

        except Exception as e:
            logger.error(f"Error converting attachment file {file_path}: {e}", exc_info=True)
            return ""


def convert_decision_content(html_content: str) -> str:
    """
    Convert decision document HTML content to Markdown.

    This is the main entry point for HTML to Markdown conversion.

    Args:
        html_content: HTML content from decision document

    Returns:
        Clean Markdown text ready for chunking
    """
    converter = MarkdownConverter()
    return converter.convert(html_content)


def convert_attachment_content(file_path: Path) -> str:
    """
    Convert attachment file content to Markdown.

    This is the main entry point for attachment file to Markdown conversion.

    Args:
        file_path: Path to attachment file

    Returns:
        Clean Markdown text ready for chunking
    """
    converter = MarkdownConverter()
    return converter.convert_attachment_file(file_path)
