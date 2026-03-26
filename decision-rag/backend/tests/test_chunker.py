"""
Tests for text chunking service.
"""

import pytest

from app.services.chunker import DocumentChunk, ParagraphChunker


class TestParagraphChunker:
    """Tests for ParagraphChunker."""

    @pytest.fixture
    def chunker(self):
        """Create a chunker instance."""
        return ParagraphChunker(target_tokens=100, min_tokens=50, max_tokens=150, overlap_tokens=20)

    def test_count_tokens(self, chunker):
        """Test token counting."""
        text = "This is a test sentence."
        count = chunker.count_tokens(text)
        assert count > 0

    def test_chunk_empty_text(self, chunker):
        """Test chunking empty text."""
        chunks = chunker.chunk_text("", "test_id")
        assert len(chunks) == 0

    def test_chunk_short_text(self, chunker):
        """Test chunking short text."""
        text = "This is a short text that should fit in one chunk."
        chunks = chunker.chunk_text(text, "test_id")
        assert len(chunks) >= 1
        assert chunks[0].native_id == "test_id"
        assert chunks[0].chunk_index == 0

    def test_chunk_with_paragraphs(self, chunker):
        """Test chunking text with multiple paragraphs."""
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        chunks = chunker.chunk_text(text, "test_id")
        assert len(chunks) >= 1
        for chunk in chunks:
            assert chunk.token_count > 0

    def test_chunk_metadata(self, chunker):
        """Test that metadata is preserved in chunks."""
        text = "Test content"
        metadata = {"title": "Test Title", "date": "2024-01-01"}
        chunks = chunker.chunk_text(text, "test_id", metadata)
        assert len(chunks) >= 1
        assert chunks[0].metadata["title"] == "Test Title"

    def test_chunk_token_limits(self, chunker):
        """Test that chunks respect token limits."""
        # Create a long text
        text = " ".join(["word"] * 500)
        chunks = chunker.chunk_text(text, "test_id")
        for chunk in chunks:
            assert chunk.token_count <= chunker.max_tokens

    def test_split_paragraphs(self, chunker):
        """Test paragraph splitting."""
        text = "Para 1\n\nPara 2\n\nPara 3"
        paragraphs = chunker._split_paragraphs(text)
        assert len(paragraphs) == 3

    def test_chunk_id_generation(self, chunker):
        """Test chunk ID generation."""
        text = "Test content"
        chunks = chunker.chunk_text(text, "doc_123")
        assert chunks[0].chunk_id == "doc_123_chunk_0"


class TestDocumentChunk:
    """Tests for DocumentChunk dataclass."""

    def test_create_chunk(self):
        """Test creating a DocumentChunk."""
        chunk = DocumentChunk(
            chunk_id="test_chunk_0",
            native_id="test_doc",
            chunk_index=0,
            text="Test text",
            token_count=10,
            metadata={"key": "value"},
        )
        assert chunk.chunk_id == "test_chunk_0"
        assert chunk.native_id == "test_doc"
        assert chunk.chunk_index == 0
        assert chunk.text == "Test text"
        assert chunk.token_count == 10
        assert chunk.metadata["key"] == "value"


class TestMetadataHeaderGeneration:
    """Tests for metadata header generation in chunks."""

    @pytest.fixture
    def chunker_with_metadata(self):
        """Create a chunker instance with metadata embedding enabled."""
        return ParagraphChunker(
            target_tokens=100,
            min_tokens=50,
            max_tokens=150,
            overlap_tokens=20,
            header_overhead_tokens=75,
            embed_metadata=True,
        )

    @pytest.fixture
    def chunker_without_metadata(self):
        """Create a chunker instance with metadata embedding disabled."""
        return ParagraphChunker(
            target_tokens=100,
            min_tokens=50,
            max_tokens=150,
            overlap_tokens=20,
            embed_metadata=False,
        )

    def test_metadata_header_generation_decision_chunk(self, chunker_with_metadata):
        """Test header generation for decision chunks with full metadata."""
        metadata = {
            "title": "Kaupunginhallituksen päätös",
            "native_id": "HEL-2024-001234",
            "case_id": "HEL 2024-000123",
            "classification_title": "Hallinto",
            "classification_code": "01 02 03",
            "organization_name": "Kaupunginhallitus",
            "date_decision": "2024-01-15T10:00:00.000",
            "section": "10",
            "is_attachment": False,
        }

        header = chunker_with_metadata._generate_metadata_header(metadata, is_attachment=False)

        # Verify all required fields are present
        assert "Dokumentin konteksti" in header
        assert "Kaupunginhallituksen päätös" in header
        assert "HEL-2024-001234" in header
        assert "Hallinto" in header
        assert "01 02 03" in header
        assert "Kaupunginhallitus" in header
        assert "2024-01-15" in header
        assert "HEL 2024-000123" in header
        assert "Pykälä: 10" in header
        assert header.startswith("---")
        assert header.endswith("---")

        # Verify token count
        token_count = chunker_with_metadata.count_tokens(header)
        assert token_count > 0
        assert token_count < 200  # Reasonable upper limit

    def test_metadata_header_generation_attachment_chunk(self, chunker_with_metadata):
        """Test header generation for attachment chunks."""
        metadata = {
            "title": "Kaupunginhallituksen päätös",
            "native_id": "HEL-2024-001234_att_1",
            "decision_native_id": "HEL-2024-001234",
            "attachment_title": "Liite 1: Budjetti",
            "attachment_url": "https://example.com/file.pdf",
            "is_attachment": True,
        }

        header = chunker_with_metadata._generate_metadata_header(metadata, is_attachment=True)

        # Verify attachment-specific fields
        assert "Liite 1: Budjetti" in header
        assert "https://example.com/file.pdf" in header
        assert "Päätöksen ID: HEL-2024-001234" in header

    def test_metadata_header_with_minimal_metadata(self, chunker_with_metadata):
        """Test header generation with minimal metadata."""
        metadata = {
            "title": "Test Decision",
            "native_id": "TEST-001",
            "is_attachment": False,
        }

        header = chunker_with_metadata._generate_metadata_header(metadata, is_attachment=False)

        assert "Test Decision" in header
        assert "TEST-001" in header
        # Should not crash with missing optional fields

    def test_metadata_header_disabled(self, chunker_without_metadata):
        """Test that header generation is disabled when embed_metadata=False."""
        metadata = {
            "title": "Test Decision",
            "native_id": "TEST-001",
        }

        header = chunker_without_metadata._generate_metadata_header(
            metadata, is_attachment=False
        )

        assert header == ""

    def test_chunk_with_metadata_header(self, chunker_with_metadata):
        """Test that chunks include metadata headers in text."""
        text = "This is the main content of the decision."
        metadata = {
            "title": "Test Decision",
            "native_id": "TEST-001",
            "case_id": "TEST-CASE-001",
            "is_attachment": False,
        }

        chunks = chunker_with_metadata.chunk_text(text, "TEST-001", metadata)

        assert len(chunks) >= 1
        # Check that chunk text includes header
        assert "Dokumentin konteksti" in chunks[0].text
        assert "Test Decision" in chunks[0].text
        assert "This is the main content" in chunks[0].text

    def test_chunk_without_metadata_header(self, chunker_without_metadata):
        """Test that chunks don't include headers when disabled."""
        text = "This is the main content of the decision."
        metadata = {
            "title": "Test Decision",
            "native_id": "TEST-001",
        }

        chunks = chunker_without_metadata.chunk_text(text, "TEST-001", metadata)

        assert len(chunks) >= 1
        # Check that chunk text does NOT include header
        assert "Dokumentin konteksti" not in chunks[0].text
        assert chunks[0].text.strip() == text.strip()

    def test_header_token_overhead(self, chunker_with_metadata):
        """Test that header token overhead is reasonable."""
        metadata = {
            "title": "Kaupunginhallituksen päätös asiassa",
            "native_id": "HEL-2024-001234",
            "case_id": "HEL 2024-000123",
            "classification_title": "Hallinto ja henkilöstö",
            "classification_code": "01 02 03",
            "organization_name": "Kaupunginhallitus",
            "date_decision": "2024-01-15T10:00:00.000",
            "section": "10",
        }

        header = chunker_with_metadata._generate_metadata_header(metadata, is_attachment=False)
        token_count = chunker_with_metadata.count_tokens(header)

        # Verify token count is within expected range
        assert token_count > 0
        assert (
            token_count <= chunker_with_metadata.header_overhead_tokens * 2
        )  # Allow some margin

    def test_chunk_size_limits_with_headers(self, chunker_with_metadata):
        """Test that chunks don't exceed max tokens even with headers."""
        # Create a long text
        text = " ".join(["word"] * 500)
        metadata = {
            "title": "Test Decision with Long Title for Testing Purposes",
            "native_id": "TEST-001",
            "case_id": "TEST-CASE-001",
            "classification_title": "Test Classification",
            "organization_name": "Test Organization",
            "is_attachment": False,
        }

        chunks = chunker_with_metadata.chunk_text(text, "TEST-001", metadata)

        # All chunks should respect max token limit
        for chunk in chunks:
            assert chunk.token_count <= chunker_with_metadata.max_tokens

    def test_header_format_consistency(self, chunker_with_metadata):
        """Test header format with missing optional fields."""
        # Test with various combinations of missing fields
        metadata_variations = [
            {"title": "Test", "native_id": "001"},
            {"title": "Test", "native_id": "001", "case_id": "CASE-001"},
            {"title": "Test", "native_id": "001", "organization_name": "Org"},
            {"native_id": "001"},  # Missing title
            {"title": "Test"},  # Missing native_id
        ]

        for metadata in metadata_variations:
            metadata["is_attachment"] = False
            header = chunker_with_metadata._generate_metadata_header(
                metadata, is_attachment=False
            )

            # Should always have header markers
            assert header.startswith("---")
            assert header.endswith("---")
            # Should not have None or empty fields displayed incorrectly
            assert "None" not in header
            assert ": \n" not in header  # No empty values

    def test_attachment_chunk_with_decision_context(self, chunker_with_metadata):
        """Test that attachment chunks include parent decision context."""
        text = "This is attachment content."
        metadata = {
            "title": "Parent Decision",
            "native_id": "DECISION-001_att_1",
            "decision_native_id": "DECISION-001",
            "attachment_title": "Budget Attachment",
            "attachment_url": "https://example.com/budget.pdf",
            "is_attachment": True,
        }

        chunks = chunker_with_metadata.chunk_text(text, "DECISION-001_att_1", metadata)

        assert len(chunks) >= 1
        chunk_text = chunks[0].text

        # Should include both decision and attachment info
        assert "Parent Decision" in chunk_text
        assert "Budget Attachment" in chunk_text
        assert "https://example.com/budget.pdf" in chunk_text
        assert "This is attachment content" in chunk_text

    def test_date_formatting_in_header(self, chunker_with_metadata):
        """Test that dates are formatted correctly in headers."""
        metadata = {
            "title": "Test Decision",
            "native_id": "TEST-001",
            "date_decision": "2024-12-31T23:59:59.000",
            "is_attachment": False,
        }

        header = chunker_with_metadata._generate_metadata_header(metadata, is_attachment=False)

        # Date should be formatted without time
        assert "2024-12-31" in header
        assert "23:59:59" not in header

