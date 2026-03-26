"""
Tests for ingestion pipeline.
"""

from unittest.mock import Mock, patch

import pytest

from app.schemas.decision import Attachment
from app.services.ingestion_pipeline import IngestionPipeline


class TestIngestionPipeline:
    """Tests for IngestionPipeline."""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        repo = Mock()
        repo.get_decision = Mock()
        return repo

    @pytest.fixture
    def mock_chunker(self):
        """Create a mock chunker."""
        chunker = Mock()
        chunker.chunk_text = Mock(return_value=[])
        return chunker

    @pytest.fixture
    def mock_embedder(self):
        """Create a mock embedder."""
        embedder = Mock()
        embedder.create_embeddings = Mock(return_value=[])
        return embedder

    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        store = Mock()
        store.document_exists = Mock(return_value=False)
        store.bulk_index_chunks = Mock(return_value={"success": 0, "failed": 0, "errors": []})
        store.delete_document = Mock(return_value=0)
        return store

    @pytest.fixture
    def pipeline(self, mock_repository, mock_chunker, mock_embedder, mock_vector_store):
        """Create a pipeline instance."""
        return IngestionPipeline(
            repository=mock_repository,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

    def test_process_document_not_found(self, pipeline, mock_repository):
        """Test processing a document that doesn't exist."""
        mock_repository.get_decision.return_value = None

        result = pipeline.process_document("missing_id")

        assert result["success"] is False
        assert result["error"] == "Document not found"

    def test_process_document_no_content(self, pipeline, mock_repository):
        """Test processing a document without content."""
        doc = Mock()
        doc.Content = None
        doc.Title = "Test"
        mock_repository.get_decision.return_value = doc

        result = pipeline.process_document("test_id")

        assert result["success"] is False
        assert result["error"] == "No content"

    def test_process_document_already_indexed(self, pipeline, mock_vector_store):
        """Test processing a document that's already indexed."""
        mock_vector_store.document_exists.return_value = True

        result = pipeline.process_document("test_id", reindex=False)

        assert result.get("skipped") is True

    def test_extract_metadata(self, pipeline):
        """Test metadata extraction from decision document."""
        doc = Mock()
        doc.Title = "Test Decision"
        doc.CaseID = "HEL-2024-001"
        doc.Section = "10"
        doc.ClassificationCode = "01 02 03"
        doc.ClassificationTitle = "Test Classification"
        doc.DateDecision = "2024-01-01T00:00:00.000"
        doc.Organization = Mock()
        doc.Organization.Name = "Test Organization"

        metadata = pipeline._extract_metadata(doc)

        assert metadata["title"] == "Test Decision"
        assert metadata["case_id"] == "HEL-2024-001"
        assert metadata["section"] == "10"
        assert metadata["organization_name"] == "Test Organization"

    def test_process_batch(self, pipeline, mock_repository):
        """Test batch processing."""
        # Setup mock to return None (document not found)
        mock_repository.get_decision.return_value = None

        result = pipeline.process_batch(["id1", "id2", "id3"])

        assert result["total"] == 3
        assert result["processed"] == 3
        assert result["failed"] == 3


class TestAttachmentProcessing:
    """Tests for attachment processing in ingestion pipeline."""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        repo = Mock()
        repo.get_decision = Mock()
        return repo

    @pytest.fixture
    def mock_chunker(self):
        """Create a mock chunker."""
        chunker = Mock()
        chunk = Mock()
        chunk.chunk_id = "chunk_1"
        chunk.text = "Test chunk text"
        chunk.native_id = "DEC-001_att_ATT-001"
        chunk.chunk_index = 0
        chunk.token_count = 10
        chunk.metadata = {}
        chunker.chunk_text = Mock(return_value=[chunk])
        return chunker

    @pytest.fixture
    def mock_embedder(self):
        """Create a mock embedder."""
        embedder = Mock()
        embedding_result = Mock()
        embedding_result.chunk_id = "chunk_1"
        embedding_result.embedding = [0.1] * 3072
        embedder.create_embeddings = Mock(return_value=[embedding_result])
        return embedder

    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        store = Mock()
        store.document_exists = Mock(return_value=False)
        store.bulk_index_chunks = Mock(return_value={"success": 1, "failed": 0, "errors": []})
        store.delete_document = Mock(return_value=0)
        return store

    @pytest.fixture
    def mock_attachment_downloader(self, tmp_path):
        """Create a mock attachment downloader."""
        downloader = Mock()
        # Mock successful download
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"PDF content")
        downloader.download_attachments = Mock(return_value={"ATT-001": test_file})
        return downloader

    @pytest.fixture
    def pipeline_with_attachments(
        self, mock_repository, mock_chunker, mock_embedder, mock_vector_store, mock_attachment_downloader
    ):
        """Create a pipeline instance with attachment downloader."""
        return IngestionPipeline(
            repository=mock_repository,
            chunker=mock_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
            attachment_downloader=mock_attachment_downloader,
        )

    def test_process_attachments_no_attachments(self, pipeline_with_attachments):
        """Test processing decision with no attachments."""
        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Attachments = []

        result = pipeline_with_attachments.process_attachments(decision)

        assert result["attachments_processed"] == 0
        assert result["chunks_indexed"] == 0

    @patch("app.services.ingestion_pipeline.convert_attachment_content")
    def test_process_attachments_success(
        self, mock_convert, pipeline_with_attachments, mock_attachment_downloader, tmp_path
    ):
        """Test successful attachment processing."""
        # Mock attachment conversion
        mock_convert.return_value = "# Attachment Content\n\nTest attachment text."

        # Ensure the mock returns a valid file path for this test
        test_file = tmp_path / "test_att.pdf"
        test_file.write_bytes(b"PDF content")
        mock_attachment_downloader.download_attachments.return_value = {"ATT-001": test_file}

        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Title = "Test Decision"
        decision.CaseID = "HEL-2024-001"
        decision.Section = "1"
        decision.ClassificationCode = "01"
        decision.ClassificationTitle = "Test"
        decision.DateDecision = "2024-01-01"
        decision.Organization = None

        attachment = Attachment(
            NativeId="ATT-001",
            AttachmentNumber=1,
            Title="Test Attachment",
            Type="pdf",
            FileURI="https://example.com/test.pdf",
            PublicityClass="Julkinen",
            PersonalData="Ei sisällä henkilötietoja",
        )
        decision.Attachments = [attachment]

        result = pipeline_with_attachments.process_attachments(decision)

        assert result["attachments_processed"] == 1
        assert result["chunks_indexed"] == 1
        assert result["chunks_created"] == 1

    @patch("app.services.content_converter.convert_attachment_content")
    def test_process_attachments_conversion_failure(
        self, mock_convert, pipeline_with_attachments
    ):
        """Test attachment processing with conversion failure."""
        # Mock empty conversion result
        mock_convert.return_value = ""

        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Attachments = [
            Attachment(
                NativeId="ATT-001",
                AttachmentNumber=1,
                Title="Test Attachment",
                Type="pdf",
                FileURI="https://example.com/test.pdf",
            )
        ]

        result = pipeline_with_attachments.process_attachments(decision)

        assert result["failed"] == 1
        assert result["attachments_processed"] == 0

    def test_extract_attachment_metadata(self, pipeline_with_attachments):
        """Test extraction of attachment-specific metadata."""
        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Title = "Test Decision"
        decision.CaseID = "HEL-2024-001"
        decision.Section = "1"
        decision.ClassificationCode = "01"
        decision.ClassificationTitle = "Test"
        decision.DateDecision = "2024-01-01"
        decision.Organization = None

        attachment = Attachment(
            NativeId="ATT-001",
            AttachmentNumber=1,
            Title="Test Attachment",
            Type="pdf",
            FileURI="https://example.com/test.pdf",
        )

        metadata = pipeline_with_attachments._extract_attachment_metadata(decision, attachment)

        assert metadata["is_attachment"] is True
        assert metadata["attachment_native_id"] == "ATT-001"
        assert metadata["attachment_title"] == "Test Attachment"
        assert metadata["attachment_number"] == 1
        assert metadata["attachment_type"] == "pdf"
        assert metadata["attachment_url"] == "https://example.com/test.pdf"
        assert metadata["decision_native_id"] == "DEC-001"
        # Should also include decision metadata
        assert metadata["title"] == "Test Decision"
        assert metadata["case_id"] == "HEL-2024-001"

    def test_process_attachments_no_downloads(self, pipeline_with_attachments, mock_attachment_downloader):
        """Test processing when no attachments are downloaded."""
        # Mock empty download result
        mock_attachment_downloader.download_attachments = Mock(return_value={})

        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Attachments = [
            Attachment(
                NativeId="ATT-001",
                Title="Test Attachment",
                FileURI="https://example.com/test.pdf",
            )
        ]

        result = pipeline_with_attachments.process_attachments(decision)

        assert result["attachments_processed"] == 0
        assert result["chunks_indexed"] == 0


class TestMetadataHeaderIntegration:
    """Integration tests for metadata header embedding in chunks."""

    @pytest.fixture
    def real_chunker(self):
        """Create a real chunker instance with metadata embedding enabled."""
        from app.services.chunker import ParagraphChunker

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
        """Create a real chunker instance with metadata embedding disabled."""
        from app.services.chunker import ParagraphChunker

        return ParagraphChunker(
            target_tokens=100,
            min_tokens=50,
            max_tokens=150,
            overlap_tokens=20,
            embed_metadata=False,
        )

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository."""
        repo = Mock()
        return repo

    @pytest.fixture
    def mock_embedder(self):
        """Create a mock embedder."""
        embedder = Mock()

        def mock_create_embeddings(chunks):
            results = []
            for chunk in chunks:
                result = Mock()
                result.chunk_id = chunk["chunk_id"]
                result.embedding = [0.1] * 3072
                results.append(result)
            return results

        embedder.create_embeddings = Mock(side_effect=mock_create_embeddings)
        return embedder

    @pytest.fixture
    def mock_vector_store(self):
        """Create a mock vector store."""
        store = Mock()
        store.document_exists = Mock(return_value=False)

        def mock_bulk_index(chunks):
            return {"success": len(chunks), "failed": 0, "errors": []}

        store.bulk_index_chunks = Mock(side_effect=mock_bulk_index)
        store.delete_document = Mock(return_value=0)
        return store

    @pytest.fixture
    def pipeline_with_metadata(self, mock_repository, real_chunker, mock_embedder, mock_vector_store):
        """Create a pipeline instance with real chunker (metadata enabled)."""
        return IngestionPipeline(
            repository=mock_repository,
            chunker=real_chunker,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

    @pytest.fixture
    def pipeline_without_metadata(
        self, mock_repository, chunker_without_metadata, mock_embedder, mock_vector_store
    ):
        """Create a pipeline instance with metadata disabled."""
        return IngestionPipeline(
            repository=mock_repository,
            chunker=chunker_without_metadata,
            embedder=mock_embedder,
            vector_store=mock_vector_store,
        )

    @patch("app.services.ingestion_pipeline.convert_decision_content")
    def test_end_to_end_with_metadata_headers(
        self, mock_convert, pipeline_with_metadata, mock_repository, mock_vector_store
    ):
        """Test end-to-end processing with metadata headers in chunks."""
        # Mock decision document
        decision = Mock()
        decision.NativeId = "HEL-2024-001234"
        decision.Title = "Kaupunginhallituksen päätös"
        decision.CaseID = "HEL 2024-000123"
        decision.Section = "10"
        decision.ClassificationCode = "01 02 03"
        decision.ClassificationTitle = "Hallinto"
        decision.DateDecision = "2024-01-15T10:00:00.000"
        decision.Organization = Mock()
        decision.Organization.Name = "Kaupunginhallitus"
        decision.Content = "<html>Test content</html>"
        decision.Attachments = []

        mock_repository.get_decision = Mock(return_value=decision)
        mock_convert.return_value = "# Test Decision\n\nThis is the decision content."

        # Process document
        result = pipeline_with_metadata.process_document("HEL-2024-001234")

        # Verify processing succeeded
        assert result["success"] is True
        assert result["chunks_created"] > 0
        assert result["chunks_indexed"] > 0

        # Verify chunks were indexed with metadata headers
        mock_vector_store.bulk_index_chunks.assert_called_once()
        indexed_chunks = mock_vector_store.bulk_index_chunks.call_args[0][0]

        # Check that chunk text includes metadata header
        assert len(indexed_chunks) > 0
        chunk_text = indexed_chunks[0]["text"]
        assert "Dokumentin konteksti" in chunk_text
        assert "Kaupunginhallituksen päätös" in chunk_text
        assert "HEL-2024-001234" in chunk_text
        assert "This is the decision content" in chunk_text

    @patch("app.services.ingestion_pipeline.convert_decision_content")
    def test_chunks_without_metadata_headers(
        self, mock_convert, pipeline_without_metadata, mock_repository, mock_vector_store
    ):
        """Test that chunks don't include headers when feature is disabled."""
        decision = Mock()
        decision.NativeId = "HEL-2024-001234"
        decision.Title = "Test Decision"
        decision.Content = "<html>Test content</html>"
        decision.CaseID = ""
        decision.Section = ""
        decision.ClassificationCode = ""
        decision.ClassificationTitle = ""
        decision.DateDecision = None
        decision.Organization = None
        decision.Attachments = []

        mock_repository.get_decision = Mock(return_value=decision)
        mock_convert.return_value = "This is the decision content without headers."

        result = pipeline_without_metadata.process_document("HEL-2024-001234")

        assert result["success"] is True

        indexed_chunks = mock_vector_store.bulk_index_chunks.call_args[0][0]
        chunk_text = indexed_chunks[0]["text"]

        # Verify no metadata header
        assert "Dokumentin konteksti" not in chunk_text
        assert chunk_text.strip() == "This is the decision content without headers."

    @patch("app.services.ingestion_pipeline.convert_attachment_content")
    @patch("app.services.ingestion_pipeline.convert_decision_content")
    def test_attachment_chunk_headers(
        self, mock_convert_decision, mock_convert_attachment, pipeline_with_metadata, mock_repository
    ):
        """Test that attachment chunks have proper headers including parent decision context."""
        from unittest.mock import patch as mock_patch

        decision = Mock()
        decision.NativeId = "DEC-001"
        decision.Title = "Parent Decision"
        decision.Content = "<html>Decision content</html>"
        decision.CaseID = "CASE-001"
        decision.Section = "5"
        decision.ClassificationCode = "01"
        decision.ClassificationTitle = "Administration"
        decision.DateDecision = "2024-01-15"
        decision.Organization = None

        attachment = Attachment(
            NativeId="ATT-001",
            AttachmentNumber=1,
            Title="Budget Attachment",
            Type="pdf",
            FileURI="https://example.com/budget.pdf",
            PublicityClass="Julkinen",
            PersonalData="Ei sisällä henkilötietoja",
        )
        decision.Attachments = [attachment]

        mock_repository.get_decision = Mock(return_value=decision)
        mock_convert_decision.return_value = "Decision content"
        mock_convert_attachment.return_value = "Budget details and financial information."

        # Create a mock attachment downloader
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_file = Path(tmp_dir) / "test.pdf"
            test_file.write_bytes(b"PDF content")

            mock_downloader = Mock()
            mock_downloader.download_attachments = Mock(return_value={"ATT-001": test_file})

            # Add downloader to pipeline
            pipeline_with_metadata.attachment_downloader = mock_downloader

            # Mock settings to enable attachment processing
            with mock_patch("app.services.ingestion_pipeline.settings") as mock_settings:
                mock_settings.PROCESS_ATTACHMENTS = True
                mock_settings.ATTACHMENT_DOWNLOAD_DIR = tmp_dir
                mock_settings.MAX_WORKERS_ATTACHMENT_PROCESSING = 1  # Use serial processing for test

                result = pipeline_with_metadata.process_document("DEC-001")

                # Verify attachment processing
                assert result["success"] is True
                assert result["attachments_processed"] > 0
                assert result["attachment_chunks_indexed"] > 0

    def test_header_searchability_simulation(self, real_chunker):
        """Test that headers improve searchability (manual inspection)."""
        text = "The budget allocation for the infrastructure project includes funds for road maintenance."
        metadata = {
            "title": "Infrastructure Budget Decision",
            "native_id": "INF-2024-001",
            "case_id": "HEL 2024-INF-555",
            "classification_title": "Infrastructure and Construction",
            "classification_code": "05 03 01",
            "organization_name": "Infrastructure Department",
            "date_decision": "2024-03-15T14:00:00.000",
            "section": "12",
            "is_attachment": False,
        }

        chunks = real_chunker.chunk_text(text, "INF-2024-001", metadata)

        assert len(chunks) > 0
        chunk_text = chunks[0].text

        # Verify rich context is embedded
        assert "Infrastructure Budget Decision" in chunk_text
        assert "Infrastructure Department" in chunk_text
        assert "Infrastructure and Construction" in chunk_text
        assert "budget allocation" in chunk_text

        # This makes the chunk semantically richer for search queries about:
        # - Infrastructure decisions
        # - Budget allocations
        # - Department-specific queries
        # - Classification-based filtering

    def test_metadata_preserved_in_indexed_chunks(
        self, pipeline_with_metadata, mock_repository, mock_vector_store
    ):
        """Test that metadata is preserved alongside enhanced text."""
        with patch("app.services.ingestion_pipeline.convert_decision_content") as mock_convert:
            decision = Mock()
            decision.NativeId = "TEST-001"
            decision.Title = "Test Decision"
            decision.Content = "<html>Content</html>"
            decision.CaseID = "CASE-001"
            decision.Section = ""
            decision.ClassificationCode = ""
            decision.ClassificationTitle = ""
            decision.DateDecision = None
            decision.Organization = None
            decision.Attachments = []

            mock_repository.get_decision = Mock(return_value=decision)
            mock_convert.return_value = "Decision content text."

            result = pipeline_with_metadata.process_document("TEST-001")

            assert result["success"] is True

            indexed_chunks = mock_vector_store.bulk_index_chunks.call_args[0][0]
            assert len(indexed_chunks) > 0

            chunk = indexed_chunks[0]
            # Verify both text and metadata are present
            assert "text" in chunk
            assert "metadata" in chunk
            assert chunk["metadata"]["title"] == "Test Decision"
            assert chunk["metadata"]["case_id"] == "CASE-001"

