"""
Unit tests for attachment downloader service.
"""

from unittest.mock import Mock, patch

import httpx
import pytest

from app.schemas.decision import Attachment
from app.services.attachment_downloader import AttachmentDownloader


@pytest.fixture
def downloader():
    """Create attachment downloader instance."""
    return AttachmentDownloader(timeout=30, rate_limit=10.0, max_retries=2)


@pytest.fixture
def sample_attachment():
    """Create sample attachment for testing."""
    return Attachment(
        NativeId="ATT-001",
        AttachmentNumber=1,
        Title="Test Attachment",
        Type="pdf",
        FileURI="https://example.com/test.pdf",
        PublicityClass="Julkinen",
        PersonalData="Ei sisällä henkilötietoja",
    )


@pytest.fixture
def private_attachment():
    """Create private attachment that should be filtered."""
    return Attachment(
        NativeId="ATT-002",
        AttachmentNumber=2,
        Title="Private Attachment",
        Type="pdf",
        FileURI="https://example.com/private.pdf",
        PublicityClass="Salainen",
        PersonalData="Ei sisällä henkilötietoja",
    )


@pytest.fixture
def personal_data_attachment():
    """Create attachment with personal data that should be filtered."""
    return Attachment(
        NativeId="ATT-003",
        AttachmentNumber=3,
        Title="Personal Data Attachment",
        Type="pdf",
        FileURI="https://example.com/personal.pdf",
        PublicityClass="Julkinen",
        PersonalData="Sisältää henkilötietoja",
    )


def test_should_fetch_attachment_public(downloader, sample_attachment):
    """Test that public attachments without personal data are fetched."""
    assert downloader.should_fetch_attachment(sample_attachment) is True


def test_should_fetch_attachment_private(downloader, private_attachment):
    """Test that private attachments are not fetched."""
    assert downloader.should_fetch_attachment(private_attachment) is False


def test_should_fetch_attachment_personal_data(downloader, personal_data_attachment):
    """Test that attachments with personal data are not fetched."""
    assert downloader.should_fetch_attachment(personal_data_attachment) is False


def test_should_fetch_attachment_no_uri(downloader):
    """Test that attachments without URI are not fetched."""
    attachment = Attachment(
        NativeId="ATT-004",
        AttachmentNumber=4,
        Title="No URI Attachment",
        Type="pdf",
        FileURI=None,
        PublicityClass="Julkinen",
        PersonalData="Ei sisällä henkilötietoja",
    )
    assert downloader.should_fetch_attachment(attachment) is False


@patch("app.services.attachment_downloader.httpx.Client.get")
def test_download_attachment_success(mock_get, downloader, tmp_path):
    """Test successful attachment download."""
    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"PDF content"
    mock_response.headers = {"content-length": "11"}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    target_path = tmp_path / "test.pdf"
    result = downloader.download_attachment(
        "https://example.com/test.pdf", target_path, "Test Attachment"
    )

    assert result == target_path
    assert target_path.exists()
    assert target_path.read_bytes() == b"PDF content"


@patch("app.services.attachment_downloader.httpx.Client.get")
def test_download_attachment_http_error(mock_get, downloader, tmp_path):
    """Test attachment download with HTTP error."""
    # Mock HTTP error
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "404 Not Found", request=Mock(), response=mock_response
    )
    mock_get.return_value = mock_response

    target_path = tmp_path / "test.pdf"
    result = downloader.download_attachment(
        "https://example.com/test.pdf", target_path, "Test Attachment"
    )

    assert result is None
    assert not target_path.exists()


@patch("app.services.attachment_downloader.httpx.Client.get")
def test_download_attachment_too_large(mock_get, downloader, tmp_path):
    """Test that large attachments are rejected."""
    # Mock response with large file
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"Large content"
    mock_response.headers = {"content-length": str(100 * 1024 * 1024)}  # 100MB
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    target_path = tmp_path / "large.pdf"
    result = downloader.download_attachment(
        "https://example.com/large.pdf", target_path, "Large Attachment"
    )

    assert result is None
    assert not target_path.exists()


@patch("app.services.attachment_downloader.httpx.Client.get")
def test_download_attachments(mock_get, downloader, tmp_path, sample_attachment, private_attachment):
    """Test batch download of attachments with filtering."""
    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"PDF content"
    mock_response.headers = {"content-length": "11"}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    attachments = [sample_attachment, private_attachment]
    result = downloader.download_attachments(attachments, tmp_path, "DEC-001")

    # Only public attachment should be downloaded
    assert len(result) == 1
    assert "ATT-001" in result
    assert result["ATT-001"].exists()


def test_get_extension_from_uri(downloader):
    """Test file extension extraction from URI."""
    assert downloader._get_extension_from_uri("https://example.com/file.pdf") == ".pdf"
    assert downloader._get_extension_from_uri("https://example.com/file.docx") == ".docx"
    assert (
        downloader._get_extension_from_uri("https://example.com/file.PDF?param=value") == ".pdf"
    )
    assert downloader._get_extension_from_uri("https://example.com/file") == ".bin"


def test_context_manager(downloader):
    """Test attachment downloader as context manager."""
    with AttachmentDownloader() as dl:
        assert dl.client is not None

    # Client should be closed after exiting context
    # (We can't easily test this without accessing private state)


def test_rate_limiting(downloader):
    """Test that rate limiting is applied."""
    import time

    start_time = time.time()

    # Make two sequential rate-limited calls
    downloader._rate_limit()
    downloader._rate_limit()

    elapsed = time.time() - start_time

    # Should take at least one rate_limit_delay interval
    assert elapsed >= downloader.rate_limit_delay * 0.9  # Allow small timing variance
