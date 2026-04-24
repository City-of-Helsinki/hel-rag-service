"""Request models for API endpoints."""

from typing import Optional

from pydantic import BaseModel, Field


class FetchRequest(BaseModel):
    """Request model for document fetching."""

    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format (defaults to settings.START_DATE)",
        example="2025-01-01",
    )
    end_date: Optional[str] = Field(
        None,
        description="End date in YYYY-MM-DD format (defaults to settings.END_DATE)",
        example="2025-12-31",
    )
    resume: bool = Field(
        False,
        description="Resume from last checkpoint",
    )
    skip_existing: bool = Field(
        False,
        description="Skip documents that already exist in storage",
    )


class IngestRequest(BaseModel):
    """Request model for document ingestion."""

    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format for filtering documents",
        example="2025-01-01",
    )
    batch_size: int = Field(
        100,
        description="Number of documents to process in each batch",
        ge=1,
        le=1000,
    )
    resume: bool = Field(
        False,
        description="Resume from last processed document",
    )
    reindex: bool = Field(
        False,
        description="Reindex documents that already exist in vector store",
    )
    skip_attachments: bool = Field(
        False,
        description="Skip processing attachments",
    )


class FullPipelineRequest(BaseModel):
    """Request model for full pipeline execution."""

    start_date: Optional[str] = Field(
        None,
        description="Start date in YYYY-MM-DD format",
        example="2025-01-01",
    )
    end_date: Optional[str] = Field(
        None,
        description="End date in YYYY-MM-DD format",
        example="2025-12-31",
    )
    batch_size: int = Field(
        100,
        description="Number of documents to process in each batch",
        ge=1,
        le=1000,
    )
    resume: bool = Field(
        False,
        description="Resume from last checkpoint",
    )
    skip_existing: bool = Field(
        False,
        description="Skip documents that already exist in storage",
    )
    reindex: bool = Field(
        False,
        description="Reindex documents that already exist in vector store",
    )
    skip_attachments: bool = Field(
        False,
        description="Skip processing attachments",
    )
    keep_files: bool = Field(
        False,
        description="Keep downloaded files after ingestion",
    )


class DataQueryRequest(BaseModel):
    """Request model for querying stored data."""

    start_date: Optional[str] = Field(
        None,
        description="Filter by start date (YYYY-MM-DD)",
    )
    end_date: Optional[str] = Field(
        None,
        description="Filter by end date (YYYY-MM-DD)",
    )
    organization: Optional[str] = Field(
        None,
        description="Filter by organization ID",
    )
    classification: Optional[str] = Field(
        None,
        description="Filter by classification",
    )
    page: int = Field(
        1,
        description="Page number for pagination",
        ge=1,
    )
    page_size: int = Field(
        50,
        description="Number of items per page",
        ge=1,
        le=1000,
    )


class SearchRequest(BaseModel):
    """Request model for vector search."""

    query: str = Field(
        ...,
        description="Search query text",
        min_length=1,
    )
    limit: int = Field(
        10,
        description="Maximum number of results to return",
        ge=1,
        le=100,
    )
    start_date: Optional[str] = Field(
        None,
        description="Filter by start date (YYYY-MM-DD)",
    )
    end_date: Optional[str] = Field(
        None,
        description="Filter by end date (YYYY-MM-DD)",
    )
    organization: Optional[str] = Field(
        None,
        description="Filter by organization ID",
    )
