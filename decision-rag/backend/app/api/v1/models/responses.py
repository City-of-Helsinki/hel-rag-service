"""Response models for API endpoints."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobStatusResponse(BaseModel):
    """Response model for job status."""

    job_id: str = Field(..., description="Unique job identifier")
    type: str = Field(..., description="Job type (fetch, ingest, full_pipeline)")
    status: str = Field(
        ...,
        description="Job status (created, running, completed, failed, cancelled)",
    )
    progress: Optional[float] = Field(
        None,
        description="Progress percentage (0-100)",
        ge=0,
        le=100,
    )
    message: Optional[str] = Field(None, description="Status message or error details")
    start_time: Optional[datetime] = Field(None, description="Job start timestamp")
    end_time: Optional[datetime] = Field(None, description="Job end timestamp")
    statistics: Optional[Dict[str, Any]] = Field(
        None,
        description="Job execution statistics",
    )


class PipelineStatsResponse(BaseModel):
    """Response model for pipeline execution statistics."""

    total_documents: int = Field(..., description="Total documents processed")
    successful: int = Field(..., description="Successfully processed documents")
    failed: int = Field(..., description="Failed documents")
    skipped: int = Field(..., description="Skipped documents")
    duration_seconds: Optional[float] = Field(
        None,
        description="Total execution time in seconds",
    )
    attachments_processed: Optional[int] = Field(
        None,
        description="Number of attachments processed",
    )


class RepositoryStatsResponse(BaseModel):
    """Response model for repository statistics."""

    total_documents: int = Field(..., description="Total documents in repository")
    storage_path: str = Field(..., description="Storage directory path")
    total_size_mb: Optional[float] = Field(
        None,
        description="Total storage size in megabytes",
    )
    date_range: Optional[Dict[str, str]] = Field(
        None,
        description="Date range of stored documents",
    )


class VectorStoreStatsResponse(BaseModel):
    """Response model for vector store statistics."""

    instance: str = Field(..., description="Vector store instance type (e.g., Elasticsearch)")
    index_name: str = Field(..., description="Index name")
    total_chunks: int = Field(..., description="Total document chunks in index")
    index_size_mb: Optional[float] = Field(
        None,
        description="Index size in megabytes",
    )


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(..., description="Overall health status (healthy, unhealthy)")
    timestamp: datetime = Field(..., description="Health check timestamp")
    components: Optional[Dict[str, Dict[str, Any]]] = Field(
        None,
        description="Component-wise health status",
    )


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str = Field(..., description="Error type or code")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional error details",
    )


class DocumentSummary(BaseModel):
    """Summary of a decision document."""

    native_id: str = Field(..., description="Native document ID")
    title: str = Field(..., description="Document title")
    decision_date: Optional[str] = Field(None, description="Decision date")
    organization: Optional[str] = Field(None, description="Organization name")
    classification: Optional[str] = Field(None, description="Document classification")


class DocumentListResponse(BaseModel):
    """Response model for document list."""

    documents: List[DocumentSummary] = Field(..., description="List of documents")
    total: int = Field(..., description="Total number of matching documents")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Items per page")
    total_pages: int = Field(..., description="Total number of pages")


class DocumentDetailResponse(BaseModel):
    """Response model for detailed document information."""

    native_id: str = Field(..., description="Native document ID")
    title: str = Field(..., description="Document title")
    content: str = Field(..., description="Document content")
    decision_date: Optional[str] = Field(None, description="Decision date")
    organization: Optional[Dict[str, Any]] = Field(
        None,
        description="Organization information",
    )
    classification: Optional[str] = Field(None, description="Document classification")
    subject: Optional[str] = Field(None, description="Document subject")
    attachments: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Document attachments",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional metadata",
    )


class SearchResult(BaseModel):
    """Single search result."""

    native_id: str = Field(..., description="Document native ID")
    title: str = Field(..., description="Document title")
    content: str = Field(..., description="Matching content snippet")
    score: float = Field(..., description="Relevance score")
    decision_date: Optional[str] = Field(None, description="Decision date")
    organization: Optional[str] = Field(None, description="Organization name")


class SearchResultResponse(BaseModel):
    """Response model for search results."""

    query: str = Field(..., description="Original search query")
    results: List[SearchResult] = Field(..., description="Search results")
    total: int = Field(..., description="Total number of matching results")
    took_ms: Optional[int] = Field(None, description="Query execution time in milliseconds")
