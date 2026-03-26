"""Data query and search endpoints."""

import math
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import get_embedder, get_repository, get_vector_store, verify_api_key
from app.api.v1.models.requests import SearchRequest
from app.api.v1.models.responses import (
    DocumentDetailResponse,
    DocumentListResponse,
    DocumentSummary,
    RepositoryStatsResponse,
    SearchResult,
    SearchResultResponse,
    VectorStoreStatsResponse,
)
from app.core import get_logger
from app.repositories import DecisionRepository
from app.services import AzureEmbedder, ElasticsearchVectorStore
from app.utils import raise_error_with_id

router = APIRouter()
logger = get_logger(__name__)


@router.get("/documents", response_model=DocumentListResponse)
async def list_documents(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
    repository: DecisionRepository = Depends(get_repository),
    _: None = Depends(verify_api_key),
):
    """
    List all stored documents with pagination.

    Returns a paginated list of decision documents with basic metadata.

    Args:
        page: Page number (1-indexed)
        page_size: Number of items per page

    Returns:
        Paginated list of documents
    """
    try:
        # Get all native IDs
        all_ids = repository.get_all_native_ids()
        total = len(all_ids)

        # Calculate pagination
        total_pages = math.ceil(total / page_size) if total > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        # Get page of IDs
        page_ids = all_ids[start_idx:end_idx]

        # Load documents for this page
        documents = []
        for native_id in page_ids:
            doc = repository.get_decision(native_id)
            if doc:
                documents.append(
                    DocumentSummary(
                        native_id=doc.NativeId,
                        title=doc.Title or "Untitled",
                        decision_date=doc.DecisionDate,
                        organization=doc.Organization.Name if doc.Organization else None,
                        classification=doc.Classification,
                        subject=doc.Subject,
                    )
                )

        return DocumentListResponse(
            documents=documents,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to list documents",
            context={"operation": "list_documents", "page": page, "page_size": page_size},
        )


@router.get("/documents/{native_id}", response_model=DocumentDetailResponse)
async def get_document(
    native_id: str,
    repository: DecisionRepository = Depends(get_repository),
    _: None = Depends(verify_api_key),
):
    """
    Get detailed information for a single document.

    Returns complete document information including content and attachments.

    Args:
        native_id: Document native ID

    Returns:
        Detailed document information
    """
    try:
        doc = repository.get_decision(native_id)

        if not doc:
            raise HTTPException(status_code=404, detail=f"Document {native_id} not found")

        # Convert to response model
        attachments = None
        if doc.Attachments:
            attachments = [
                {
                    "native_id": att.NativeId,
                    "title": att.Title,
                    "file_uri": att.FileURI,
                    "publicity_class": att.PublicityClass,
                    "type": att.Type,
                    "language": att.Language,
                }
                for att in doc.Attachments
            ]

        organization = None
        if doc.Organization:
            organization = {
                "id": doc.Organization.Id,
                "name": doc.Organization.Name,
            }

        return DocumentDetailResponse(
            native_id=doc.NativeId,
            title=doc.Title or "Untitled",
            content=doc.Content or "",
            decision_date=doc.DecisionDate,
            organization=organization,
            classification=doc.Classification,
            subject=doc.Subject,
            attachments=attachments,
            metadata={
                "diary_number": doc.DiaryNumber,
                "decision_maker": doc.Decisionmaker,
                "section": doc.Section,
                "language": doc.Language,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to retrieve document",
            context={"operation": "get_document", "native_id": native_id},
        )


@router.post("/search", response_model=SearchResultResponse)
async def search_documents(
    request: SearchRequest,
    embedder: AzureEmbedder = Depends(get_embedder),
    vector_store: ElasticsearchVectorStore = Depends(get_vector_store),
    _: None = Depends(verify_api_key),
):
    """
    Perform semantic search across document content.

    Uses vector embeddings to find semantically similar content.
    Results are ranked by relevance score.

    Args:
        request: Search request with query and filters

    Returns:
        Ranked search results
    """
    try:
        # Generate embedding for query
        query_embedding = embedder.embed_text(request.query)

        # Build filter conditions if provided
        filter_conditions = None
        if request.start_date or request.end_date or request.organization:
            filter_conditions = {}
            if request.start_date:
                filter_conditions["metadata.decision_date"] = {"gte": request.start_date}
            if request.end_date:
                if "metadata.decision_date" in filter_conditions:
                    filter_conditions["metadata.decision_date"]["lte"] = request.end_date
                else:
                    filter_conditions["metadata.decision_date"] = {"lte": request.end_date}
            if request.organization:
                filter_conditions["metadata.organization"] = request.organization

        # Perform search
        start_time = datetime.now()
        search_results = vector_store.search(
            query_vector=query_embedding,
            top_k=request.limit,
            filter_conditions=filter_conditions,
        )
        took_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        # Convert to response format
        results = []
        for hit in search_results:
            metadata = hit.get("metadata", {})
            results.append(
                SearchResult(
                    native_id=hit.get("native_id", ""),
                    title=metadata.get("title", "Untitled"),
                    content=hit.get("text", "")[:500],  # Truncate content
                    score=hit.get("score", 0.0),
                    decision_date=metadata.get("decision_date"),
                    organization=metadata.get("organization"),
                )
            )

        return SearchResultResponse(
            query=request.query,
            results=results,
            total=len(results),
            took_ms=took_ms,
        )

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Search operation failed",
            context={"operation": "search_documents", "query": request.query},
        )


@router.get("/stats", response_model=RepositoryStatsResponse)
async def get_repository_stats(
    repository: DecisionRepository = Depends(get_repository),
    _: None = Depends(verify_api_key),
):
    """
    Get repository statistics.

    Returns information about stored documents and storage usage.

    Returns:
        Repository statistics
    """
    try:
        stats = repository.get_statistics()

        return RepositoryStatsResponse(
            total_documents=stats["total_documents"],
            storage_path=stats["storage_path"],
            total_size_mb=stats["storage_size_mb"],
        )

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to retrieve repository statistics",
            context={"operation": "get_repository_stats"},
        )


@router.get("/vector-store/stats", response_model=VectorStoreStatsResponse)
async def get_vector_store_stats(
    vector_store: ElasticsearchVectorStore = Depends(get_vector_store),
    _: None = Depends(verify_api_key),
):
    """
    Get vector store statistics.

    Returns information about the Elasticsearch index and stored embeddings.

    Returns:
        Vector store statistics
    """
    try:
        # Get index stats
        stats = vector_store.client.indices.stats(index=vector_store.index_name)
        index_stats = stats["indices"].get(vector_store.index_name, {})

        # Get document count
        count_response = vector_store.client.count(index=vector_store.index_name)
        total_chunks = count_response.get("count", 0)

        # Get index size
        total_size_bytes = index_stats.get("total", {}).get("store", {}).get("size_in_bytes", 0)
        index_size_mb = total_size_bytes / (1024 * 1024)

        # Get index health
        health = vector_store.client.cluster.health(index=vector_store.index_name)
        status = health.get("status", "unknown")

        return VectorStoreStatsResponse(
            index_name=vector_store.index_name,
            total_chunks=total_chunks,
            index_size_mb=index_size_mb,
            status=status,
        )

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to retrieve vector store statistics",
            context={"operation": "get_vector_store_stats"},
        )
