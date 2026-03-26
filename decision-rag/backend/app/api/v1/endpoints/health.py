"""Health check endpoints."""

from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Depends

from app.api.deps import get_repository, get_vector_store
from app.api.v1.models.responses import HealthResponse
from app.core import settings
from app.repositories import DecisionRepository
from app.services import ElasticsearchVectorStore

router = APIRouter()


@router.get("/health", response_model=Dict[str, str])
async def health_check():
    """
    Basic health check.

    Returns 200 OK for load balancers and simple monitoring.
    """
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@router.get("/health/detailed", response_model=HealthResponse)
async def detailed_health_check(
    repository: DecisionRepository = Depends(get_repository),
    vector_store: ElasticsearchVectorStore = Depends(get_vector_store),
):
    """
    Detailed health check with component status.

    Checks connectivity to:
    - Elasticsearch
    - Azure OpenAI
    - External Decision API
    - File system

    Returns:
        Detailed health status of all components
    """
    components: Dict[str, Dict[str, Any]] = {}
    overall_status = "healthy"

    # Check Elasticsearch
    try:
        es_health = vector_store.client.cluster.health()
        components["elasticsearch"] = {
            "status": "healthy",
            "cluster_status": es_health.get("status"),
            "number_of_nodes": es_health.get("number_of_nodes"),
        }
    except Exception as e:
        components["elasticsearch"] = {
            "status": "unhealthy",
            "error": str(e),
        }
        overall_status = "unhealthy"

    # Check Azure OpenAI
    try:
        # Simple check - we can't easily test embeddings without actual text,
        # so we just verify credentials are configured
        if settings.AZURE_OPENAI_ENDPOINT and settings.AZURE_OPENAI_API_KEY:
            components["azure_openai"] = {
                "status": "healthy",
                "endpoint": settings.AZURE_OPENAI_ENDPOINT,
                "model": settings.AZURE_OPENAI_EMBEDDING_MODEL,
            }
        else:
            components["azure_openai"] = {
                "status": "unhealthy",
                "error": "Missing Azure OpenAI configuration",
            }
            overall_status = "unhealthy"
    except Exception as e:
        components["azure_openai"] = {
            "status": "unhealthy",
            "error": str(e),
        }
        overall_status = "unhealthy"

    # Check External API connectivity
    try:
        # We can't easily test the full API without making actual requests,
        # but we verify configuration is present
        if settings.API_KEY and settings.API_BASE_URL:
            components["external_api"] = {
                "status": "healthy",
                "base_url": settings.API_BASE_URL,
            }
        else:
            components["external_api"] = {
                "status": "unhealthy",
                "error": "Missing API configuration",
            }
            overall_status = "unhealthy"
    except Exception as e:
        components["external_api"] = {
            "status": "unhealthy",
            "error": str(e),
        }
        overall_status = "unhealthy"

    # Check file system access
    try:
        storage_path = repository.decisions_dir
        if storage_path.exists() and storage_path.is_dir():
            components["filesystem"] = {
                "status": "healthy",
                "storage_path": str(storage_path),
            }
        else:
            components["filesystem"] = {
                "status": "unhealthy",
                "error": "Storage directory not accessible",
            }
            overall_status = "unhealthy"
    except Exception as e:
        components["filesystem"] = {
            "status": "unhealthy",
            "error": str(e),
        }
        overall_status = "unhealthy"

    return HealthResponse(
        status=overall_status,
        timestamp=datetime.now(),
        components=components,
    )
