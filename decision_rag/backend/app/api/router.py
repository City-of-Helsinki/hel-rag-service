"""Main API router configuration."""

from fastapi import APIRouter

from app.api.v1.endpoints import admin, data, health, pipeline, scheduler

# Create main API router
api_router = APIRouter()

# Include endpoint routers with prefixes and tags
api_router.include_router(
    health.router,
    tags=["health"],
)

api_router.include_router(
    pipeline.router,
    prefix="/pipeline",
    tags=["pipeline"],
)

api_router.include_router(
    data.router,
    prefix="/data",
    tags=["data"],
)

api_router.include_router(
    admin.router,
    prefix="/admin",
    tags=["admin"],
)

api_router.include_router(
    scheduler.router,
    tags=["scheduler"],
)
