"""
Main FastAPI application entrypoint.
"""

from contextlib import asynccontextmanager
from typing import Any, Dict

import uvicorn
import sentry_sdk
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from app.api.deps import get_scheduler
from app.api.router import api_router
from app.core import get_logger, settings, setup_logging
from app.utils import create_error_response, generate_error_id, log_error_with_id

# Setup logging
setup_logging(
    log_level=settings.LOG_LEVEL,
    log_dir=settings.LOG_DIR,
    log_file=settings.LOG_FILE,
    api_log_file=settings.API_LOG_FILE,
    error_log_file=settings.ERROR_LOG_FILE,
    retention_days=settings.LOG_RETENTION_DAYS,
    rotation_when=settings.LOG_ROTATION_WHEN,
    rotation_interval=settings.LOG_ROTATION_INTERVAL,
)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting FastAPI application...")
    logger.info(f"API Title: {settings.API_TITLE}")
    logger.info(f"API Version: {settings.API_VERSION}")
    logger.info(f"API Prefix: {settings.API_PREFIX}")
    logger.info(f"Elasticsearch URL: {settings.ELASTICSEARCH_URL}")
    logger.info(f"Elasticsearch Index: {settings.ELASTICSEARCH_INDEX}")

    # Initialize and start scheduler if enabled
    scheduler = None
    if settings.SCHEDULER_ENABLED:
        logger.info("Scheduler is enabled, initializing...")
        try:
            scheduler = get_scheduler()
            scheduler.start()
            logger.info("Scheduler started successfully")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}", exc_info=True)
            logger.warning("Continuing without scheduler")
    else:
        logger.info("Scheduler is disabled")

    # Initialize Sentry, if enabled
    if settings.SENTRY_ENABLED:
        logger.info("Initializing Sentry for error tracking...")
        try:
            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                environment=settings.SENTRY_ENVIRONMENT,
            )
            logger.info("Sentry initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Sentry: {e}", exc_info=True)
            logger.warning("Continuing without Sentry")

    yield

    # Shutdown
    logger.info("Shutting down FastAPI application...")

    # Shutdown scheduler if running
    if scheduler is not None:
        try:
            logger.info("Shutting down scheduler...")
            scheduler.shutdown(wait=True)
            logger.info("Scheduler shutdown complete")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)


# Create FastAPI application
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None,
)

# Add OpenAPI security scheme for API key authentication
if settings.API_AUTH_ENABLED:
    # Add security scheme to OpenAPI schema
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )

        # Add security scheme
        openapi_schema["components"]["securitySchemes"] = {
            "ApiKeyAuth": {
                "type": "apiKey",
                "in": "header",
                "name": settings.API_AUTH_KEY_HEADER,
                "description": "API key for authentication. Provide your API key in this header.",
            }
        }

        # Apply security globally (will be overridden for health endpoints)
        openapi_schema["security"] = [{"ApiKeyAuth": []}]

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.API_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", settings.API_AUTH_KEY_HEADER],  # Explicitly include API key header
)


# Exception handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Handle validation errors.

    Returns a standardized error response for validation failures.
    """
    error_id = generate_error_id()

    # Log validation error with ID
    logger.warning(
        f"Error ID: {error_id} | Validation error on {request.method} {request.url.path} | "
        f"Errors: {exc.errors()}"
    )

    # In DEBUG mode, include validation details
    if settings.DEBUG:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "error": "validation_error",
                "error_id": error_id,
                "message": "Request validation failed",
                "details": exc.errors(),
            },
        )

    # In production, use sanitized response
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=create_error_response(
            error_id=error_id,
            error_type="validation_error",
            message="Request validation failed. Please check your input.",
        ),
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """
    Handle general exceptions.

    Returns a standardized error response for unexpected errors.
    """
    error_id = generate_error_id()

    # Log error with full context and traceback
    log_error_with_id(logger, exc, error_id, request)

    # In DEBUG mode, include exception details
    if settings.DEBUG:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "internal_server_error",
                "error_id": error_id,
                "message": "An unexpected error occurred",
                "details": f"{type(exc).__name__}: {str(exc)}",
            },
        )

    # In production, return sanitized response with error ID
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=create_error_response(error_id=error_id),
    )


# Root endpoint
@app.get("/", tags=["root"])
async def root() -> Dict[str, Any]:
    """
    Root endpoint.

    Returns basic API information.
    """
    return {
        "name": settings.API_TITLE,
        "version": settings.API_VERSION,
        "description": settings.API_DESCRIPTION,
        "docs": "/docs" if settings.DEBUG else None,
        "openapi": "/openapi.json" if settings.DEBUG else None,
    }

# Include API router with prefix
app.include_router(api_router, prefix=settings.API_PREFIX)


# Run application
if __name__ == "__main__":

    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        workers=settings.API_WORKERS,
        reload=settings.API_RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
    )
