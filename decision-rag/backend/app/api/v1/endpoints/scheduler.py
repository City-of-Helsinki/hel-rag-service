"""Scheduler endpoints for automated pipeline execution."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.deps import get_scheduler, verify_api_key
from app.services import SchedulerService

router = APIRouter(prefix="/scheduler", tags=["scheduler"])


# Request/Response Models
class SchedulerStatusResponse(BaseModel):
    """Scheduler status response."""

    enabled: bool = Field(..., description="Whether scheduler is enabled")
    running: bool = Field(..., description="Whether scheduler is running")
    paused: bool = Field(..., description="Whether scheduler is paused")
    interval_hours: int = Field(..., description="Interval between executions (hours)")
    start_time: Optional[str] = Field(None, description="Specific start time (HH:MM)")
    timezone: str = Field(..., description="Timezone for scheduling")
    last_execution: Optional[str] = Field(None, description="Last execution timestamp")
    next_execution: Optional[str] = Field(None, description="Next scheduled execution")
    next_scheduled_run: Optional[str] = Field(
        None, description="Next run time from scheduler"
    )
    failure_count: int = Field(..., description="Consecutive failure count")
    current_job_id: Optional[str] = Field(None, description="Current running job ID")


class SchedulerHealthResponse(BaseModel):
    """Scheduler health response."""

    healthy: bool = Field(..., description="Whether scheduler is healthy")
    status: str = Field(..., description="Health status description")


class TriggerResponse(BaseModel):
    """Manual trigger response."""

    success: bool = Field(..., description="Whether trigger was successful")
    job_id: Optional[str] = Field(None, description="Job ID if triggered")
    message: str = Field(..., description="Status message")


class PauseResumeResponse(BaseModel):
    """Pause/resume response."""

    success: bool = Field(..., description="Whether operation was successful")
    message: str = Field(..., description="Status message")


class ConfigUpdateRequest(BaseModel):
    """Configuration update request."""

    interval_hours: Optional[int] = Field(None, description="Update interval (hours)")
    timezone: Optional[str] = Field(None, description="Update timezone")


class ConfigUpdateResponse(BaseModel):
    """Configuration update response."""

    success: bool = Field(..., description="Whether update was successful")
    message: str = Field(..., description="Status message")


class ExecutionHistoryResponse(BaseModel):
    """Execution history response."""

    timestamp: str
    status: str
    duration_seconds: Optional[float] = None
    documents_processed: Optional[int] = None
    error: Optional[str] = None
    statistics: Dict[str, Any] = Field(default_factory=dict)


# Endpoints
@router.get(
    "/health",
    response_model=SchedulerHealthResponse,
    summary="Check scheduler health",
    description="Check if scheduler is healthy and functioning correctly",
)
async def get_scheduler_health(
    scheduler: SchedulerService = Depends(get_scheduler),
) -> SchedulerHealthResponse:
    """
    Check scheduler health.

    Returns health status of the scheduler service.
    """
    is_healthy = scheduler.is_healthy()

    if is_healthy:
        status_text = "Scheduler is healthy"
    else:
        status_text = "Scheduler is not healthy"

    return SchedulerHealthResponse(healthy=is_healthy, status=status_text)


@router.get(
    "/status",
    response_model=SchedulerStatusResponse,
    summary="Get scheduler status",
    description="Get current status and configuration of the scheduler",
)
async def get_scheduler_status(
    scheduler: SchedulerService = Depends(get_scheduler),
    _: None = Depends(verify_api_key),
) -> SchedulerStatusResponse:
    """
    Get scheduler status.

    Returns:
        Current scheduler status including configuration and execution info
    """
    status_data = scheduler.get_status()
    return SchedulerStatusResponse(**status_data)


@router.post(
    "/trigger",
    response_model=TriggerResponse,
    summary="Trigger manual execution",
    description="Manually trigger pipeline execution immediately (bypasses schedule)",
)
async def trigger_manual_execution(
    scheduler: SchedulerService = Depends(get_scheduler),
    _: None = Depends(verify_api_key),
) -> TriggerResponse:
    """
    Trigger manual pipeline execution.

    Bypasses the schedule and triggers immediate execution.
    Returns error if another job is already running.
    """
    job_id = scheduler.trigger_now()

    if job_id:
        return TriggerResponse(
            success=True,
            job_id=job_id,
            message="Pipeline execution triggered successfully",
        )
    else:
        return TriggerResponse(
            success=False,
            job_id=None,
            message="Cannot trigger: another job is already running",
        )


@router.post(
    "/pause",
    response_model=PauseResumeResponse,
    summary="Pause scheduler",
    description="Pause scheduled pipeline execution (manual triggers still work)",
)
async def pause_scheduler(
    scheduler: SchedulerService = Depends(get_scheduler),
    _: None = Depends(verify_api_key),
) -> PauseResumeResponse:
    """
    Pause scheduled execution.

    The scheduler will not automatically execute the pipeline until resumed.
    Manual triggers via /trigger endpoint will still work.
    """
    success = scheduler.pause()

    if success:
        return PauseResumeResponse(
            success=True,
            message="Scheduler paused successfully",
        )
    else:
        return PauseResumeResponse(
            success=False,
            message="Failed to pause scheduler (may not be running)",
        )


@router.post(
    "/resume",
    response_model=PauseResumeResponse,
    summary="Resume scheduler",
    description="Resume scheduled pipeline execution",
)
async def resume_scheduler(
    scheduler: SchedulerService = Depends(get_scheduler),
    _: None = Depends(verify_api_key),
) -> PauseResumeResponse:
    """
    Resume scheduled execution.

    The scheduler will resume automatic pipeline execution according to the schedule.
    """
    success = scheduler.resume()

    if success:
        return PauseResumeResponse(
            success=True,
            message="Scheduler resumed successfully",
        )
    else:
        return PauseResumeResponse(
            success=False,
            message="Failed to resume scheduler",
        )


@router.get(
    "/history",
    response_model=List[ExecutionHistoryResponse],
    summary="Get execution history",
    description="Get recent execution history",
)
async def get_execution_history(
    count: int = 10,
    scheduler: SchedulerService = Depends(get_scheduler),
    _: None = Depends(verify_api_key),
) -> List[ExecutionHistoryResponse]:
    """
    Get recent execution history.

    Args:
        count: Number of recent records to return (default: 10)

    Returns:
        List of execution records
    """
    if count < 1 or count > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Count must be between 1 and 100",
        )

    records = scheduler.state_manager.get_recent_history(count)

    return [
        ExecutionHistoryResponse(
            timestamp=record.timestamp,
            status=record.status,
            duration_seconds=record.duration_seconds,
            documents_processed=record.documents_processed,
            error=record.error,
            statistics=record.statistics,
        )
        for record in records
    ]
