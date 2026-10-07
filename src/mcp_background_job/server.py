"""FastMCP server for background job management.

pattern-forge: periodic_gc_loop starts on first JobManager init so timeouts
and terminal purges reclaim slots without waiting for the next tool call.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from .config import load_config
from .models import ExecuteOutput, KillOutput, ListOutput, ProcessOutput, StatusOutput
from .service import JobManager

logger = logging.getLogger(__name__)

_job_manager: Optional[JobManager] = None
_gc_task: Optional[asyncio.Task] = None


async def _gc_loop(manager: JobManager) -> None:
    """asyncio GC loop — forge race winner: periodic_gc_loop."""
    interval = manager.config.cleanup_interval_seconds
    logger.info("Starting job GC loop every %ss", interval)
    while True:
        try:
            await asyncio.sleep(interval)
            stats = await manager.maintain()
            if stats.get("timed_out") or stats.get("purged") or stats.get("running"):
                logger.info("GC tick stats=%s", stats)
        except asyncio.CancelledError:
            logger.info("Job GC loop cancelled")
            raise
        except Exception as e:
            logger.warning("Job GC loop error: %s", e)


def get_job_manager() -> JobManager:
    global _job_manager, _gc_task
    if _job_manager is None:
        config = load_config()
        _job_manager = JobManager(config)
        logger.info("Initialized JobManager")
        try:
            loop = asyncio.get_running_loop()
            _gc_task = loop.create_task(_gc_loop(_job_manager), name="mcp-bg-gc")
        except RuntimeError:
            logger.warning("No running loop yet; GC loop will start on next tool call")
    elif _gc_task is None or _gc_task.done():
        try:
            loop = asyncio.get_running_loop()
            _gc_task = loop.create_task(_gc_loop(_job_manager), name="mcp-bg-gc")
        except RuntimeError:
            pass
    return _job_manager


mcp = FastMCP("mcp-background-job")


@mcp.tool()
async def list_jobs() -> ListOutput:
    """List all background jobs with their status."""
    try:
        jobs = await get_job_manager().list_jobs()
        return ListOutput(jobs=jobs)
    except Exception as e:
        logger.error("Error listing jobs: %s", e)
        raise ToolError(f"Failed to list jobs: {str(e)}")


@mcp.tool()
async def get_job_status(
    job_id: str = Field(..., description="Job ID to check"),
) -> StatusOutput:
    """Get the current status of a background job."""
    try:
        status = await get_job_manager().get_job_status(job_id)
        return StatusOutput(status=status)
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except Exception as e:
        raise ToolError(f"Failed to get job status: {str(e)}")


@mcp.tool()
async def get_job_output(
    job_id: str = Field(..., description="Job ID to get output from"),
) -> ProcessOutput:
    """Get the complete stdout and stderr output of a job."""
    try:
        return await get_job_manager().get_job_output(job_id)
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except Exception as e:
        raise ToolError(f"Failed to get job output: {str(e)}")


@mcp.tool()
async def tail_job_output(
    job_id: str = Field(..., description="Job ID to tail"),
    lines: int = Field(50, description="Number of lines to return", ge=1, le=1000),
) -> ProcessOutput:
    """Get the last N lines of stdout and stderr from a job."""
    try:
        return await get_job_manager().tail_job_output(job_id, lines)
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except ValueError as e:
        raise ToolError(f"Invalid parameter: {str(e)}")
    except Exception as e:
        raise ToolError(f"Failed to tail job output: {str(e)}")


@mcp.tool()
async def execute_command(
    command: str = Field(..., description="Shell command to execute"),
) -> ExecuteOutput:
    """Execute a command as a background job and return job ID."""
    try:
        job_id = await get_job_manager().execute_command(command)
        return ExecuteOutput(job_id=job_id)
    except ValueError as e:
        raise ToolError(f"Invalid command: {str(e)}")
    except RuntimeError as e:
        if "Maximum concurrent jobs limit" in str(e):
            raise ToolError(f"Job limit reached: {str(e)}")
        raise ToolError(f"Failed to start job: {str(e)}")
    except Exception as e:
        logger.error("Error executing command '%s': %s", command, e)
        raise ToolError(f"Failed to execute command: {str(e)}")


@mcp.tool()
async def interact_with_job(
    job_id: str = Field(..., description="Job ID to interact with"),
    input: str = Field(..., description="Input to send to the job's stdin"),
) -> ProcessOutput:
    """Send input to a job's stdin and return any immediate output."""
    try:
        return await get_job_manager().interact_with_job(job_id, input)
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except RuntimeError as e:
        if "not running" in str(e):
            raise ToolError(f"Job {job_id} is not running and cannot accept input")
        raise ToolError(f"Failed to interact with job: {str(e)}")
    except Exception as e:
        raise ToolError(f"Failed to interact with job: {str(e)}")


@mcp.tool()
async def kill_job(
    job_id: str = Field(..., description="Job ID to kill"),
) -> KillOutput:
    """Kill a running background job."""
    try:
        return KillOutput(status=await get_job_manager().kill_job(job_id))
    except Exception as e:
        raise ToolError(f"Failed to kill job: {str(e)}")


@mcp.tool()
async def get_job_stats() -> dict:
    """Slot/pool stats: running vs max, terminal counts (SlotPool visibility)."""
    try:
        manager = get_job_manager()
        await manager._sync_all_statuses()
        return manager.get_stats()
    except Exception as e:
        raise ToolError(f"Failed to get job stats: {str(e)}")


@mcp.tool()
async def purge_jobs(
    force: bool = Field(
        True, description="If true, purge all terminal job records immediately"
    ),
) -> dict:
    """GC terminal jobs now — frees memory and ensures slot recount is fresh."""
    try:
        manager = get_job_manager()
        await manager._sync_all_statuses()
        timed_out = await manager._enforce_timeouts()
        purged = manager.cleanup_completed_jobs(force_purge=force)
        stats = manager.get_stats()
        stats["timed_out"] = timed_out
        stats["purged"] = purged
        return stats
    except Exception as e:
        raise ToolError(f"Failed to purge jobs: {str(e)}")


async def cleanup_on_shutdown():
    global _job_manager, _gc_task
    if _gc_task and not _gc_task.done():
        _gc_task.cancel()
        try:
            await _gc_task
        except asyncio.CancelledError:
            pass
    if _job_manager:
        logger.info("Shutting down JobManager...")
        await _job_manager.shutdown()
        logger.info("JobManager shutdown complete")


def main():
    import signal
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )
    logger.info("Starting MCP Background Job Server (estate SlotPool GC fork)")

    def signal_handler(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        try:
            asyncio.get_event_loop().create_task(cleanup_on_shutdown())
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        asyncio.run(cleanup_on_shutdown())
    except Exception as e:
        logger.error("Server error: %s", e)
        asyncio.run(cleanup_on_shutdown())
        sys.exit(1)


if __name__ == "__main__":
    main()
