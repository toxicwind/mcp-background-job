"""FastMCP server for background job management."""

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

# Global job manager instance + periodic GC task
_job_manager: Optional[JobManager] = None
_cleanup_task: Optional[asyncio.Task] = None
_cleanup_stop: Optional[asyncio.Event] = None


def get_job_manager() -> JobManager:
    """Get or create the global job manager instance."""
    global _job_manager
    if _job_manager is None:
        config = load_config()
        _job_manager = JobManager(config)
        logger.info("Initialized JobManager")
        _ensure_cleanup_loop()
    return _job_manager


def _ensure_cleanup_loop() -> None:
    """Start periodic cleanup on the running event loop (server start / first tool).

    Uses MCP_BG_CLEANUP_INTERVAL via BackgroundJobConfig.cleanup_interval_seconds.
    Safe to call repeatedly; only one task is created.
    """
    global _cleanup_task, _cleanup_stop, _job_manager
    if _job_manager is None:
        return
    if _cleanup_task is not None and not _cleanup_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop yet (sync import / unit tests) — deferred to first async call
        return
    _cleanup_stop = asyncio.Event()
    _cleanup_task = loop.create_task(
        _job_manager.run_cleanup_loop(_cleanup_stop),
        name="mcp-bg-cleanup",
    )
    logger.info(
        "Started periodic cleanup loop "
        f"(interval={_job_manager.config.cleanup_interval_seconds}s)"
    )


async def ensure_cleanup_loop_async() -> JobManager:
    """Async entry used by tools to guarantee the GC loop is running."""
    manager = get_job_manager()
    _ensure_cleanup_loop()
    return manager


# Initialize FastMCP server
mcp = FastMCP("mcp-background-job")


@mcp.tool()
async def list_jobs() -> ListOutput:
    """List all background jobs with their status.

    Returns a list of all background jobs, including their job ID, status,
    command, and start time. Jobs are sorted by start time (newest first).
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        jobs = await job_manager.list_jobs()
        return ListOutput(jobs=jobs)
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise ToolError(f"Failed to list jobs: {str(e)}")


@mcp.tool()
async def get_job_status(
    job_id: str = Field(..., description="Job ID to check"),
) -> StatusOutput:
    """Get the current status of a background job.

    Args:
        job_id: The UUID of the job to check

    Returns:
        The current status of the job (running, completed, failed, or killed)
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        job_status = await job_manager.get_job_status(job_id)
        return StatusOutput(status=job_status)
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {e}")
        raise ToolError(f"Failed to get job status: {str(e)}")


@mcp.tool()
async def get_job_output(
    job_id: str = Field(..., description="Job ID to get output from"),
) -> ProcessOutput:
    """Get the complete stdout and stderr output of a job.

    Args:
        job_id: The UUID of the job to get output from

    Returns:
        ProcessOutput containing the complete stdout and stderr content
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        job_output = await job_manager.get_job_output(job_id)
        return job_output
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except Exception as e:
        logger.error(f"Error getting job output for {job_id}: {e}")
        raise ToolError(f"Failed to get job output: {str(e)}")


@mcp.tool()
async def tail_job_output(
    job_id: str = Field(..., description="Job ID to tail"),
    lines: int = Field(50, description="Number of lines to return", ge=1, le=1000),
) -> ProcessOutput:
    """Get the last N lines of stdout and stderr from a job.

    Args:
        job_id: The UUID of the job to tail
        lines: Number of lines to return (1-1000, default 50)

    Returns:
        ProcessOutput containing the last N lines of stdout and stderr
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        job_output = await job_manager.tail_job_output(job_id, lines)
        return job_output
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except ValueError as e:
        raise ToolError(f"Invalid parameter: {str(e)}")
    except Exception as e:
        logger.error(f"Error tailing job output for {job_id}: {e}")
        raise ToolError(f"Failed to tail job output: {str(e)}")


@mcp.tool()
async def execute_command(
    command: str = Field(..., description="Shell command to execute"),
) -> ExecuteOutput:
    """Execute a command as a background job and return job ID.

    Args:
        command: Shell command to execute in the background

    Returns:
        ExecuteOutput containing the job ID (UUID) of the started job
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        job_id = await job_manager.execute_command(command)
        return ExecuteOutput(job_id=job_id)
    except ValueError as e:
        raise ToolError(f"Invalid command: {str(e)}")
    except RuntimeError as e:
        if "Maximum concurrent jobs limit" in str(e):
            raise ToolError(f"Job limit reached: {str(e)}")
        else:
            raise ToolError(f"Failed to start job: {str(e)}")
    except Exception as e:
        logger.error(f"Error executing command '{command}': {e}")
        raise ToolError(f"Failed to execute command: {str(e)}")


@mcp.tool()
async def interact_with_job(
    job_id: str = Field(..., description="Job ID to interact with"),
    input: str = Field(..., description="Input to send to the job's stdin"),
) -> ProcessOutput:
    """Send input to a job's stdin and return any immediate output.

    Args:
        job_id: The UUID of the job to interact with
        input: Text to send to the job's stdin

    Returns:
        ProcessOutput containing any immediate stdout/stderr output after sending input
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        interaction_result = await job_manager.interact_with_job(job_id, input)
        return interaction_result
    except KeyError:
        raise ToolError(f"Job {job_id} not found")
    except RuntimeError as e:
        if "not running" in str(e):
            raise ToolError(f"Job {job_id} is not running and cannot accept input")
        else:
            raise ToolError(f"Failed to interact with job: {str(e)}")
    except Exception as e:
        logger.error(f"Error interacting with job {job_id}: {e}")
        raise ToolError(f"Failed to interact with job: {str(e)}")


@mcp.tool()
async def kill_job(
    job_id: str = Field(..., description="Job ID to kill"),
) -> KillOutput:
    """Kill a running background job.

    Args:
        job_id: The UUID of the job to terminate

    Returns:
        KillOutput indicating the result of the kill operation
    """
    try:
        job_manager = await ensure_cleanup_loop_async()
        kill_result = await job_manager.kill_job(job_id)
        return KillOutput(status=kill_result)
    except Exception as e:
        logger.error(f"Error killing job {job_id}: {e}")
        raise ToolError(f"Failed to kill job: {str(e)}")


async def cleanup_on_shutdown():
    """Cleanup function called on server shutdown."""
    global _job_manager, _cleanup_task, _cleanup_stop
    if _cleanup_stop is not None:
        _cleanup_stop.set()
    if _cleanup_task is not None and not _cleanup_task.done():
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"Cleanup task shutdown error: {e}")
    _cleanup_task = None
    _cleanup_stop = None
    if _job_manager:
        logger.info("Shutting down JobManager...")
        await _job_manager.shutdown()
        logger.info("JobManager shutdown complete")


def main():
    """Main entry point for the MCP server."""
    import asyncio
    import signal
    import sys

    # Set up logging to stderr (required for stdio transport)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )

    logger.info("Starting MCP Background Job Server")

    # Set up graceful shutdown
    async def shutdown_handler():
        await cleanup_on_shutdown()
        sys.exit(0)

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(shutdown_handler())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Eager-load config/manager so MCP_BG_* env is applied at boot.
        # Periodic GC task starts on the first async tool call (running loop).
        get_job_manager()
        logger.info(
            "JobManager ready; periodic cleanup starts with the asyncio loop "
            f"(MCP_BG_CLEANUP_INTERVAL / cleanup_interval_seconds)"
        )
        # Run the FastMCP server
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        asyncio.run(cleanup_on_shutdown())
    except Exception as e:
        logger.error(f"Server error: {e}")
        asyncio.run(cleanup_on_shutdown())
        sys.exit(1)


if __name__ == "__main__":
    main()
