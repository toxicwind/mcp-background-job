"""Job management service for background processes."""

import asyncio
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .config import BackgroundJobConfig
from .models import BackgroundJob, JobStatus, JobSummary, ProcessOutput
from .process import ProcessWrapper

logger = logging.getLogger(__name__)

# Dangerous command patterns to block for basic security
BLOCKED_COMMAND_PATTERNS = [
    r"rm\s+.*-rf.*/",  # Prevent rm -rf with paths
    r"sudo\s+rm",  # Prevent sudo rm
    r">\s*/dev/",  # Prevent writing to /dev/
    r"wget.*\|.*sh",  # Prevent wget | sh
    r"curl.*\|.*sh",  # Prevent curl | sh
    r"curl.*\|.*bash",  # Prevent curl | bash
    r"dd\s+if=.*of=/dev/",  # Prevent disk writes
    r"mkfs\.",  # Prevent filesystem creation
    r"fdisk",  # Prevent disk partitioning
    r":(){ :|:& };:",  # Prevent fork bomb
    r"cat\s+/dev/urandom",  # Prevent random data spam
    r"chmod.*777.*/",  # Prevent dangerous permissions on root
    r"chown.*root.*/",  # Prevent ownership changes to root
]


class JobManager:
    """Central service for managing background processes."""

    def __init__(self, config: Optional[BackgroundJobConfig] = None):
        """Initialize the job manager.

        Args:
            config: Configuration object, uses defaults if None
        """
        self.config = config or BackgroundJobConfig()
        self._jobs: Dict[str, BackgroundJob] = {}
        self._processes: Dict[str, ProcessWrapper] = {}

        logger.info(
            f"JobManager initialized with max_jobs={self.config.max_concurrent_jobs}, "
            f"max_output_size={self.config.max_output_size_bytes}, "
            f"cleanup_interval={self.config.cleanup_interval_seconds}s, "
            f"job_retention={self.config.job_retention_seconds}s"
        )

    def _validate_command_security(self, command: str) -> None:
        """Validate command against security policies.

        Args:
            command: Shell command to validate

        Raises:
            ValueError: If command contains dangerous patterns or violates policies
        """
        # Check against blocked patterns
        for pattern in BLOCKED_COMMAND_PATTERNS:
            if re.search(pattern, command, re.IGNORECASE):
                logger.warning(f"Blocked dangerous command pattern: {command}")
                raise ValueError(
                    f"Command contains dangerous pattern and is not allowed: {command}"
                )

        # Check against configured allowed patterns (if any)
        if self.config.allowed_command_patterns:
            allowed = False
            for allowed_pattern in self.config.allowed_command_patterns:
                if re.search(allowed_pattern, command, re.IGNORECASE):
                    allowed = True
                    break

            if not allowed:
                logger.warning(f"Command not in allowed patterns: {command}")
                raise ValueError(f"Command not in allowed patterns: {command}")

        logger.debug(f"Command security validation passed: {command}")

    async def execute_command(self, command: str) -> str:
        """Execute command as background job, return job_id.

        Args:
            command: Shell command to execute

        Returns:
            UUID v4 job identifier

        Raises:
            RuntimeError: If maximum concurrent jobs limit is reached
            ValueError: If command is empty or invalid
        """
        if not command or not command.strip():
            raise ValueError("Command cannot be empty")

        # Validate command security
        self._validate_command_security(command.strip())

        # SlotPool-style reclaim before admit (pattern-forge: SlotPool
        # acquire/release + corral removeJob). Sync statuses, free terminal
        # slots, then recount RUNNING — never reject while completed jobs
        # still occupy the pool.
        await self.reclaim_slots(force_purge_terminal=True)

        running_jobs = sum(
            1 for job in self._jobs.values() if job.status == JobStatus.RUNNING
        )
        if running_jobs >= self.config.max_concurrent_jobs:
            raise RuntimeError(
                f"Maximum concurrent jobs limit ({self.config.max_concurrent_jobs}) reached"
            )

        # Generate unique job ID
        job_id = str(uuid.uuid4())

        # Create job record
        job = BackgroundJob(
            job_id=job_id,
            command=command.strip(),
            status=JobStatus.RUNNING,
            started=datetime.now(timezone.utc),
        )

        # Create process wrapper
        process_wrapper = ProcessWrapper(
            job_id=job_id,
            command=command.strip(),
            max_output_size=self.config.max_output_size_bytes,
        )

        try:
            # Start the process
            await process_wrapper.start()

            # Update job with process info
            job.pid = process_wrapper.get_pid()

            # Store job and process
            self._jobs[job_id] = job
            self._processes[job_id] = process_wrapper

            logger.info(f"Started job {job_id}: {command.strip()}")
            return job_id

        except Exception as e:
            logger.error(f"Failed to start job {job_id}: {e}")
            # Clean up on failure
            try:
                process_wrapper.cleanup()
            except Exception:
                pass
            raise

    async def get_job_status(self, job_id: str) -> JobStatus:
        """Get current status of job.

        Args:
            job_id: Job identifier

        Returns:
            Current job status

        Raises:
            KeyError: If job_id doesn't exist
        """
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")

        # Update job status from process
        await self._update_job_status(job_id)

        return self._jobs[job_id].status

    async def kill_job(self, job_id: str) -> str:
        """Kill running job.

        Args:
            job_id: Job identifier

        Returns:
            Kill result: 'killed', 'already_terminated', or 'not_found'
        """
        if job_id not in self._jobs:
            return "not_found"

        job = self._jobs[job_id]
        process_wrapper = self._processes.get(job_id)

        # Update status first
        await self._update_job_status(job_id)

        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.KILLED]:
            return "already_terminated"

        if process_wrapper is None:
            job.status = JobStatus.FAILED
            return "already_terminated"

        # Kill the process
        if process_wrapper.kill():
            job.status = JobStatus.KILLED
            job.completed = datetime.now(timezone.utc)
            job.exit_code = process_wrapper.get_exit_code()
            logger.info(f"Killed job {job_id}")
            return "killed"
        else:
            return "already_terminated"

    async def get_job_output(self, job_id: str) -> ProcessOutput:
        """Get full stdout/stderr output.

        Args:
            job_id: Job identifier

        Returns:
            ProcessOutput with complete stdout and stderr

        Raises:
            KeyError: If job_id doesn't exist
        """
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")

        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            return ProcessOutput(stdout="", stderr="")

        return process_wrapper.get_output()

    async def tail_job_output(self, job_id: str, lines: int) -> ProcessOutput:
        """Get last N lines of output.

        Args:
            job_id: Job identifier
            lines: Number of lines to return

        Returns:
            ProcessOutput with last N lines of stdout and stderr

        Raises:
            KeyError: If job_id doesn't exist
            ValueError: If lines is not positive
        """
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")

        if lines <= 0:
            raise ValueError("Number of lines must be positive")

        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            return ProcessOutput(stdout="", stderr="")

        return process_wrapper.tail_output(lines)

    async def interact_with_job(self, job_id: str, input_text: str) -> ProcessOutput:
        """Send input to job stdin, return immediate output.

        Args:
            job_id: Job identifier
            input_text: Text to send to stdin

        Returns:
            ProcessOutput with any immediate stdout/stderr output

        Raises:
            KeyError: If job_id doesn't exist
            RuntimeError: If job is not running or stdin not available
        """
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")

        # Update job status first
        await self._update_job_status(job_id)

        job = self._jobs[job_id]
        if job.status != JobStatus.RUNNING:
            raise RuntimeError(f"Job {job_id} is not running (status: {job.status})")

        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            raise RuntimeError(f"Process wrapper for job {job_id} not found")

        return await process_wrapper.send_input(input_text)

    async def list_jobs(self) -> List[JobSummary]:
        """List all jobs.

        Returns:
            List of JobSummary objects for all jobs
        """
        # Update all job statuses
        for job_id in list(self._jobs.keys()):
            try:
                await self._update_job_status(job_id)
            except Exception as e:
                logger.warning(f"Failed to update status for job {job_id}: {e}")

        # Create summaries
        summaries = []
        for job in self._jobs.values():
            summaries.append(
                JobSummary(
                    job_id=job.job_id,
                    status=job.status,
                    command=job.command,
                    started=job.started,
                )
            )

        # Sort by start time (newest first)
        summaries.sort(key=lambda x: x.started, reverse=True)
        return summaries

    async def _update_job_status(self, job_id: str) -> None:
        """Update job status based on process state.

        Args:
            job_id: Job identifier
        """
        if job_id not in self._jobs:
            return

        job = self._jobs[job_id]
        process_wrapper = self._processes.get(job_id)

        if process_wrapper is None:
            if job.status == JobStatus.RUNNING:
                job.status = JobStatus.FAILED
                job.completed = datetime.now(timezone.utc)
            return

        # Get current process status
        current_status = process_wrapper.get_status()

        # Update job if status changed
        if job.status != current_status:
            job.status = current_status

            # Set completion time and exit code for terminated processes
            if current_status in [
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.KILLED,
            ]:
                if job.completed is None:
                    job.completed = process_wrapper.completed_at or datetime.now(
                        timezone.utc
                    )
                job.exit_code = process_wrapper.get_exit_code()

                logger.info(
                    f"Job {job_id} completed with status {current_status}, "
                    f"exit_code={job.exit_code}"
                )

    def cleanup_completed_jobs(
        self, *, purge_records: bool | None = None, force: bool = False
    ) -> int:
        """Clean up terminated processes and purge job records from `_jobs`.

        Pattern: SlotPool.release on terminal + removeJob (corral scheduledTasks).
        Terminal jobs (COMPLETED/FAILED/KILLED) free their slot so new admits
        are not starved by finished work still held in the dict.

        Args:
            purge_records: Override retention. None = follow retention/force.
            force: Purge all terminal records immediately (used before admit).

        Returns:
            Number of process wrappers and/or records cleaned.
        """
        cleaned_count = 0
        jobs_to_remove: list[str] = []
        now = datetime.now(timezone.utc)
        retention = self.config.job_retention_seconds

        if purge_records is None:
            do_purge = True if force else True
        else:
            do_purge = purge_records

        for job_id, job in list(self._jobs.items()):
            if job.status not in [
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.KILLED,
            ]:
                continue

            process_wrapper = self._processes.get(job_id)
            if process_wrapper:
                try:
                    process_wrapper.cleanup()
                    del self._processes[job_id]
                    cleaned_count += 1
                    logger.debug(f"Cleaned up process for job {job_id}")
                except Exception as e:
                    logger.warning(f"Error cleaning up job {job_id}: {e}")

            if force:
                should_remove = True
            elif do_purge:
                if retention <= 0:
                    should_remove = True
                elif job.completed is not None:
                    should_remove = (now - job.completed).total_seconds() >= retention
                else:
                    should_remove = True
            else:
                should_remove = False

            if should_remove:
                jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            if job_id in self._jobs:
                del self._jobs[job_id]
                cleaned_count += 1
                logger.debug(f"Purged job record {job_id}")
            self._processes.pop(job_id, None)

        if cleaned_count > 0:
            logger.info(
                f"Cleaned up {cleaned_count} completed job artifacts "
                f"(purged_records={len(jobs_to_remove)})"
            )

        return cleaned_count

    async def reclaim_slots(self, *, force_purge_terminal: bool = False) -> int:
        """Sync statuses, enforce timeouts, and free terminal slots.

        Called before admit (force_purge_terminal=True) and by the periodic GC
        loop. Mirrors SlotPool.release: completed/failed/killed must not keep
        counting against max_concurrent_jobs.
        """
        for job_id in list(self._jobs.keys()):
            try:
                await self._update_job_status(job_id)
            except Exception as e:
                logger.warning(f"Failed to update status for job {job_id}: {e}")

        timeout = self.config.default_job_timeout
        if timeout:
            now = datetime.now(timezone.utc)
            for job_id, job in list(self._jobs.items()):
                if job.status != JobStatus.RUNNING:
                    continue
                age = (now - job.started).total_seconds()
                if age >= timeout:
                    logger.warning(
                        f"Job {job_id} exceeded timeout {timeout}s "
                        f"(age={age:.0f}s); killing"
                    )
                    try:
                        await self.kill_job(job_id)
                    except Exception as e:
                        logger.warning(f"Timeout kill failed for {job_id}: {e}")

        return self.cleanup_completed_jobs(force=force_purge_terminal)

    async def run_cleanup_loop(
        self, stop_event: "asyncio.Event | None" = None
    ) -> None:
        """Periodic GC loop started on server boot (MCP_BG_CLEANUP_INTERVAL)."""
        interval = max(10, int(self.config.cleanup_interval_seconds))
        logger.info(f"Starting periodic cleanup loop every {interval}s")
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("Cleanup loop stop requested")
                return
            try:
                if stop_event is not None:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=interval)
                        return
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled")
                raise
            try:
                reclaimed = await self.reclaim_slots(force_purge_terminal=False)
                if reclaimed:
                    logger.info(f"Periodic GC reclaimed {reclaimed} artifacts")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Periodic GC error: {e}")

    async def get_job(self, job_id: str) -> BackgroundJob:
        """Get complete job information.

        Args:
            job_id: Job identifier

        Returns:
            Complete BackgroundJob object

        Raises:
            KeyError: If job_id doesn't exist
        """
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")

        # Update status before returning
        await self._update_job_status(job_id)
        return self._jobs[job_id]

    def get_stats(self) -> Dict[str, int]:
        """Get job statistics.

        Returns:
            Dictionary with job count statistics
        """
        stats = {
            "total": len(self._jobs),
            "running": 0,
            "completed": 0,
            "failed": 0,
            "killed": 0,
        }

        for job in self._jobs.values():
            if job.status == JobStatus.RUNNING:
                stats["running"] += 1
            elif job.status == JobStatus.COMPLETED:
                stats["completed"] += 1
            elif job.status == JobStatus.FAILED:
                stats["failed"] += 1
            elif job.status == JobStatus.KILLED:
                stats["killed"] += 1

        return stats

    async def shutdown(self) -> None:
        """Gracefully shutdown the job manager.

        Kills all running processes and cleans up resources.
        """
        logger.info("Shutting down JobManager...")

        # Kill all running jobs
        for job_id, job in self._jobs.items():
            if job.status == JobStatus.RUNNING:
                try:
                    await self.kill_job(job_id)
                    logger.info(f"Killed job {job_id} during shutdown")
                except Exception as e:
                    logger.warning(f"Error killing job {job_id} during shutdown: {e}")

        # Clean up all processes
        self.cleanup_completed_jobs()

        logger.info("JobManager shutdown complete")
