"""Job management service for background processes.

Slot lifecycle follows pattern-forge winners:
- retrieve: estate/python/robomp/src/slot_pool.py (acquire/release on terminal)
- retrieve: estate/corral/src/scheduledTasks.ts (removeJob when complete)
- race: sweep_then_admit before limit reject; asyncio GC loop; release_on_terminal
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .config import BackgroundJobConfig
from .models import BackgroundJob, JobStatus, JobSummary, ProcessOutput
from .process import ProcessWrapper

logger = logging.getLogger(__name__)

TERMINAL = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.KILLED)

BLOCKED_COMMAND_PATTERNS = [
    r"rm\s+.*-rf.*/",
    r"sudo\s+rm",
    r">\s*/dev/",
    r"wget.*\|.*sh",
    r"curl.*\|.*sh",
    r"curl.*\|.*bash",
    r"dd\s+if=.*of=/dev/",
    r"mkfs\.",
    r"fdisk",
    r":(){ :|:& };:",
    r"cat\s+/dev/urandom",
    r"chmod.*777.*/",
    r"chown.*root.*/",
]


class JobManager:
    """Central service for managing background processes (SlotPool-style slots)."""

    def __init__(self, config: Optional[BackgroundJobConfig] = None):
        self.config = config or BackgroundJobConfig()
        self._jobs: Dict[str, BackgroundJob] = {}
        self._processes: Dict[str, ProcessWrapper] = {}

        logger.info(
            "JobManager initialized max_jobs=%s timeout=%s cleanup=%ss retention=%ss",
            self.config.max_concurrent_jobs,
            self.config.default_job_timeout,
            self.config.cleanup_interval_seconds,
            self.config.job_retention_seconds,
        )

    def _validate_command_security(self, command: str) -> None:
        for pattern in BLOCKED_COMMAND_PATTERNS:
            if re.search(pattern, command, re.IGNORECASE):
                logger.warning("Blocked dangerous command pattern: %s", command)
                raise ValueError(
                    f"Command contains dangerous pattern and is not allowed: {command}"
                )

        if self.config.allowed_command_patterns:
            allowed = any(
                re.search(p, command, re.IGNORECASE)
                for p in self.config.allowed_command_patterns
            )
            if not allowed:
                raise ValueError(f"Command not in allowed patterns: {command}")

    def _running_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.status == JobStatus.RUNNING)

    async def _sync_all_statuses(self) -> None:
        for job_id in list(self._jobs.keys()):
            try:
                await self._update_job_status(job_id)
            except Exception as e:
                logger.warning("Failed to update status for job %s: %s", job_id, e)

    async def _enforce_timeouts(self) -> int:
        """Kill RUNNING jobs past default_job_timeout (forge: free-on-timeout)."""
        timeout = self.config.default_job_timeout
        if not timeout:
            return 0
        now = datetime.now(timezone.utc)
        killed = 0
        for job_id, job in list(self._jobs.items()):
            if job.status != JobStatus.RUNNING:
                continue
            age = (now - job.started).total_seconds()
            if age < timeout:
                continue
            logger.warning(
                "Job %s exceeded timeout %ss (age=%.0fs); killing",
                job_id,
                timeout,
                age,
            )
            result = await self.kill_job(job_id)
            if result == "killed":
                job.status = JobStatus.FAILED
                if job.completed is None:
                    job.completed = datetime.now(timezone.utc)
                killed += 1
        return killed

    async def execute_command(self, command: str) -> str:
        if not command or not command.strip():
            raise ValueError("Command cannot be empty")

        self._validate_command_security(command.strip())

        # race:sweep_then_admit — sync + GC before rejecting at limit
        await self._sync_all_statuses()
        await self._enforce_timeouts()
        self.cleanup_completed_jobs(force_purge=False)

        if self._running_count() >= self.config.max_concurrent_jobs:
            # One more aggressive purge of terminal records, then recheck
            self.cleanup_completed_jobs(force_purge=True)
            await self._sync_all_statuses()
            if self._running_count() >= self.config.max_concurrent_jobs:
                raise RuntimeError(
                    f"Maximum concurrent jobs limit "
                    f"({self.config.max_concurrent_jobs}) reached "
                    f"(running={self._running_count()})"
                )

        job_id = str(uuid.uuid4())
        job = BackgroundJob(
            job_id=job_id,
            command=command.strip(),
            status=JobStatus.RUNNING,
            started=datetime.now(timezone.utc),
        )
        process_wrapper = ProcessWrapper(
            job_id=job_id,
            command=command.strip(),
            max_output_size=self.config.max_output_size_bytes,
        )

        try:
            await process_wrapper.start()
            job.pid = process_wrapper.get_pid()
            self._jobs[job_id] = job
            self._processes[job_id] = process_wrapper
            logger.info("Started job %s: %s", job_id, command.strip())
            return job_id
        except Exception as e:
            logger.error("Failed to start job %s: %s", job_id, e)
            try:
                process_wrapper.cleanup()
            except Exception:
                pass
            raise

    async def get_job_status(self, job_id: str) -> JobStatus:
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")
        await self._update_job_status(job_id)
        return self._jobs[job_id].status

    async def kill_job(self, job_id: str) -> str:
        if job_id not in self._jobs:
            return "not_found"

        job = self._jobs[job_id]
        process_wrapper = self._processes.get(job_id)
        await self._update_job_status(job_id)

        if job.status in TERMINAL:
            return "already_terminated"

        if process_wrapper is None:
            job.status = JobStatus.FAILED
            job.completed = datetime.now(timezone.utc)
            return "already_terminated"

        if process_wrapper.kill():
            job.status = JobStatus.KILLED
            job.completed = datetime.now(timezone.utc)
            job.exit_code = process_wrapper.get_exit_code()
            logger.info("Killed job %s", job_id)
            return "killed"
        return "already_terminated"

    async def get_job_output(self, job_id: str) -> ProcessOutput:
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")
        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            return ProcessOutput(stdout="", stderr="")
        return process_wrapper.get_output()

    async def tail_job_output(self, job_id: str, lines: int) -> ProcessOutput:
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")
        if lines <= 0:
            raise ValueError("Number of lines must be positive")
        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            return ProcessOutput(stdout="", stderr="")
        return process_wrapper.tail_output(lines)

    async def interact_with_job(self, job_id: str, input_text: str) -> ProcessOutput:
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")
        await self._update_job_status(job_id)
        job = self._jobs[job_id]
        if job.status != JobStatus.RUNNING:
            raise RuntimeError(f"Job {job_id} is not running (status: {job.status})")
        process_wrapper = self._processes.get(job_id)
        if process_wrapper is None:
            raise RuntimeError(f"Process wrapper for job {job_id} not found")
        return await process_wrapper.send_input(input_text)

    async def list_jobs(self) -> List[JobSummary]:
        await self._sync_all_statuses()
        summaries = [
            JobSummary(
                job_id=job.job_id,
                status=job.status,
                command=job.command,
                started=job.started,
            )
            for job in self._jobs.values()
        ]
        summaries.sort(key=lambda x: x.started, reverse=True)
        return summaries

    async def _update_job_status(self, job_id: str) -> None:
        if job_id not in self._jobs:
            return

        job = self._jobs[job_id]
        process_wrapper = self._processes.get(job_id)

        if process_wrapper is None:
            if job.status == JobStatus.RUNNING:
                # reconnect resilience: orphan RUNNING → FAILED (slot freed)
                job.status = JobStatus.FAILED
                job.completed = datetime.now(timezone.utc)
            return

        current_status = process_wrapper.get_status()
        if job.status != current_status:
            job.status = current_status
            if current_status in TERMINAL:
                if job.completed is None:
                    job.completed = process_wrapper.completed_at or datetime.now(
                        timezone.utc
                    )
                job.exit_code = process_wrapper.get_exit_code()
                logger.info(
                    "Job %s completed status=%s exit_code=%s (slot released)",
                    job_id,
                    current_status,
                    job.exit_code,
                )

    def cleanup_completed_jobs(self, force_purge: bool = False) -> int:
        """GC terminal jobs — pattern-forge SlotPool.release + corral removeJob.

        Cleans process wrappers always; removes job records after retention
        (or immediately when force_purge=True / retention=0).
        """
        cleaned_count = 0
        jobs_to_remove: List[str] = []
        now = datetime.now(timezone.utc)
        retention = 0 if force_purge else self.config.job_retention_seconds

        for job_id, job in list(self._jobs.items()):
            if job.status not in TERMINAL:
                continue

            process_wrapper = self._processes.get(job_id)
            if process_wrapper:
                try:
                    process_wrapper.cleanup()
                    del self._processes[job_id]
                    cleaned_count += 1
                except Exception as e:
                    logger.warning("Error cleaning up job %s: %s", job_id, e)

            completed_at = job.completed or job.started
            age = (now - completed_at).total_seconds()
            if age >= retention:
                jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            self._jobs.pop(job_id, None)
            self._processes.pop(job_id, None)
            cleaned_count += 1
            logger.debug("Purged terminal job record %s", job_id)

        if cleaned_count > 0:
            logger.info(
                "GC cleaned %s (running=%s total=%s)",
                cleaned_count,
                self._running_count(),
                len(self._jobs),
            )
        return cleaned_count

    async def maintain(self) -> dict:
        """One GC tick: sync, timeout kill, purge. Called by server loop."""
        await self._sync_all_statuses()
        timed_out = await self._enforce_timeouts()
        purged = self.cleanup_completed_jobs(force_purge=False)
        stats = self.get_stats()
        stats["timed_out"] = timed_out
        stats["purged"] = purged
        return stats

    async def get_job(self, job_id: str) -> BackgroundJob:
        if job_id not in self._jobs:
            raise KeyError(f"Job {job_id} not found")
        await self._update_job_status(job_id)
        return self._jobs[job_id]

    def get_stats(self) -> Dict[str, int]:
        stats = {
            "total": len(self._jobs),
            "running": 0,
            "completed": 0,
            "failed": 0,
            "killed": 0,
            "max_concurrent": self.config.max_concurrent_jobs,
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
        logger.info("Shutting down JobManager...")
        for job_id, job in list(self._jobs.items()):
            if job.status == JobStatus.RUNNING:
                try:
                    await self.kill_job(job_id)
                except Exception as e:
                    logger.warning("Error killing job %s during shutdown: %s", job_id, e)
        self.cleanup_completed_jobs(force_purge=True)
        logger.info("JobManager shutdown complete")
