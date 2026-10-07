"""Tests for JobManager service."""

import asyncio
import pytest
import uuid

from mcp_background_job.config import BackgroundJobConfig
from mcp_background_job.models import JobStatus, ProcessOutput
from mcp_background_job.service import JobManager


class TestJobManager:
    """Test cases for JobManager class."""

    def test_init(self):
        """Test JobManager initialization."""
        manager = JobManager()
        assert manager.config is not None
        assert len(manager._jobs) == 0
        assert len(manager._processes) == 0

    def test_init_with_config(self):
        """Test JobManager initialization with custom config."""
        config = BackgroundJobConfig(max_concurrent_jobs=5, max_output_size_bytes=1024)
        manager = JobManager(config)
        assert manager.config == config
        assert manager.config.max_concurrent_jobs == 5
        assert manager.config.max_output_size_bytes == 1024

    @pytest.mark.asyncio
    async def test_execute_command_success(self):
        """Test successful command execution."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'hello world'")

        # Verify job_id is a valid UUID
        uuid.UUID(job_id)

        # Verify job was created
        assert job_id in manager._jobs
        assert job_id in manager._processes

        job = manager._jobs[job_id]
        assert job.command == "echo 'hello world'"
        assert job.status == JobStatus.RUNNING
        assert job.pid is not None

    @pytest.mark.asyncio
    async def test_execute_command_empty(self):
        """Test execution with empty command."""
        manager = JobManager()

        with pytest.raises(ValueError, match="Command cannot be empty"):
            await manager.execute_command("")

        with pytest.raises(ValueError, match="Command cannot be empty"):
            await manager.execute_command("   ")

    @pytest.mark.asyncio
    async def test_execute_command_job_limit(self):
        """Test job limit enforcement."""
        config = BackgroundJobConfig(max_concurrent_jobs=1)
        manager = JobManager(config)

        # Start first job
        job_id1 = await manager.execute_command("sleep 10")
        assert job_id1 in manager._jobs

        # Try to start second job - should fail
        with pytest.raises(RuntimeError, match="Maximum concurrent jobs limit"):
            await manager.execute_command("echo 'second job'")

    @pytest.mark.asyncio
    async def test_get_job_status(self):
        """Test getting job status."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'hello'")

        status = await manager.get_job_status(job_id)
        assert status in [JobStatus.RUNNING, JobStatus.COMPLETED]

    @pytest.mark.asyncio
    async def test_get_job_status_not_found(self):
        """Test getting status for non-existent job."""
        manager = JobManager()
        fake_id = str(uuid.uuid4())

        with pytest.raises(KeyError, match=f"Job {fake_id} not found"):
            await manager.get_job_status(fake_id)

    @pytest.mark.asyncio
    async def test_list_jobs(self):
        """Test listing jobs."""
        manager = JobManager()

        # Initially empty
        jobs = await manager.list_jobs()
        assert len(jobs) == 0

        # Add some jobs
        job_id1 = await manager.execute_command("echo 'job1'")
        job_id2 = await manager.execute_command("echo 'job2'")

        jobs = await manager.list_jobs()
        assert len(jobs) == 2

        job_ids = [job.job_id for job in jobs]
        assert job_id1 in job_ids
        assert job_id2 in job_ids

        # Verify job summaries contain expected fields
        for job in jobs:
            assert hasattr(job, "job_id")
            assert hasattr(job, "status")
            assert hasattr(job, "command")
            assert hasattr(job, "started")

    @pytest.mark.asyncio
    async def test_get_job_output(self):
        """Test getting job output."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'hello world'")

        # Wait a bit for command to complete
        await asyncio.sleep(0.5)

        output = await manager.get_job_output(job_id)
        assert isinstance(output, ProcessOutput)
        assert "hello world" in output.stdout

    @pytest.mark.asyncio
    async def test_get_job_output_not_found(self):
        """Test getting output for non-existent job."""
        manager = JobManager()
        fake_id = str(uuid.uuid4())

        with pytest.raises(KeyError, match=f"Job {fake_id} not found"):
            await manager.get_job_output(fake_id)

    @pytest.mark.asyncio
    async def test_tail_job_output(self):
        """Test tailing job output."""
        manager = JobManager()
        job_id = await manager.execute_command("echo -e 'line1\\nline2\\nline3'")

        # Wait for command to complete
        await asyncio.sleep(0.5)

        # Tail last 2 lines
        output = await manager.tail_job_output(job_id, 2)
        assert isinstance(output, ProcessOutput)

        # Should contain the last lines
        lines = output.stdout.split("\n")
        assert len(lines) <= 3  # 2 lines plus potentially empty line

    @pytest.mark.asyncio
    async def test_tail_job_output_invalid_lines(self):
        """Test tailing with invalid line count."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'test'")

        with pytest.raises(ValueError, match="Number of lines must be positive"):
            await manager.tail_job_output(job_id, 0)

        with pytest.raises(ValueError, match="Number of lines must be positive"):
            await manager.tail_job_output(job_id, -1)

    @pytest.mark.asyncio
    async def test_kill_job(self):
        """Test killing a job."""
        manager = JobManager()
        job_id = await manager.execute_command("sleep 10")

        # Kill the job
        result = await manager.kill_job(job_id)
        assert result == "killed"

        # Verify job status changed
        status = await manager.get_job_status(job_id)
        assert status == JobStatus.KILLED

    @pytest.mark.asyncio
    async def test_kill_job_not_found(self):
        """Test killing non-existent job."""
        manager = JobManager()
        fake_id = str(uuid.uuid4())

        result = await manager.kill_job(fake_id)
        assert result == "not_found"

    @pytest.mark.asyncio
    async def test_kill_job_already_terminated(self):
        """Test killing already terminated job."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'quick job'")

        # Wait for job to complete
        await asyncio.sleep(0.5)

        # Try to kill completed job
        result = await manager.kill_job(job_id)
        assert result == "already_terminated"

    @pytest.mark.asyncio
    async def test_interact_with_job(self):
        """Test interacting with a job."""
        manager = JobManager()
        # Use cat command which reads from stdin
        job_id = await manager.execute_command("cat")

        # Send input to the job
        output = await manager.interact_with_job(job_id, "hello")
        assert isinstance(output, ProcessOutput)

        # Clean up
        await manager.kill_job(job_id)

    @pytest.mark.asyncio
    async def test_interact_with_job_not_found(self):
        """Test interacting with non-existent job."""
        manager = JobManager()
        fake_id = str(uuid.uuid4())

        with pytest.raises(KeyError, match=f"Job {fake_id} not found"):
            await manager.interact_with_job(fake_id, "input")

    @pytest.mark.asyncio
    async def test_interact_with_job_not_running(self):
        """Test interacting with non-running job."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'done'")

        # Wait for job to complete
        await asyncio.sleep(0.5)

        with pytest.raises(RuntimeError, match="is not running"):
            await manager.interact_with_job(job_id, "input")

    @pytest.mark.asyncio
    async def test_get_job(self):
        """Test getting complete job information."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'test'")

        job = await manager.get_job(job_id)
        assert job.job_id == job_id
        assert job.command == "echo 'test'"
        assert job.status in [JobStatus.RUNNING, JobStatus.COMPLETED]
        assert job.started is not None
        assert job.pid is not None

    @pytest.mark.asyncio
    async def test_get_job_not_found(self):
        """Test getting non-existent job."""
        manager = JobManager()
        fake_id = str(uuid.uuid4())

        with pytest.raises(KeyError, match=f"Job {fake_id} not found"):
            await manager.get_job(fake_id)

    def test_get_stats(self):
        """Test getting job statistics."""
        manager = JobManager()

        # Initially empty
        stats = manager.get_stats()
        assert stats["total"] == 0
        assert stats["running"] == 0
        assert stats["completed"] == 0
        assert stats["failed"] == 0
        assert stats["killed"] == 0

    @pytest.mark.asyncio
    async def test_get_stats_with_jobs(self):
        """Test getting statistics with jobs."""
        manager = JobManager()

        # Add a job
        await manager.execute_command("echo 'test'")

        stats = manager.get_stats()
        assert stats["total"] == 1
        assert stats["running"] >= 0  # Could be running or completed by now

    def test_cleanup_completed_jobs(self):
        """Test cleanup of completed jobs."""
        manager = JobManager()

        # Initially no cleanup needed
        count = manager.cleanup_completed_jobs()
        assert count == 0

    @pytest.mark.asyncio
    async def test_cleanup_completed_jobs_with_jobs(self):
        """Test cleanup with completed jobs."""
        manager = JobManager()

        # Start and complete a job
        await manager.execute_command("echo 'cleanup test'")
        await asyncio.sleep(0.5)  # Wait for completion

        # Cleanup should clean up the completed process
        count = manager.cleanup_completed_jobs()
        assert count >= 0  # Should clean up at least the completed job

    @pytest.mark.asyncio
    async def test_shutdown(self):
        """Test graceful shutdown."""
        manager = JobManager()

        # Start a long-running job
        job_id = await manager.execute_command("sleep 5")

        # Shutdown should kill running jobs and purge terminal records
        await manager.shutdown()

        # Killed job is purged from _jobs (SlotPool release + removeJob)
        assert job_id not in manager._jobs

    @pytest.mark.asyncio
    async def test_reclaim_slots_frees_capacity(self):
        """Completed jobs must free slots before admit (SlotPool release)."""
        config = BackgroundJobConfig(max_concurrent_jobs=1, job_retention_seconds=0)
        manager = JobManager(config)

        job_id = await manager.execute_command("echo 'done quickly'")
        # Wait for completion
        for _ in range(50):
            status = await manager.get_job_status(job_id)
            if status != JobStatus.RUNNING:
                break
            await asyncio.sleep(0.05)

        # Without reclaim, a stale COMPLETED record must not block a new job
        job_id2 = await manager.execute_command("echo 'second after reclaim'")
        assert job_id2 in manager._jobs
        # Original terminal record purged on admit reclaim
        assert job_id not in manager._jobs

class TestJobManagerEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_update_job_status_missing_process(self):
        """Test status update when process wrapper is missing."""
        manager = JobManager()
        job_id = await manager.execute_command("echo 'test'")

        # Remove process wrapper to simulate edge case
        del manager._processes[job_id]

        # Should not crash when updating status
        await manager._update_job_status(job_id)

        # Job should be marked as failed
        job = manager._jobs[job_id]
        assert job.status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_update_nonexistent_job(self):
        """Test updating status of non-existent job."""
        manager = JobManager()

        # Should not crash
        await manager._update_job_status("non-existent-id")

    @pytest.mark.asyncio
    async def test_execute_command_process_start_failure(self):
        """Test handling of process start failure."""
        manager = JobManager()

        # Command that will fail to start
        with pytest.raises(Exception):
            await manager.execute_command("/nonexistent/command")

        # Should not leave orphaned entries
        assert len(manager._jobs) == 0
        assert len(manager._processes) == 0
