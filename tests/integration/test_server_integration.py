"""Integration tests for MCP server."""

import asyncio
import pytest
import uuid

from mcp_background_job.config import BackgroundJobConfig
from mcp_background_job.models import JobStatus
from mcp_background_job.server import get_job_manager
from mcp_background_job.service import JobManager


class TestServerIntegration:
    """Integration tests for the MCP server functionality."""

    @pytest.fixture
    def job_manager(self):
        """Create a fresh job manager for each test."""
        config = BackgroundJobConfig(max_concurrent_jobs=5)
        return JobManager(config)

    @pytest.mark.asyncio
    async def test_job_manager_singleton(self):
        """Test that get_job_manager returns a singleton instance."""
        manager1 = get_job_manager()
        manager2 = get_job_manager()

        assert manager1 is manager2
        assert isinstance(manager1, JobManager)

    @pytest.mark.asyncio
    async def test_complete_job_workflow(self, job_manager):
        """Test complete job workflow: execute -> status -> output -> kill."""
        # Execute a command
        job_id = await job_manager.execute_command("echo 'Hello World'")
        assert uuid.UUID(job_id)  # Should be valid UUID

        # Check initial status
        status = await job_manager.get_job_status(job_id)
        assert status in [JobStatus.RUNNING, JobStatus.COMPLETED]

        # Wait a bit for completion
        await asyncio.sleep(0.5)

        # Check final status
        final_status = await job_manager.get_job_status(job_id)
        assert final_status == JobStatus.COMPLETED

        # Get output
        output = await job_manager.get_job_output(job_id)
        assert "Hello World" in output.stdout

        # Try to kill completed job
        kill_result = await job_manager.kill_job(job_id)
        assert kill_result == "already_terminated"

    @pytest.mark.asyncio
    async def test_interactive_job_workflow(self, job_manager):
        """Test interactive job workflow: execute -> interact -> kill."""
        # Start an interactive command
        job_id = await job_manager.execute_command("cat")

        # Send input to the job
        result = await job_manager.interact_with_job(job_id, "hello")
        assert isinstance(result.stdout, str)

        # Kill the job
        kill_result = await job_manager.kill_job(job_id)
        assert kill_result == "killed"

        # Verify job is killed
        status = await job_manager.get_job_status(job_id)
        assert status == JobStatus.KILLED

    @pytest.mark.asyncio
    async def test_concurrent_jobs(self, job_manager):
        """Test handling multiple concurrent jobs."""
        # Start multiple jobs
        job_ids = []
        for i in range(3):
            job_id = await job_manager.execute_command(f"echo 'Job {i}'")
            job_ids.append(job_id)

        # Wait for completion
        await asyncio.sleep(1.0)

        # Check all jobs
        jobs = await job_manager.list_jobs()
        assert len(jobs) >= 3

        # Verify job IDs are in the list
        listed_job_ids = [job.job_id for job in jobs]
        for job_id in job_ids:
            assert job_id in listed_job_ids

    @pytest.mark.asyncio
    async def test_tail_functionality(self, job_manager):
        """Test tail functionality with multi-line output."""
        # Create a job with multiple lines of output
        job_id = await job_manager.execute_command(
            "echo -e 'line1\\nline2\\nline3\\nline4\\nline5'"
        )

        # Wait for completion
        await asyncio.sleep(0.5)

        # Tail last 3 lines
        tail_output = await job_manager.tail_job_output(job_id, 3)
        lines = [line for line in tail_output.stdout.split("\n") if line.strip()]

        # Should have at most 3 non-empty lines
        assert len(lines) <= 3

        # Should contain some of the expected lines
        output_text = tail_output.stdout
        assert any(f"line{i}" in output_text for i in [3, 4, 5])

    @pytest.mark.asyncio
    async def test_job_limit_enforcement(self, job_manager):
        """Test that job limits are enforced."""
        # Set a low job limit
        job_manager.config.max_concurrent_jobs = 2

        # Start jobs up to the limit
        job_ids = []
        for i in range(2):
            job_id = await job_manager.execute_command("sleep 2")
            job_ids.append(job_id)

        # Try to start one more job - should fail
        with pytest.raises(RuntimeError, match="Maximum concurrent jobs limit"):
            await job_manager.execute_command("echo 'should fail'")

        # Clean up
        for job_id in job_ids:
            await job_manager.kill_job(job_id)

    @pytest.mark.asyncio
    async def test_error_handling(self, job_manager):
        """Test error handling for various edge cases."""
        # Test with non-existent job ID
        fake_job_id = str(uuid.uuid4())

        with pytest.raises(KeyError):
            await job_manager.get_job_status(fake_job_id)

        with pytest.raises(KeyError):
            await job_manager.get_job_output(fake_job_id)

        with pytest.raises(KeyError):
            await job_manager.interact_with_job(fake_job_id, "input")

        # kill_job should return "not_found" instead of raising
        result = await job_manager.kill_job(fake_job_id)
        assert result == "not_found"

    @pytest.mark.asyncio
    async def test_job_statistics(self, job_manager):
        """Test job statistics functionality."""
        # Initial stats
        stats = job_manager.get_stats()
        initial_total = stats["total"]

        # Add some jobs
        await job_manager.execute_command("echo 'job1'")
        job_id2 = await job_manager.execute_command("sleep 5")

        # Check updated stats
        stats = job_manager.get_stats()
        assert stats["total"] == initial_total + 2
        assert stats["running"] >= 1  # At least the sleep job should be running

        # Clean up
        await job_manager.kill_job(job_id2)

    @pytest.mark.asyncio
    async def test_cleanup_functionality(self, job_manager):
        """Test cleanup of completed jobs."""
        # Start and complete a job
        job_id = await job_manager.execute_command("echo 'cleanup test'")
        await asyncio.sleep(0.5)  # Wait for completion

        # Verify job exists
        jobs = await job_manager.list_jobs()
        job_ids = [job.job_id for job in jobs]
        assert job_id in job_ids

        # Cleanup completed jobs — purges terminal records (SlotPool release + removeJob)
        cleanup_count = job_manager.cleanup_completed_jobs(force=True)
        assert cleanup_count >= 1

        # Job record should be gone so the slot is free for new admits
        jobs_after = await job_manager.list_jobs()
        job_ids_after = [job.job_id for job in jobs_after]
        assert job_id not in job_ids_after


class TestServerErrorScenarios:
    """Test error scenarios and edge cases."""

    @pytest.fixture
    def job_manager(self):
        """Create a fresh job manager for each test."""
        return JobManager()

    @pytest.mark.asyncio
    async def test_invalid_commands(self, job_manager):
        """Test handling of invalid commands."""
        # Empty command
        with pytest.raises(ValueError, match="Command cannot be empty"):
            await job_manager.execute_command("")

        # Whitespace-only command
        with pytest.raises(ValueError, match="Command cannot be empty"):
            await job_manager.execute_command("   ")

    @pytest.mark.asyncio
    async def test_interact_with_completed_job(self, job_manager):
        """Test interaction with a completed job."""
        # Start and complete a job
        job_id = await job_manager.execute_command("echo 'done'")
        await asyncio.sleep(0.5)  # Wait for completion

        # Try to interact with completed job
        with pytest.raises(RuntimeError, match="not running"):
            await job_manager.interact_with_job(job_id, "input")

    @pytest.mark.asyncio
    async def test_invalid_tail_parameters(self, job_manager):
        """Test tail with invalid parameters."""
        job_id = await job_manager.execute_command("echo 'test'")

        # Invalid line counts
        with pytest.raises(ValueError, match="Number of lines must be positive"):
            await job_manager.tail_job_output(job_id, 0)

        with pytest.raises(ValueError, match="Number of lines must be positive"):
            await job_manager.tail_job_output(job_id, -1)
