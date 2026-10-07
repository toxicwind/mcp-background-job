"""Unit tests for configuration management."""

import os
import tempfile
import pytest
from unittest.mock import patch

from mcp_background_job.config import BackgroundJobConfig, load_config


class TestBackgroundJobConfig:
    """Tests for BackgroundJobConfig model."""

    def test_default_config(self):
        """Test default configuration values."""
        config = BackgroundJobConfig()

        assert config.max_concurrent_jobs == 32
        assert config.max_output_size_bytes == 10 * 1024 * 1024  # 10MB
        assert config.default_job_timeout is None
        assert config.cleanup_interval_seconds == 60
        assert config.job_retention_seconds == 0
        assert config.allowed_command_patterns == []
        assert config.working_directory == "."

    def test_custom_config(self):
        """Test custom configuration values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = BackgroundJobConfig(
                max_concurrent_jobs=5,
                max_output_size_bytes=5 * 1024 * 1024,
                default_job_timeout=1800,
                cleanup_interval_seconds=600,
                job_retention_seconds=30,
                allowed_command_patterns=["echo*", "ls*"],
                working_directory=temp_dir,
            )

            assert config.max_concurrent_jobs == 5
            assert config.max_output_size_bytes == 5 * 1024 * 1024
            assert config.default_job_timeout == 1800
            assert config.cleanup_interval_seconds == 600
            assert config.job_retention_seconds == 30
            assert config.allowed_command_patterns == ["echo*", "ls*"]
            assert config.working_directory == os.path.abspath(temp_dir)

    def test_validation_max_jobs(self):
        """Test validation of max_concurrent_jobs."""
        # Valid values
        BackgroundJobConfig(max_concurrent_jobs=1)
        BackgroundJobConfig(max_concurrent_jobs=100)

        # Invalid values
        with pytest.raises(ValueError):
            BackgroundJobConfig(max_concurrent_jobs=0)

        with pytest.raises(ValueError):
            BackgroundJobConfig(max_concurrent_jobs=101)

    def test_validation_output_size(self):
        """Test validation of max_output_size_bytes."""
        # Valid values
        BackgroundJobConfig(max_output_size_bytes=1024)  # 1KB
        BackgroundJobConfig(max_output_size_bytes=100 * 1024 * 1024)  # 100MB

        # Invalid values
        with pytest.raises(ValueError):
            BackgroundJobConfig(max_output_size_bytes=1023)  # Less than 1KB

        with pytest.raises(ValueError):
            BackgroundJobConfig(
                max_output_size_bytes=101 * 1024 * 1024
            )  # More than 100MB

    def test_validation_working_directory(self):
        """Test validation of working_directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Valid directory
            config = BackgroundJobConfig(working_directory=temp_dir)
            assert config.working_directory == os.path.abspath(temp_dir)

        # Non-existent directory
        with pytest.raises(ValueError, match="Working directory does not exist"):
            BackgroundJobConfig(working_directory="/non/existent/path")

    def test_command_patterns_splitting(self):
        """Test splitting comma-separated command patterns."""
        config = BackgroundJobConfig(allowed_command_patterns="echo*, ls *, pwd")
        assert config.allowed_command_patterns == ["echo*", "ls *", "pwd"]

        config = BackgroundJobConfig(allowed_command_patterns="  echo  ,  ls  ,  ")
        assert config.allowed_command_patterns == ["echo", "ls"]


class TestEnvironmentLoading:
    """Tests for loading configuration from environment variables."""

    def test_from_environment_defaults(self):
        """Test loading with no environment variables set."""
        with patch.dict(os.environ, {}, clear=True):
            config = BackgroundJobConfig.from_environment()

            assert config.max_concurrent_jobs == 32
            assert config.max_output_size_bytes == 10 * 1024 * 1024
            assert config.default_job_timeout is None

    def test_from_environment_all_set(self):
        """Test loading with all environment variables set."""
        env_vars = {
            "MCP_BG_MAX_JOBS": "5",
            "MCP_BG_MAX_OUTPUT_SIZE": "20MB",
            "MCP_BG_JOB_TIMEOUT": "1800",
            "MCP_BG_CLEANUP_INTERVAL": "600",
            "MCP_BG_JOB_RETENTION": "30",
            "MCP_BG_ALLOWED_COMMANDS": "echo*,ls*",
            "MCP_BG_WORKING_DIR": ".",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = BackgroundJobConfig.from_environment()

            assert config.max_concurrent_jobs == 5
            assert config.max_output_size_bytes == 20 * 1024 * 1024
            assert config.default_job_timeout == 1800
            assert config.cleanup_interval_seconds == 600
            assert config.job_retention_seconds == 30
            assert config.allowed_command_patterns == ["echo*", "ls*"]

    def test_from_environment_output_size_bytes(self):
        """Test parsing output size in bytes."""
        with patch.dict(os.environ, {"MCP_BG_MAX_OUTPUT_SIZE": "1048576"}, clear=True):
            config = BackgroundJobConfig.from_environment()
            assert config.max_output_size_bytes == 1048576

    def test_load_config_fallback(self):
        """Test load_config with fallback to defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = load_config()
            assert isinstance(config, BackgroundJobConfig)
            assert config.max_concurrent_jobs == 32

    def test_load_config_invalid_env(self):
        """Test load_config with invalid environment variables."""
        with patch.dict(os.environ, {"MCP_BG_MAX_JOBS": "invalid"}, clear=True):
            config = load_config()
            # Should fall back to defaults
            assert config.max_concurrent_jobs == 32
