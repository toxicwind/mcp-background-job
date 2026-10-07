"""Configuration management for MCP Background Job Server."""

import os
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class BackgroundJobConfig(BaseModel):
    """Configuration for the background job server.

    Defaults tuned for agent-fleet floods (pattern-forge: SlotPool + removeJob).
    Env: MCP_BG_MAX_JOBS, MCP_BG_MAX_OUTPUT_SIZE, MCP_BG_JOB_TIMEOUT,
    MCP_BG_CLEANUP_INTERVAL, MCP_BG_JOB_RETENTION, MCP_BG_ALLOWED_COMMANDS,
    MCP_BG_WORKING_DIR.
    """

    max_concurrent_jobs: int = Field(
        default=32, description="Maximum number of concurrent RUNNING jobs", ge=1, le=100
    )
    max_output_size_bytes: int = Field(
        default=10 * 1024 * 1024,
        description="Maximum output buffer size per job in bytes",
        ge=1024,
        le=100 * 1024 * 1024,
    )
    default_job_timeout: Optional[int] = Field(
        default=1800,
        description="Default job timeout in seconds (kill RUNNING past this)",
        ge=1,
    )
    cleanup_interval_seconds: int = Field(
        default=60,
        description="Periodic GC interval for terminal jobs / timeouts",
        ge=10,
        le=3600,
    )
    job_retention_seconds: int = Field(
        default=120,
        description="Seconds to keep COMPLETED/FAILED/KILLED records before purge",
        ge=0,
        le=86400,
    )
    allowed_command_patterns: List[str] = Field(
        default_factory=list,
        description="List of allowed command patterns (empty = allow all)",
    )
    working_directory: str = Field(
        default=".", description="Working directory for job execution"
    )

    @field_validator("allowed_command_patterns", mode="before")
    @classmethod
    def split_command_patterns(cls, v):
        if isinstance(v, str):
            return [pattern.strip() for pattern in v.split(",") if pattern.strip()]
        return v

    @field_validator("working_directory")
    @classmethod
    def validate_working_directory(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"Working directory does not exist: {v}")
        if not os.path.isdir(v):
            raise ValueError(f"Working directory is not a directory: {v}")
        if not os.access(v, os.R_OK | os.W_OK):
            raise ValueError(f"Working directory is not accessible: {v}")
        return v

    @classmethod
    def from_environment(cls) -> "BackgroundJobConfig":
        config_data = {}

        if max_jobs := os.getenv("MCP_BG_MAX_JOBS"):
            config_data["max_concurrent_jobs"] = int(max_jobs)

        if max_output := os.getenv("MCP_BG_MAX_OUTPUT_SIZE"):
            if max_output.upper().endswith("MB"):
                config_data["max_output_size_bytes"] = (
                    int(max_output[:-2]) * 1024 * 1024
                )
            else:
                config_data["max_output_size_bytes"] = int(max_output)

        if job_timeout := os.getenv("MCP_BG_JOB_TIMEOUT"):
            config_data["default_job_timeout"] = int(job_timeout)

        if cleanup_interval := os.getenv("MCP_BG_CLEANUP_INTERVAL"):
            config_data["cleanup_interval_seconds"] = int(cleanup_interval)

        if retention := os.getenv("MCP_BG_JOB_RETENTION"):
            config_data["job_retention_seconds"] = int(retention)

        if allowed_commands := os.getenv("MCP_BG_ALLOWED_COMMANDS"):
            config_data["allowed_command_patterns"] = allowed_commands

        if working_dir := os.getenv("MCP_BG_WORKING_DIR"):
            config_data["working_directory"] = working_dir

        return cls(**config_data)


def load_config() -> BackgroundJobConfig:
    try:
        return BackgroundJobConfig.from_environment()
    except Exception as e:
        import sys

        print(
            f"Warning: Failed to load configuration from environment: {e}",
            file=sys.stderr,
        )
        print("Using default configuration", file=sys.stderr)
        return BackgroundJobConfig()
