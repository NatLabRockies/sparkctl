"""Pydantic models for MCP server responses."""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class MCPBaseModel(BaseModel):
    """Base model for MCP response models."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
        use_enum_values=True,
    )


class ErrorCategory(StrEnum):
    """Categories of Spark errors."""

    OOM = "out_of_memory"
    SHUFFLE = "shuffle_failure"
    STAGE = "stage_failure"
    RESOURCE = "resource_exhaustion"
    CONNECTION = "connection_failure"
    SERIALIZATION = "serialization_error"
    DISK = "disk_failure"
    TIMEOUT = "timeout"
    CONFIGURATION = "configuration_error"
    UNKNOWN = "unknown"


# --- Log retrieval models ---


class LogEntry(MCPBaseModel):
    """A single log file's content."""

    source: str = Field(description="Log source type (e.g., 'master', 'worker-0', 'executor-1')")
    filepath: str = Field(description="Path to the log file")
    content: str = Field(description="Log content (possibly truncated)")
    line_count: int = Field(description="Number of lines in the returned content")
    last_modified: datetime | None = Field(
        default=None, description="Last modification time of the log file"
    )


class SparkLogsResponse(MCPBaseModel):
    """Response from get_spark_logs tool."""

    logs: list[LogEntry] = Field(default_factory=list, description="List of log entries")
    total_entries: int = Field(description="Total number of log files found")
    truncated: bool = Field(default=False, description="Whether any log content was truncated")
    spark_scratch: str = Field(description="Path to spark_scratch directory used")


# --- Failure analysis models ---


class ErrorOccurrence(MCPBaseModel):
    """A detected error in the logs."""

    error_type: ErrorCategory = Field(description="Category of the error")
    severity: Literal["critical", "error", "warning"] = Field(
        description="Severity level of the error"
    )
    message: str = Field(description="Error message or summary")
    stack_trace: str | None = Field(default=None, description="Full stack trace if available")
    source_file: str = Field(description="Log file where error was found")
    line_number: int = Field(description="Line number in the log file")
    timestamp: datetime | None = Field(
        default=None, description="Timestamp of the error if parseable"
    )
    context_lines: list[str] = Field(default_factory=list, description="Surrounding context lines")


class SparkFailureAnalysis(MCPBaseModel):
    """Response from analyze_spark_failure tool."""

    app_id: str | None = Field(default=None, description="Application ID if detected")
    errors: list[ErrorOccurrence] = Field(
        default_factory=list, description="List of detected errors"
    )
    error_summary: dict[str, int] = Field(
        default_factory=dict, description="Count of errors by type"
    )
    likely_root_cause: str | None = Field(
        default=None, description="Most likely root cause based on error patterns"
    )
    affected_stages: list[str] = Field(
        default_factory=list, description="Stage IDs that had failures"
    )
    affected_executors: list[str] = Field(
        default_factory=list, description="Executor IDs that had failures"
    )
    analysis_timestamp: datetime = Field(
        default_factory=datetime.now, description="When analysis was performed"
    )
    recommend_torc_analysis: bool = Field(
        default=False,
        description="Whether torc's analyze_workflow_logs should be used for system-level issues",
    )
    torc_recommendation: str | None = Field(
        default=None, description="Specific recommendation for torc analysis"
    )


# --- Recovery suggestion models ---


class Suggestion(MCPBaseModel):
    """A recovery suggestion."""

    priority: int = Field(description="Priority (1 = highest)")
    category: str = Field(
        description="Category (e.g., 'memory', 'shuffle', 'resource', 'configuration')"
    )
    title: str = Field(description="Short title for the suggestion")
    description: str = Field(description="Detailed description of the fix")
    config_changes: dict[str, str] | None = Field(
        default=None, description="Spark configuration changes to apply"
    )
    sparkctl_command: str | None = Field(
        default=None, description="sparkctl command to apply the fix"
    )
    estimated_impact: str = Field(description="Expected impact of applying this fix")


class RecoverySuggestions(MCPBaseModel):
    """Response from get_recovery_suggestions tool."""

    suggestions: list[Suggestion] = Field(
        default_factory=list, description="Ordered list of recovery suggestions"
    )
    requires_cluster_restart: bool = Field(
        default=False, description="Whether applying these changes requires a cluster restart"
    )
    recommend_torc_analysis: bool = Field(
        default=False, description="Whether to recommend torc analysis for system-level issues"
    )
    torc_recommendation: str | None = Field(
        default=None, description="Specific guidance for using torc"
    )


# --- Application listing models ---


class SparkApplication(MCPBaseModel):
    """Information about a Spark application."""

    app_id: str = Field(description="Application ID (e.g., 'app-20240115120000-0000')")
    start_time: datetime | None = Field(default=None, description="When the application started")
    has_executor_logs: bool = Field(
        default=False, description="Whether executor logs exist for this app"
    )
    executor_count: int = Field(default=0, description="Number of executors found")
    log_files: list[str] = Field(default_factory=list, description="Paths to associated log files")


class SparkApplicationList(MCPBaseModel):
    """Response from list_spark_applications tool."""

    applications: list[SparkApplication] = Field(
        default_factory=list, description="List of Spark applications"
    )
    total_count: int = Field(description="Total number of applications found")
    spark_scratch: str = Field(description="Path to spark_scratch directory used")
