"""MCP Server for sparkctl Spark job failure diagnostics.

This module provides an MCP (Model Context Protocol) server that diagnoses
Spark job failures by analyzing logs from master, worker, executor,
thrift-server, and connect-server components.

Tools provided:
- get_spark_logs: Retrieve and aggregate Spark logs
- analyze_spark_failure: Detect error patterns and diagnose issues
- get_recovery_suggestions: Get remediation suggestions for detected errors
- list_spark_applications: List Spark applications in spark_scratch

Usage:
    Run the MCP server with: sparkctl-mcp-server

The server is designed to work alongside torc's analyze_workflow_logs tool
for full-stack diagnostics (Spark + Slurm/system-level).
"""

from sparkctl.mcp_server.error_patterns import ErrorPatternRegistry
from sparkctl.mcp_server.log_parser import SparkLogLocator, SparkLogParser
from sparkctl.mcp_server.models import (
    ErrorCategory,
    ErrorOccurrence,
    LogEntry,
    RecoverySuggestions,
    SparkApplication,
    SparkApplicationList,
    SparkFailureAnalysis,
    SparkLogsResponse,
    Suggestion,
)
from sparkctl.mcp_server.recovery import RecoveryEngine
from sparkctl.mcp_server.tools import (
    analyze_spark_failure,
    get_recovery_suggestions,
    get_spark_logs,
    list_spark_applications,
)

__all__ = [
    # Models
    "ErrorCategory",
    "ErrorOccurrence",
    "LogEntry",
    "RecoverySuggestions",
    "SparkApplication",
    "SparkApplicationList",
    "SparkFailureAnalysis",
    "SparkLogsResponse",
    "Suggestion",
    # Tools (can be used directly without MCP)
    "analyze_spark_failure",
    "get_recovery_suggestions",
    "get_spark_logs",
    "list_spark_applications",
    # Utilities
    "ErrorPatternRegistry",
    "RecoveryEngine",
    "SparkLogLocator",
    "SparkLogParser",
]
