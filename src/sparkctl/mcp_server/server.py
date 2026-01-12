"""MCP Server for sparkctl Spark job failure diagnostics.

This server provides tools for diagnosing Spark job failures by analyzing
logs from master, worker, executor, thrift-server, and connect-server.

Usage:
    sparkctl-mcp-server

The server communicates over stdio using the MCP protocol.
"""

import sys
from typing import Literal

# Check for mcp package availability
try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    print(
        "Error: The 'mcp' package is required to run the MCP server.\n"
        "Install it with: pip install 'sparkctl[mcp]'",
        file=sys.stderr,
    )
    raise SystemExit(1) from e

from sparkctl.mcp_server import tools


# Create the MCP server
mcp = FastMCP(
    name="sparkctl-diagnostics",
)


@mcp.tool()
def get_spark_logs(
    spark_scratch: str,
    log_type: Literal["master", "worker", "executor", "connect", "thrift", "all"] = "all",
    app_id: str | None = None,
    executor_id: str | None = None,
    tail_lines: int = 500,
) -> str:
    """Retrieve and aggregate Spark logs from the cluster.

    Use this tool to fetch log content from Spark components for analysis.
    Logs are retrieved from the spark_scratch directory which contains
    master, worker, and executor logs.

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory containing logs.
    log_type
        Type of logs to retrieve:
        - "master": Spark master logs
        - "worker": Spark worker logs
        - "executor": Executor stderr logs (per application)
        - "connect": Spark Connect server logs
        - "thrift": Thrift server logs
        - "all": All available logs
    app_id
        Filter executor logs by application ID (e.g., "app-20240115120000-0000").
    executor_id
        Filter by specific executor ID (e.g., "0", "1").
    tail_lines
        Number of lines to retrieve from the end of each log file.

    Returns
    -------
    str
        JSON containing log entries with source, filepath, content, and metadata.
    """
    result = tools.get_spark_logs(
        spark_scratch=spark_scratch,
        log_type=log_type,
        app_id=app_id,
        executor_id=executor_id,
        tail_lines=tail_lines,
    )
    return result.model_dump_json(indent=2)


@mcp.tool()
def analyze_spark_failure(
    spark_scratch: str,
    app_id: str | None = None,
    include_stack_traces: bool = True,
    max_errors: int = 50,
) -> str:
    """Analyze Spark logs for failure patterns and provide diagnosis.

    This is the primary diagnostic tool. It scans all Spark logs for known
    error patterns including:
    - Out of memory errors (OOM)
    - Shuffle failures (FetchFailedException)
    - Stage and task failures
    - Connection/network issues
    - Disk space issues
    - Serialization errors
    - Timeout errors

    The analysis provides:
    - Categorized list of detected errors with severity
    - Error count summary by type
    - Most likely root cause determination
    - Affected stages and executors
    - Recommendation on whether to use torc for system-level analysis

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory containing logs.
    app_id
        Specific application ID to analyze. If not specified, analyzes all apps.
    include_stack_traces
        Whether to include full stack traces in the output.
    max_errors
        Maximum number of error occurrences to include (default 50).

    Returns
    -------
    str
        JSON containing SparkFailureAnalysis with errors, summary, and recommendations.
    """
    result = tools.analyze_spark_failure(
        spark_scratch=spark_scratch,
        app_id=app_id,
        include_stack_traces=include_stack_traces,
        max_errors=max_errors,
    )
    return result.model_dump_json(indent=2)


@mcp.tool()
def get_recovery_suggestions(
    error_types: list[str],
    current_config: dict[str, str] | None = None,
) -> str:
    """Get recovery suggestions for detected Spark errors.

    Based on the error types detected by analyze_spark_failure, this tool
    provides prioritized suggestions for fixing the issues. Each suggestion
    includes:
    - Priority ranking
    - Category (memory, shuffle, resource, etc.)
    - Description of the fix
    - Specific configuration changes to apply
    - sparkctl commands to run (if applicable)
    - Expected impact

    Common error_types values:
    - "out_of_memory": OOM errors
    - "shuffle_failure": Shuffle/fetch failures
    - "stage_failure": Stage or task failures
    - "resource_exhaustion": Cluster resource issues
    - "connection_failure": Network issues
    - "disk_failure": Disk space issues
    - "timeout": Timeout errors

    Parameters
    ----------
    error_types
        List of error type strings from analyze_spark_failure's error_summary.
    current_config
        Current Spark configuration values (optional, for context).

    Returns
    -------
    str
        JSON containing prioritized recovery suggestions and guidance.
    """
    result = tools.get_recovery_suggestions(
        error_types=error_types,
        current_config=current_config,
    )
    return result.model_dump_json(indent=2)


@mcp.tool()
def list_spark_applications(
    spark_scratch: str,
) -> str:
    """List Spark applications found in the spark_scratch directory.

    Discovers applications from executor log directories and provides
    metadata about each application including:
    - Application ID
    - Start time (from directory creation)
    - Whether executor logs exist
    - Number of executors
    - Paths to log files

    This is useful for identifying which applications to analyze.

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory.

    Returns
    -------
    str
        JSON containing list of SparkApplication objects with metadata.
    """
    result = tools.list_spark_applications(spark_scratch=spark_scratch)
    return result.model_dump_json(indent=2)


def main():
    """Entry point for sparkctl-mcp-server command."""
    # Configure logging to stderr only (stdout is for MCP protocol)
    import logging

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    # Run the MCP server with stdio transport
    mcp.run()


if __name__ == "__main__":
    main()
