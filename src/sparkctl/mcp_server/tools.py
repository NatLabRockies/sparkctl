"""MCP tool implementations for Spark log analysis."""

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

from loguru import logger
from sparkctl.mcp_server.error_patterns import ErrorPatternRegistry
from sparkctl.mcp_server.log_parser import SparkLogLocator, SparkLogParser
from sparkctl.mcp_server.models import (
    ErrorCategory,
    ErrorOccurrence,
    LogEntry,
    SparkApplication,
    SparkApplicationList,
    SparkFailureAnalysis,
    SparkLogsResponse,
)
from sparkctl.mcp_server.recovery import RecoveryEngine


def get_spark_logs(
    spark_scratch: str,
    log_type: Literal["master", "worker", "executor", "connect", "thrift", "all"] = "all",
    app_id: str | None = None,
    executor_id: str | None = None,
    tail_lines: int = 500,
) -> SparkLogsResponse:
    """Retrieve and aggregate Spark logs from the cluster.

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory containing logs.
    log_type
        Type of logs to retrieve (master, worker, executor, connect, thrift, or all).
    app_id
        Filter executor logs by application ID (e.g., "app-20240115120000-0000").
    executor_id
        Filter by specific executor ID (e.g., "0", "1").
    tail_lines
        Number of lines to retrieve from the end of each log file.

    Returns
    -------
    SparkLogsResponse
        SparkLogsResponse containing aggregated log content from requested sources.
    """
    scratch_path = Path(spark_scratch)
    locator = SparkLogLocator(scratch_path)
    parser = SparkLogParser()

    log_entries: list[LogEntry] = []
    truncated = False

    # Get requested log files
    log_files = []
    if log_type in ("master", "all"):
        log_files.extend(locator.get_master_logs())
    if log_type in ("worker", "all"):
        log_files.extend(locator.get_worker_logs())
    if log_type in ("connect", "all"):
        log_files.extend(locator.get_connect_logs())
    if log_type in ("thrift", "all"):
        log_files.extend(locator.get_thrift_logs())
    if log_type in ("executor", "all"):
        executor_logs = locator.get_executor_logs(app_id)
        # Filter by executor_id if specified
        if executor_id is not None:
            executor_logs = [f for f in executor_logs if f.source_id.endswith(f"/{executor_id}")]
        log_files.extend(executor_logs)

    for log_file in log_files:
        try:
            lines = parser._read_lines(log_file.path, tail_lines)
            content = "\n".join(lines)

            # Check if we truncated
            if tail_lines and len(lines) == tail_lines:
                truncated = True

            source = f"{log_file.source_type}-{log_file.source_id}"
            log_entries.append(
                LogEntry(
                    source=source,
                    filepath=str(log_file.path),
                    content=content,
                    line_count=len(lines),
                    last_modified=log_file.last_modified,
                )
            )
        except Exception:
            # Skip files we can't read
            continue

    return SparkLogsResponse(
        logs=log_entries,
        total_entries=len(log_entries),
        truncated=truncated,
        spark_scratch=str(scratch_path),
    )


def _extract_stack_trace(entry_full_text: str, include_stack_traces: bool) -> str | None:
    """Extract stack trace from log entry if present."""
    if not include_stack_traces or "\n" not in entry_full_text:
        return None
    return "\n".join(entry_full_text.split("\n")[1:])


def _extract_affected_resources(
    entry_full_text: str,
    stage_pattern: re.Pattern[str],
    executor_pattern: re.Pattern[str],
    affected_stages: set[str],
    affected_executors: set[str],
) -> None:
    """Extract and update affected stages and executors from log entry."""
    stage_match = stage_pattern.search(entry_full_text)
    if stage_match:
        affected_stages.add(stage_match.group(1))

    executor_match = executor_pattern.search(entry_full_text)
    if executor_match:
        affected_executors.add(executor_match.group(1))


def _process_log_entry(
    entry,
    log_file,
    errors: list[ErrorOccurrence],
    error_counts: dict[str, int],
    affected_stages: set[str],
    affected_executors: set[str],
    stage_pattern: re.Pattern[str],
    executor_pattern: re.Pattern[str],
    include_stack_traces: bool,
    max_errors: int,
) -> tuple[bool, str | None]:
    """Process a single log entry and update analysis state."""
    should_torc, reason = ErrorPatternRegistry.should_recommend_torc(entry.full_text)

    pattern = ErrorPatternRegistry.classify_error(entry.full_text)
    if pattern is None:
        return should_torc, reason

    category_str = pattern.category.value
    error_counts[category_str] = error_counts.get(category_str, 0) + 1

    _extract_affected_resources(
        entry.full_text, stage_pattern, executor_pattern, affected_stages, affected_executors
    )

    if len(errors) < max_errors:
        stack_trace = _extract_stack_trace(entry.full_text, include_stack_traces)
        errors.append(
            ErrorOccurrence(
                error_type=pattern.category,
                severity=pattern.severity,
                message=entry.message,
                stack_trace=stack_trace,
                source_file=str(log_file.path),
                line_number=entry.line_start,
                timestamp=entry.timestamp,
                context_lines=[],
            )
        )

    return should_torc, reason


def analyze_spark_failure(
    spark_scratch: str,
    app_id: str | None = None,
    include_stack_traces: bool = True,
    max_errors: int = 50,
) -> SparkFailureAnalysis:
    """Analyze Spark logs for failure patterns and provide diagnosis.

    Scans master, worker, and executor logs for known error patterns including
    OOM errors, shuffle failures, stage failures, connection issues, and more.
    Provides a summary of detected errors and recommends recovery actions.

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory containing logs.
    app_id
        Specific application ID to analyze (optional, analyzes all if not specified).
    include_stack_traces
        Whether to include full stack traces in the output.
    max_errors
        Maximum number of error occurrences to include in response.

    Returns
    -------
    SparkFailureAnalysis
        SparkFailureAnalysis with categorized errors, root cause analysis,
        and recommendations for recovery.
    """
    scratch_path = Path(spark_scratch)
    locator = SparkLogLocator(scratch_path)
    parser = SparkLogParser()

    errors: list[ErrorOccurrence] = []
    error_counts: dict[str, int] = {}
    affected_stages: set[str] = set()
    affected_executors: set[str] = set()
    recommend_torc = False
    torc_reason: str | None = None

    stage_pattern = re.compile(r"stage[s]?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
    executor_pattern = re.compile(r"executor[s]?\s*(\d+)", re.IGNORECASE)

    log_files = locator.get_all_log_files()
    if app_id:
        log_files = [f for f in log_files if f.source_type != "executor" or app_id in f.source_id]

    for log_file in log_files:
        try:
            entries = parser.parse_file(log_file.path)
            problem_entries = parser.get_warnings_and_errors(entries)

            for entry in problem_entries:
                should_torc, reason = _process_log_entry(
                    entry,
                    log_file,
                    errors,
                    error_counts,
                    affected_stages,
                    affected_executors,
                    stage_pattern,
                    executor_pattern,
                    include_stack_traces,
                    max_errors,
                )

                if should_torc:
                    recommend_torc = True
                    if torc_reason is None:
                        torc_reason = reason
        except Exception:
            logger.exception("Failed to analyze file {}", log_file)
            continue

    likely_root_cause = _determine_root_cause(error_counts)

    torc_recommendation = None
    if recommend_torc:
        torc_recommendation = (
            f"{torc_reason}. Use torc's analyze_workflow_logs tool to check for "
            "Slurm job failures, node health issues, or filesystem problems."
        )

    return SparkFailureAnalysis(
        app_id=app_id,
        errors=errors,
        error_summary=error_counts,
        likely_root_cause=likely_root_cause,
        affected_stages=sorted(affected_stages),
        affected_executors=sorted(affected_executors),
        analysis_timestamp=datetime.now(),
        recommend_torc_analysis=recommend_torc,
        torc_recommendation=torc_recommendation,
    )


def _determine_root_cause(error_counts: dict[str, int]) -> str | None:
    """Determine the most likely root cause from error counts."""
    if not error_counts:
        return None

    # Priority order for root cause determination
    priority = ErrorPatternRegistry.get_root_cause_priority()

    # Find the highest priority error that occurred
    root_cause = None
    best_priority = float("inf")

    for error_type, count in error_counts.items():
        if count > 0:
            try:
                category = ErrorCategory(error_type)
                p = priority.get(category, 100)
                if p < best_priority:
                    best_priority = p
                    root_cause = error_type
            except ValueError:
                continue

    if root_cause:
        # Get description from pattern registry
        for pattern in ErrorPatternRegistry.PATTERNS:
            if pattern.category.value == root_cause:
                causes = ", ".join(pattern.common_causes[:2])
                return f"{pattern.description}. Common causes: {causes}"

    return root_cause


def get_recovery_suggestions(
    error_types: list[str],
    current_config: dict[str, str] | None = None,
):
    """Get recovery suggestions for detected Spark errors.

    Based on the error types detected by analyze_spark_failure, provides
    prioritized suggestions for fixing the issues.

    Parameters
    ----------
    error_types
        List of error type strings from analyze_spark_failure
        (e.g., ["out_of_memory", "shuffle_failure"]).
    current_config
        Current Spark configuration values (optional, for context).

    Returns
    -------
    RecoverySuggestions
        RecoverySuggestions with prioritized remediation steps and
        guidance on whether to use torc for system-level analysis.
    """
    engine = RecoveryEngine()

    # Convert string error types to ErrorCategory
    categories: list[ErrorCategory] = []
    for error_type in error_types:
        try:
            categories.append(ErrorCategory(error_type))
        except ValueError:
            # Skip unknown error types
            continue

    return engine.get_suggestions(categories, current_config)


def list_spark_applications(
    spark_scratch: str,
    include_completed: bool = True,
    include_failed: bool = True,
) -> SparkApplicationList:
    """List Spark applications found in the spark_scratch directory.

    Discovers applications from executor log directories and provides
    metadata about each application.

    Parameters
    ----------
    spark_scratch
        Path to the spark_scratch directory.
    include_completed
        Include applications that completed successfully.
    include_failed
        Include applications that failed.

    Returns
    -------
    SparkApplicationList
        SparkApplicationList with application metadata.
    """
    scratch_path = Path(spark_scratch)
    locator = SparkLogLocator(scratch_path)

    applications: list[SparkApplication] = []
    app_ids = locator.get_application_ids()

    for app_id in app_ids:
        executor_logs = locator.get_executor_logs(app_id)
        executor_count = len(executor_logs)

        # Get start time from directory creation time
        app_dir = scratch_path / "workers" / app_id
        start_time = None
        if app_dir.exists():
            try:
                start_time = datetime.fromtimestamp(app_dir.stat().st_ctime)
            except OSError:
                pass

        log_files = [str(log.path) for log in executor_logs]

        applications.append(
            SparkApplication(
                app_id=app_id,
                start_time=start_time,
                has_executor_logs=executor_count > 0,
                executor_count=executor_count,
                log_files=log_files,
            )
        )

    return SparkApplicationList(
        applications=applications,
        total_count=len(applications),
        spark_scratch=str(scratch_path),
    )
