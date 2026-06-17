"""Log parsing utilities for Spark logs."""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ParsedLogEntry:
    """A parsed log entry from a Spark log file."""

    timestamp: datetime | None
    level: str  # INFO, WARN, ERROR, DEBUG, FATAL
    logger: str  # e.g., "Master", "Worker", "BlockManager"
    message: str
    full_text: str  # Original text including stack trace
    line_start: int
    line_end: int


@dataclass
class LogFileInfo:
    """Information about a log file."""

    path: Path
    source_type: str  # master, worker, executor, connect, thrift
    source_id: str  # e.g., "0" for executor-0
    last_modified: datetime | None = None


class SparkLogLocator:
    """Discovers Spark log files from spark_scratch directory."""

    def __init__(self, spark_scratch: Path):
        self.spark_scratch = Path(spark_scratch)
        self.logs_dir = self.spark_scratch / "logs"
        self.workers_dir = self.spark_scratch / "workers"

    def get_all_log_files(self) -> list[LogFileInfo]:
        """Get all log files with their metadata."""
        files: list[LogFileInfo] = []
        files.extend(self.get_master_logs())
        files.extend(self.get_worker_logs())
        files.extend(self.get_connect_logs())
        files.extend(self.get_thrift_logs())
        files.extend(self.get_executor_logs())
        return files

    def get_master_logs(self) -> list[LogFileInfo]:
        """Get master log files.

        Returns
        -------
        list[LogFileInfo]
            Master log files matching pattern:
            spark-{user}-org.apache.spark.deploy.master.Master-*.out
        """
        if not self.logs_dir.exists():
            return []
        files = []
        for path in self.logs_dir.glob("spark-*-org.apache.spark.deploy.master.Master-*.out"):
            files.append(self._make_log_info(path, "master", "0"))
        return files

    def get_worker_logs(self) -> list[LogFileInfo]:
        """Get worker log files.

        Returns
        -------
        list[LogFileInfo]
            Worker log files matching pattern:
            spark-{user}-org.apache.spark.deploy.worker.Worker-*.out
        """
        if not self.logs_dir.exists():
            return []
        files = []
        for path in self.logs_dir.glob("spark-*-org.apache.spark.deploy.worker.Worker-*.out"):
            # Extract worker instance from filename
            match = re.search(r"Worker-(\d+)-", path.name)
            worker_id = match.group(1) if match else "0"
            files.append(self._make_log_info(path, "worker", worker_id))
        return files

    def get_connect_logs(self) -> list[LogFileInfo]:
        """Get Spark Connect server log files.

        Returns
        -------
        list[LogFileInfo]
            Connect server log files matching pattern:
            spark-*-org.apache.spark.sql.connect.service.SparkConnectServer-*.out
        """
        if not self.logs_dir.exists():
            return []
        files = []
        for path in self.logs_dir.glob("spark-*-org.apache.spark.sql.connect.*.out"):
            files.append(self._make_log_info(path, "connect", "0"))
        return files

    def get_thrift_logs(self) -> list[LogFileInfo]:
        """Get Thrift server log files.

        Returns
        -------
        list[LogFileInfo]
            Thrift server log files matching pattern:
            spark-*-org.apache.spark.sql.hive.thriftserver.*.out
        """
        if not self.logs_dir.exists():
            return []
        files = []
        for path in self.logs_dir.glob("spark-*-org.apache.spark.sql.hive.thriftserver.*.out"):
            files.append(self._make_log_info(path, "thrift", "0"))
        return files

    def get_executor_logs(self, app_id: str | None = None) -> list[LogFileInfo]:
        """Get executor log files.

        Parameters
        ----------
        app_id
            Optional application ID to filter executor logs.

        Returns
        -------
        list[LogFileInfo]
            Executor log files matching pattern:
            spark_scratch/workers/app-{id}/{executor}/stderr
        """
        if not self.workers_dir.exists():
            return []
        files = []
        pattern = f"{app_id}/*/stderr" if app_id else "app-*/*/stderr"
        for path in self.workers_dir.glob(pattern):
            # Extract app_id and executor_id from path
            parts = path.parts
            exec_id = parts[-2]  # The executor directory name
            app = parts[-3]  # The app directory name
            source_id = f"{app}/{exec_id}"
            files.append(self._make_log_info(path, "executor", source_id))
        return files

    def get_application_ids(self) -> list[str]:
        """Get list of application IDs from executor logs."""
        if not self.workers_dir.exists():
            return []
        app_ids = set()
        for path in self.workers_dir.glob("app-*/"):
            if path.is_dir():
                app_ids.add(path.name)
        return sorted(app_ids)

    def _make_log_info(self, path: Path, source_type: str, source_id: str) -> LogFileInfo:
        """Create LogFileInfo with metadata."""
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
        except OSError:
            mtime = None
        return LogFileInfo(
            path=path,
            source_type=source_type,
            source_id=source_id,
            last_modified=mtime,
        )


class SparkLogParser:
    """Parses Spark log files handling multiline entries (stack traces)."""

    # Matches: 25/01/15 12:00:00 INFO Master: Starting Spark...
    LOG_PATTERN = re.compile(
        r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) "  # timestamp
        r"(DEBUG|INFO|WARN|ERROR|FATAL) "  # level
        r"(\S+): "  # logger name
        r"(.*)$"  # message
    )

    def parse_file(self, filepath: Path, tail_lines: int | None = None) -> list[ParsedLogEntry]:
        """Parse log file, handling multiline entries (stack traces).

        Parameters
        ----------
        filepath
            Path to the log file.
        tail_lines
            If specified, only read the last N lines.

        Returns
        -------
        list[ParsedLogEntry]
            List of parsed log entries.
        """
        lines = self._read_lines(filepath, tail_lines)
        return self._parse_lines(lines)

    def _parse_lines(self, lines: list[str]) -> list[ParsedLogEntry]:
        """Parse lines into log entries, handling multiline stack traces."""
        entries: list[ParsedLogEntry] = []
        current_lines: list[str] = []
        current_start = 0

        for i, line in enumerate(lines):
            if self.LOG_PATTERN.match(line):
                # New log entry starts
                if current_lines:
                    entries.append(self._create_entry(current_lines, current_start, i - 1))
                current_lines = [line]
                current_start = i
            else:
                # Continuation of previous entry (stack trace or wrapped line)
                current_lines.append(line)

        # Don't forget the last entry
        if current_lines:
            entries.append(self._create_entry(current_lines, current_start, len(lines) - 1))

        return entries

    def _read_lines(self, filepath: Path, tail_lines: int | None) -> list[str]:
        """Read lines from file, optionally tailing.

        Parameters
        ----------
        filepath
            Path to the file.
        tail_lines
            If specified, only return the last N lines.

        Returns
        -------
        list[str]
            List of lines from the file.
        """
        try:
            content = filepath.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return []

        all_lines = content.splitlines()

        if tail_lines is None or tail_lines >= len(all_lines):
            return all_lines

        return all_lines[-tail_lines:]

    def _create_entry(self, lines: list[str], start: int, end: int) -> ParsedLogEntry:
        """Create ParsedLogEntry from accumulated lines."""
        full_text = "\n".join(lines)
        match = self.LOG_PATTERN.match(lines[0])

        if match:
            ts_str, level, logger, message = match.groups()
            try:
                # Handle year ambiguity - assume 20xx
                timestamp = datetime.strptime(f"20{ts_str}", "%Y/%m/%d %H:%M:%S")
            except ValueError:
                timestamp = None
        else:
            timestamp, level, logger, message = None, "UNKNOWN", "unknown", lines[0]

        return ParsedLogEntry(
            timestamp=timestamp,
            level=level,
            logger=logger,
            message=message,
            full_text=full_text,
            line_start=start,
            line_end=end,
        )

    def filter_by_level(
        self, entries: list[ParsedLogEntry], levels: list[str]
    ) -> list[ParsedLogEntry]:
        """Filter entries by log level.

        Parameters
        ----------
        entries
            List of parsed log entries.
        levels
            List of levels to include (e.g., ["ERROR", "WARN"]).

        Returns
        -------
        list[ParsedLogEntry]
            Filtered list of entries.
        """
        levels_upper = [x.upper() for x in levels]
        return [e for e in entries if e.level in levels_upper]

    def get_errors_only(self, entries: list[ParsedLogEntry]) -> list[ParsedLogEntry]:
        """Get only ERROR and FATAL entries."""
        return self.filter_by_level(entries, ["ERROR", "FATAL"])

    def get_warnings_and_errors(self, entries: list[ParsedLogEntry]) -> list[ParsedLogEntry]:
        """Get WARN, ERROR, and FATAL entries."""
        return self.filter_by_level(entries, ["WARN", "ERROR", "FATAL"])
