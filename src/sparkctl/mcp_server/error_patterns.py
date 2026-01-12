"""Error pattern registry for classifying Spark log errors."""

import re
from dataclasses import dataclass, field
from typing import Literal

from sparkctl.mcp_server.models import ErrorCategory


@dataclass
class ErrorPattern:
    """Defines an error pattern with its classification."""

    category: ErrorCategory
    severity: Literal["critical", "error", "warning"]
    patterns: list[re.Pattern[str]]
    description: str
    common_causes: list[str] = field(default_factory=list)


class ErrorPatternRegistry:
    """Registry of known Spark error patterns."""

    PATTERNS: list[ErrorPattern] = [
        # OOM Errors
        ErrorPattern(
            category=ErrorCategory.OOM,
            severity="critical",
            patterns=[
                re.compile(r"java\.lang\.OutOfMemoryError", re.IGNORECASE),
                re.compile(r"GC overhead limit exceeded"),
                re.compile(r"Java heap space"),
                re.compile(r"Unable to acquire.*memory", re.IGNORECASE),
                re.compile(r"Required executor memory.*is above", re.IGNORECASE),
                re.compile(r"Container killed by YARN for exceeding memory limits"),
                re.compile(r"ExecutorLostFailure.*memory", re.IGNORECASE),
            ],
            description="Out of memory error in JVM heap or off-heap",
            common_causes=[
                "Executor memory too small for data",
                "Memory leak in UDF",
                "Too many partitions cached",
                "Large broadcast variables",
                "Data skew causing some tasks to process much more data",
            ],
        ),
        # Shuffle Failures
        ErrorPattern(
            category=ErrorCategory.SHUFFLE,
            severity="error",
            patterns=[
                re.compile(r"FetchFailedException"),
                re.compile(r"Failed to connect to.*shuffle", re.IGNORECASE),
                re.compile(r"Too Large Frame.*shuffle", re.IGNORECASE),
                re.compile(r"Shuffle.*failed", re.IGNORECASE),
                re.compile(r"ShuffleBlockFetcherIterator.*failed", re.IGNORECASE),
                re.compile(r"Unable to fetch.*shuffle", re.IGNORECASE),
                re.compile(r"MetadataFetchFailedException"),
            ],
            description="Shuffle data transfer failed between executors",
            common_causes=[
                "Executor lost during shuffle",
                "Network connectivity issues",
                "Disk full on shuffle storage",
                "Shuffle block too large (>2GB)",
                "Too few shuffle partitions",
            ],
        ),
        # Stage Failures
        ErrorPattern(
            category=ErrorCategory.STAGE,
            severity="error",
            patterns=[
                re.compile(r"Lost task \d+\.\d+ in stage"),
                re.compile(r"Stage \d+ \(.*\) failed"),
                re.compile(r"Job aborted due to stage failure"),
                re.compile(r"TaskSetManager.*Lost task"),
                re.compile(r"Task \d+ failed \d+ times"),
                re.compile(r"Aborting job.*stage.*failed"),
            ],
            description="Task or stage execution failed",
            common_causes=[
                "Exception in task code",
                "Executor failure",
                "Data corruption",
                "Resource constraints",
                "Task exceeded max retries",
            ],
        ),
        # Resource Issues
        ErrorPattern(
            category=ErrorCategory.RESOURCE,
            severity="critical",
            patterns=[
                re.compile(r"requires more resource than any of Workers", re.IGNORECASE),
                re.compile(r"Not enough workers", re.IGNORECASE),
                re.compile(r"Could not find.*worker", re.IGNORECASE),
                re.compile(r"No workers available", re.IGNORECASE),
                re.compile(r"Executor.*lost", re.IGNORECASE),
                re.compile(r"Worker.*removed", re.IGNORECASE),
                re.compile(r"Initial job has not accepted any resources"),
            ],
            description="Cluster resources insufficient for job",
            common_causes=[
                "Cluster too small for requested resources",
                "Resource configuration mismatch",
                "Workers crashed or became unhealthy",
                "Memory requirements exceed node capacity",
            ],
        ),
        # Connection Failures
        ErrorPattern(
            category=ErrorCategory.CONNECTION,
            severity="error",
            patterns=[
                re.compile(r"Unable to connect to", re.IGNORECASE),
                re.compile(r"Connection refused"),
                re.compile(r"Connection reset by peer"),
                re.compile(r"Failed to connect", re.IGNORECASE),
                re.compile(r"java\.net\.ConnectException"),
                re.compile(r"java\.net\.SocketException"),
                re.compile(r"Connection timed out"),
            ],
            description="Network connection failure",
            common_causes=[
                "Node unreachable",
                "Firewall blocking ports",
                "Service not running",
                "DNS resolution failure",
                "Network partition",
            ],
        ),
        # Serialization Errors
        ErrorPattern(
            category=ErrorCategory.SERIALIZATION,
            severity="error",
            patterns=[
                re.compile(r"NotSerializableException"),
                re.compile(r"Task not serializable"),
                re.compile(r"java\.io\.NotSerializableException"),
                re.compile(r"Kryo.*serialization", re.IGNORECASE),
                re.compile(r"Failed to serialize", re.IGNORECASE),
                re.compile(r"InvalidClassException"),
            ],
            description="Object serialization failed",
            common_causes=[
                "Non-serializable object in closure",
                "Missing serializer registration",
                "Complex object graph with circular references",
                "Class version mismatch",
            ],
        ),
        # Disk Failures
        ErrorPattern(
            category=ErrorCategory.DISK,
            severity="critical",
            patterns=[
                re.compile(r"No space left on device"),
                re.compile(r"IOException.*disk", re.IGNORECASE),
                re.compile(r"DiskBlockManager.*failed", re.IGNORECASE),
                re.compile(r"Failed to write", re.IGNORECASE),
                re.compile(r"Disk I/O error", re.IGNORECASE),
                re.compile(r"BlockManager.*failed to persist", re.IGNORECASE),
            ],
            description="Disk I/O failure",
            common_causes=[
                "Disk full",
                "Disk hardware failure",
                "Insufficient temp space for shuffle",
                "Permission issues on storage directory",
            ],
        ),
        # Timeout Errors
        ErrorPattern(
            category=ErrorCategory.TIMEOUT,
            severity="warning",
            patterns=[
                re.compile(r"TimeoutException"),
                re.compile(r"timed out", re.IGNORECASE),
                re.compile(r"heartbeat.*timeout", re.IGNORECASE),
                re.compile(r"Connection timed out"),
                re.compile(r"RPC.*timeout", re.IGNORECASE),
                re.compile(r"Executor heartbeat timed out"),
            ],
            description="Operation timeout",
            common_causes=[
                "Slow network",
                "Overloaded node",
                "Long GC pauses",
                "Timeout configured too short",
                "Executor doing expensive computation",
            ],
        ),
        # Configuration Errors
        ErrorPattern(
            category=ErrorCategory.CONFIGURATION,
            severity="error",
            patterns=[
                re.compile(r"Invalid.*configuration", re.IGNORECASE),
                re.compile(r"ClassNotFoundException"),
                re.compile(r"NoSuchMethodError"),
                re.compile(r"NoClassDefFoundError"),
                re.compile(r"IllegalArgumentException.*config", re.IGNORECASE),
                re.compile(r"spark\..*is not set", re.IGNORECASE),
            ],
            description="Configuration or classpath error",
            common_causes=[
                "Missing JAR dependency",
                "Incompatible library versions",
                "Invalid configuration value",
                "Missing required configuration",
            ],
        ),
    ]

    # Patterns that suggest system-level issues (recommend torc analysis)
    TORC_INDICATORS: dict[str, list[re.Pattern[str]]] = {
        "slurm": [
            re.compile(r"SLURM.*CANCELLED", re.IGNORECASE),
            re.compile(r"srun:.*error", re.IGNORECASE),
            re.compile(r"Job.*exceeded.*limit", re.IGNORECASE),
            re.compile(r"PREEMPTED", re.IGNORECASE),
        ],
        "filesystem": [
            re.compile(r"Permission denied"),
            re.compile(r"Stale file handle"),
            re.compile(r"Input/output error"),
            re.compile(r"Read-only file system"),
        ],
        "node_health": [
            re.compile(r"Node.*unhealthy", re.IGNORECASE),
            re.compile(r"Lost executor on.*Node", re.IGNORECASE),
            re.compile(r"Could not reach", re.IGNORECASE),
            re.compile(r"Node blacklisted", re.IGNORECASE),
        ],
    }

    @classmethod
    def classify_error(cls, text: str) -> ErrorPattern | None:
        """Classify text by matching against known error patterns.

        Parameters
        ----------
        text
            The log text to classify (can be multi-line).

        Returns
        -------
        ErrorPattern | None
            The matching ErrorPattern or None if no match.
        """
        for pattern in cls.PATTERNS:
            for regex in pattern.patterns:
                if regex.search(text):
                    return pattern
        return None

    @classmethod
    def should_recommend_torc(cls, text: str) -> tuple[bool, str | None]:
        """Determine if torc analysis should be recommended based on log text.

        Parameters
        ----------
        text
            The log text to check.

        Returns
        -------
        tuple[bool, str | None]
            Tuple of (should_recommend, reason).
        """
        for category, patterns in cls.TORC_INDICATORS.items():
            for pattern in patterns:
                if pattern.search(text):
                    reason_map = {
                        "slurm": "Slurm job management issue detected",
                        "filesystem": "Filesystem/permission issue detected",
                        "node_health": "Node health issue detected",
                    }
                    return True, reason_map.get(category, f"{category} issue detected")
        return False, None

    @classmethod
    def get_root_cause_priority(cls) -> dict[ErrorCategory, int]:
        """Return priority ordering for root cause determination.

        Returns
        -------
        dict[ErrorCategory, int]
            Priority ordering where lower number = more likely to be root cause.
        """
        return {
            ErrorCategory.OOM: 1,
            ErrorCategory.RESOURCE: 2,
            ErrorCategory.DISK: 3,
            ErrorCategory.SHUFFLE: 4,
            ErrorCategory.CONNECTION: 5,
            ErrorCategory.SERIALIZATION: 6,
            ErrorCategory.STAGE: 7,
            ErrorCategory.TIMEOUT: 8,
            ErrorCategory.CONFIGURATION: 9,
            ErrorCategory.UNKNOWN: 10,
        }
