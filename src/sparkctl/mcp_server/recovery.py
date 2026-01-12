"""Recovery suggestion engine for Spark failures."""

from sparkctl.mcp_server.models import (
    ErrorCategory,
    RecoverySuggestions,
    Suggestion,
)


class RecoveryEngine:
    """Generates recovery suggestions based on detected errors."""

    # Recovery strategies organized by error category
    RECOVERY_STRATEGIES: dict[ErrorCategory, list[Suggestion]] = {
        ErrorCategory.OOM: [
            Suggestion(
                priority=1,
                category="memory",
                title="Increase executor memory",
                description=(
                    "Allocate more memory per executor to handle larger data. "
                    "Double the current executor memory as a starting point."
                ),
                config_changes={"spark.executor.memory": "16g"},
                sparkctl_command="sparkctl configure --executor-memory-gb 16",
                estimated_impact="High - directly addresses memory pressure",
            ),
            Suggestion(
                priority=2,
                category="memory",
                title="Reduce executor cores",
                description=(
                    "Fewer cores per executor means more memory per task. "
                    "This trades parallelism for memory headroom."
                ),
                config_changes={"spark.executor.cores": "2"},
                sparkctl_command="sparkctl configure --executor-cores 2",
                estimated_impact="Medium - trades parallelism for memory headroom",
            ),
            Suggestion(
                priority=3,
                category="shuffle",
                title="Increase shuffle partitions",
                description=(
                    "More partitions means smaller data chunks per task. "
                    "Increase the shuffle partition multiplier to spread data more evenly."
                ),
                config_changes={"spark.sql.shuffle.partitions": "400"},
                sparkctl_command="sparkctl configure --shuffle-partition-multiplier 2",
                estimated_impact="Medium - reduces memory per partition",
            ),
            Suggestion(
                priority=4,
                category="memory",
                title="Enable off-heap memory",
                description=(
                    "Use off-heap memory for caching and shuffle to reduce GC pressure. "
                    "Requires additional configuration in spark-defaults.conf."
                ),
                config_changes={
                    "spark.memory.offHeap.enabled": "true",
                    "spark.memory.offHeap.size": "4g",
                },
                sparkctl_command=None,
                estimated_impact="Medium - reduces GC pressure",
            ),
        ],
        ErrorCategory.SHUFFLE: [
            Suggestion(
                priority=1,
                category="shuffle",
                title="Increase shuffle partitions",
                description=(
                    "Shuffle block too large errors occur when partitions exceed 2GB. "
                    "Increase partitions to reduce individual shuffle block sizes."
                ),
                config_changes={"spark.sql.shuffle.partitions": "800"},
                sparkctl_command="sparkctl configure --shuffle-partition-multiplier 4",
                estimated_impact="High - prevents 2GB shuffle block limit",
            ),
            Suggestion(
                priority=2,
                category="shuffle",
                title="Enable shuffle compression",
                description=("Compress shuffle data to reduce network transfer and disk usage."),
                config_changes={
                    "spark.shuffle.compress": "true",
                    "spark.shuffle.spill.compress": "true",
                },
                sparkctl_command=None,
                estimated_impact="Medium - reduces shuffle data size",
            ),
            Suggestion(
                priority=3,
                category="resource",
                title="Use local storage for shuffle",
                description=(
                    "Use fast local disks instead of shared filesystem for shuffle. "
                    "Significantly improves shuffle I/O performance."
                ),
                config_changes=None,
                sparkctl_command="sparkctl configure --local-storage",
                estimated_impact="High - faster shuffle I/O",
            ),
            Suggestion(
                priority=4,
                category="network",
                title="Increase network timeout",
                description=("Allow more time for shuffle block transfers on slow networks."),
                config_changes={
                    "spark.network.timeout": "600s",
                    "spark.shuffle.io.maxRetries": "6",
                },
                sparkctl_command=None,
                estimated_impact="Medium - prevents premature timeouts",
            ),
        ],
        ErrorCategory.RESOURCE: [
            Suggestion(
                priority=1,
                category="resource",
                title="Reduce executor resource requirements",
                description=(
                    "Lower executor memory/cores to fit within worker capacity. "
                    "Check that requested resources don't exceed any single worker's limits."
                ),
                config_changes=None,
                sparkctl_command="sparkctl configure --executor-cores 4 --executor-memory-gb 8",
                estimated_impact="High - ensures jobs can be scheduled",
            ),
            Suggestion(
                priority=2,
                category="resource",
                title="Enable dynamic allocation",
                description=(
                    "Let Spark scale executors based on workload demand. "
                    "Helps with resource utilization and prevents resource starvation."
                ),
                config_changes=None,
                sparkctl_command="sparkctl configure --dynamic-allocation",
                estimated_impact="Medium - better resource utilization",
            ),
            Suggestion(
                priority=3,
                category="resource",
                title="Check cluster health",
                description=(
                    "Verify all workers are running and healthy. "
                    "Use sparkctl status or check Spark UI for worker status."
                ),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="High - identifies infrastructure issues",
            ),
        ],
        ErrorCategory.DISK: [
            Suggestion(
                priority=1,
                category="disk",
                title="Use local storage",
                description=(
                    "Use node-local storage for shuffle and temp data. "
                    "Provides dedicated disk space separate from shared filesystem."
                ),
                config_changes=None,
                sparkctl_command="sparkctl configure --local-storage",
                estimated_impact="High - dedicated local disk space",
            ),
            Suggestion(
                priority=2,
                category="disk",
                title="Clean up scratch directory",
                description=(
                    "Remove old application data from spark_scratch to free disk space. "
                    "Old executor logs and shuffle data can accumulate."
                ),
                config_changes=None,
                sparkctl_command="rm -rf spark_scratch/workers/app-* spark_scratch/local/*",
                estimated_impact="High - frees disk space immediately",
            ),
            Suggestion(
                priority=3,
                category="shuffle",
                title="Enable shuffle compression",
                description=("Compress shuffle data to reduce disk usage during spills."),
                config_changes={
                    "spark.shuffle.compress": "true",
                    "spark.shuffle.spill.compress": "true",
                },
                sparkctl_command=None,
                estimated_impact="Medium - reduces disk space for shuffle",
            ),
        ],
        ErrorCategory.CONNECTION: [
            Suggestion(
                priority=1,
                category="network",
                title="Increase network timeout",
                description=(
                    "Increase timeout for RPC and network operations. "
                    "Helps with transient network issues."
                ),
                config_changes={
                    "spark.network.timeout": "600s",
                    "spark.rpc.askTimeout": "600s",
                },
                sparkctl_command=None,
                estimated_impact="Medium - prevents premature timeouts",
            ),
            Suggestion(
                priority=2,
                category="network",
                title="Check network connectivity",
                description=(
                    "Verify all nodes can communicate on required ports. "
                    "Spark uses various ports for master/worker communication."
                ),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="High - identifies network issues",
            ),
            Suggestion(
                priority=3,
                category="resource",
                title="Restart the cluster",
                description=(
                    "Connection issues may resolve after a cluster restart. "
                    "Stop and start the cluster to reset all connections."
                ),
                config_changes=None,
                sparkctl_command="sparkctl stop && sparkctl start",
                estimated_impact="Medium - resets connection state",
            ),
        ],
        ErrorCategory.SERIALIZATION: [
            Suggestion(
                priority=1,
                category="code",
                title="Check for non-serializable objects",
                description=(
                    "Ensure all objects used in closures are serializable. "
                    "Move non-serializable objects outside of closures or use broadcast variables."
                ),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="High - fixes root cause",
            ),
            Suggestion(
                priority=2,
                category="configuration",
                title="Use Kryo serialization",
                description=(
                    "Kryo is faster and more compact than Java serialization. "
                    "May require registering custom classes."
                ),
                config_changes={
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                },
                sparkctl_command=None,
                estimated_impact="Medium - better serialization performance",
            ),
        ],
        ErrorCategory.TIMEOUT: [
            Suggestion(
                priority=1,
                category="configuration",
                title="Increase heartbeat interval",
                description=("Increase executor heartbeat timeout for long-running tasks."),
                config_changes={
                    "spark.executor.heartbeatInterval": "60s",
                    "spark.network.timeout": "600s",
                },
                sparkctl_command=None,
                estimated_impact="Medium - prevents false timeouts",
            ),
            Suggestion(
                priority=2,
                category="memory",
                title="Reduce GC pressure",
                description=(
                    "Long GC pauses can cause heartbeat timeouts. "
                    "Increase memory or enable off-heap to reduce GC."
                ),
                config_changes={"spark.executor.memory": "16g"},
                sparkctl_command="sparkctl configure --executor-memory-gb 16",
                estimated_impact="Medium - reduces GC pauses",
            ),
        ],
        ErrorCategory.STAGE: [
            Suggestion(
                priority=1,
                category="retry",
                title="Increase task max failures",
                description=("Allow more retries for transient failures."),
                config_changes={"spark.task.maxFailures": "8"},
                sparkctl_command=None,
                estimated_impact="Low - masks underlying issues",
            ),
            Suggestion(
                priority=2,
                category="resource",
                title="Check executor logs",
                description=(
                    "Stage failures are usually caused by task failures. "
                    "Check executor logs for the root cause."
                ),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="High - identifies root cause",
            ),
        ],
        ErrorCategory.CONFIGURATION: [
            Suggestion(
                priority=1,
                category="configuration",
                title="Check classpath and dependencies",
                description=(
                    "ClassNotFoundException usually indicates missing JARs. "
                    "Ensure all required dependencies are available to executors."
                ),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="High - fixes missing dependencies",
            ),
            Suggestion(
                priority=2,
                category="configuration",
                title="Verify configuration values",
                description=("Check spark-defaults.conf for invalid configuration values."),
                config_changes=None,
                sparkctl_command=None,
                estimated_impact="Medium - fixes configuration errors",
            ),
        ],
    }

    def get_suggestions(
        self,
        error_categories: list[ErrorCategory],
        current_config: dict[str, str] | None = None,
    ) -> RecoverySuggestions:
        """Generate recovery suggestions based on detected error categories.

        Parameters
        ----------
        error_categories
            List of error categories detected.
        current_config
            Current Spark configuration (optional, for context).

        Returns
        -------
        RecoverySuggestions
            RecoverySuggestions with prioritized remediation steps.
        """
        suggestions: list[Suggestion] = []
        seen_titles: set[str] = set()

        # Collect suggestions for each error category
        for category in error_categories:
            if category in self.RECOVERY_STRATEGIES:
                for suggestion in self.RECOVERY_STRATEGIES[category]:
                    # Avoid duplicate suggestions
                    if suggestion.title not in seen_titles:
                        suggestions.append(suggestion)
                        seen_titles.add(suggestion.title)

        # Sort by priority
        suggestions.sort(key=lambda s: s.priority)

        # Determine if cluster restart is needed
        restart_categories = {
            "memory",
            "resource",
        }
        requires_restart = any(s.category in restart_categories for s in suggestions)

        # Determine if torc analysis is recommended
        system_categories = {
            ErrorCategory.CONNECTION,
            ErrorCategory.DISK,
            ErrorCategory.TIMEOUT,
        }
        recommend_torc = bool(set(error_categories) & system_categories)

        torc_recommendation = None
        if recommend_torc:
            torc_recommendation = (
                "System-level issues detected (connection, disk, or timeout errors). "
                "Use torc's analyze_workflow_logs tool to check for Slurm job failures, "
                "node health issues, or filesystem problems that may be causing Spark failures."
            )

        return RecoverySuggestions(
            suggestions=suggestions,
            requires_cluster_restart=requires_restart,
            recommend_torc_analysis=recommend_torc,
            torc_recommendation=torc_recommendation,
        )
