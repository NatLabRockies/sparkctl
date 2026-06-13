import abc
from pathlib import Path

from sparkctl.models import SparkConfig


class ComputeInterface(abc.ABC):
    """Interface for compute environments"""

    def __init__(self, config: SparkConfig) -> None:
        self._config = config

    @abc.abstractmethod
    def get_node_memory_overhead_gb(
        self, driver_memory_gb: int, node_memory_overhead_gb: int
    ) -> int:
        """Return the node memory overhead in GB."""

    @abc.abstractmethod
    def get_node_names(self) -> list[str]:
        """Return the node names in the job."""

    @abc.abstractmethod
    def get_scratch_dir(self) -> Path:
        """Return a directory that can be used for Spark shuffle data.."""

    @abc.abstractmethod
    def get_worker_node_names(self) -> list[str]:
        """Return the node names in the job that will be used as workers."""

    @abc.abstractmethod
    def get_num_workers(self) -> int:
        """Return the number of workers in the cluster."""

    @abc.abstractmethod
    def get_worker_memory_gb(self) -> int:
        """Return the worker memory in GB."""

    @abc.abstractmethod
    def get_worker_num_cpus(self) -> int:
        """Return the number of CPUs in the compute node"""

    @abc.abstractmethod
    def get_worker_num_gpus(self) -> int:
        """Return the number of GPUs in the compute node. Returns 0 if none are detected."""

    @abc.abstractmethod
    def is_heterogeneous_slurm_job(self) -> bool:
        """Return True if the environment indicates a heterogeneous Slurm job."""

    @abc.abstractmethod
    def is_worker_node(self, node_name: str) -> bool:
        """Return True if the node is a worker node."""

    @abc.abstractmethod
    def run_checks(self) -> None:
        """Run checks on Slurm environment variables."""
