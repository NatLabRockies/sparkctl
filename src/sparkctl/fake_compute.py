import tempfile
from pathlib import Path
from socket import gethostname

from sparkctl.compute_interface import ComputeInterface


class FakeCompute(ComputeInterface):
    """Provides interface to Slurm."""

    def get_node_memory_overhead_gb(
        self, driver_memory_gb: int, node_memory_overhead_gb: int
    ) -> int:
        return driver_memory_gb + self._config.runtime.node_memory_overhead_gb

    def get_num_workers(self) -> int:
        return 1

    def get_node_names(self) -> list[str]:
        return [gethostname()]

    def get_scratch_dir(self) -> Path:
        return Path(tempfile.gettempdir())

    def get_worker_node_names(self) -> list[str]:
        return self.get_node_names()

    def get_worker_memory_gb(self) -> int:
        return 32

    def get_worker_num_cpus(self) -> int:
        return 12

    def is_heterogeneous_slurm_job(self) -> bool:
        return False

    def is_worker_node(self, node_name: str) -> bool:
        return True

    def run_checks(self) -> None:
        pass
