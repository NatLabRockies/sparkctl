import multiprocessing
import shutil
import subprocess
import tempfile
from pathlib import Path
from socket import gethostname

import psutil

from sparkctl.compute_interface import ComputeInterface


class NativeCompute(ComputeInterface):
    """Provides interface to Slurm."""

    def get_node_memory_overhead_gb(
        self, driver_memory_gb: int, node_memory_overhead_gb: int
    ) -> int:
        return driver_memory_gb + node_memory_overhead_gb

    def get_num_workers(self) -> int:
        return 1

    def get_node_names(self) -> list[str]:
        return [gethostname()]

    def get_scratch_dir(self) -> Path:
        return Path(tempfile.gettempdir())

    def get_worker_node_names(self) -> list[str]:
        return self.get_node_names()

    def get_worker_memory_gb(self) -> int:
        # Spark documentation recommends only using 75% of system memory,
        # leaving the rest for the OS and buffer cache.
        # https://spark.apache.org/docs/latest/hardware-provisioning.html#memory
        return int(psutil.virtual_memory().total / (1024 * 1024 * 1024) * 3 / 4)

    def get_worker_num_cpus(self) -> int:
        return multiprocessing.cpu_count()

    def get_worker_num_gpus(self) -> int:
        # Best-effort detection via nvidia-smi. Returns 0 when it is unavailable or fails so that
        # GPU support stays opt-in and never blocks a normal CPU-only run.
        nvidia_smi = shutil.which("nvidia-smi")
        if nvidia_smi is None:
            return 0
        try:
            proc = subprocess.run(
                [nvidia_smi, "--query-gpu=name", "--format=csv,noheader"],
                capture_output=True,
                check=True,
                text=True,
            )
        except (subprocess.CalledProcessError, OSError):
            return 0
        return len([x for x in proc.stdout.splitlines() if x.strip()])

    def is_heterogeneous_slurm_job(self) -> bool:
        return False

    def is_worker_node(self, node_name: str) -> bool:
        return True

    def run_checks(self) -> None:
        pass
