import os
import re
import subprocess
from pathlib import Path
from socket import gethostname

from sparkctl.compute_interface import ComputeInterface
from sparkctl.models import SparkConfig


class SlurmCompute(ComputeInterface):
    """Provides interface to Slurm."""

    def __init__(self, config: SparkConfig) -> None:
        super().__init__(config)
        # The node list does not change during a job, so cache it to avoid repeatedly shelling
        # out to squeue/scontrol (get_node_names is called several times per configure/stop).
        self._node_names: list[str] | None = None

    def get_node_memory_overhead_gb(
        self, driver_memory_gb: int, node_memory_overhead_gb: int
    ) -> int:
        if self.is_heterogeneous_slurm_job():
            return node_memory_overhead_gb

        return driver_memory_gb + node_memory_overhead_gb

    def get_num_workers(self) -> int:
        master_node = gethostname()
        num_workers = 0
        node_names = self.get_node_names()
        is_het = self.is_heterogeneous_slurm_job()
        for node_name in node_names:
            if not is_het or node_name != master_node:
                num_workers += 1

        return num_workers

    def get_node_names(self) -> list[str]:
        if self._node_names is None:
            self._node_names = get_node_names(os.environ["SLURM_JOB_ID"])
        return self._node_names

    def get_worker_node_names(self) -> list[str]:
        node_names = self.get_node_names()
        if self.is_heterogeneous_slurm_job():
            return [x for x in node_names if x != gethostname()]
        return node_names

    def get_scratch_dir(self) -> Path:
        return Path(os.environ["TMPDIR"])

    def get_worker_memory_gb(self) -> int:
        # Spark documentation recommends only using 75% of system memory,
        # leaving the rest for the OS and buffer cache.
        # https://spark.apache.org/docs/latest/hardware-provisioning.html#memory
        het_memory_group1 = os.getenv("SLURM_MEM_PER_NODE_HET_GROUP_1")
        if het_memory_group1 is not None:
            memory_gb = int(het_memory_group1) / 1024 * 3 / 4
        else:
            memory_gb = int(os.environ["SLURM_MEM_PER_NODE"]) / 1024 * 3 / 4

        return int(memory_gb)

    def get_worker_num_cpus(self) -> int:
        het_cpus = os.getenv("SLURM_JOB_CPUS_PER_NODE_HET_GROUP_1")
        if het_cpus is not None:
            # Example of 4 nodes with 104 CPUs each: 104(x4)
            regex = re.compile(r"^(\d+)\(x\d+")
            match = regex.search(het_cpus)
            if match is None:
                msg = f"Failed to parse SLURM_JOB_CPUS_PER_NODE_HET_GROUP_1 = {het_cpus}"
                raise ValueError(msg)
            num_cpus = match.group(1)
        else:
            num_cpus = os.environ["SLURM_CPUS_ON_NODE"]

        return int(num_cpus)

    def is_heterogeneous_slurm_job(self) -> bool:
        return "SLURM_HET_SIZE" in os.environ

    def is_worker_node(self, node_name: str) -> bool:
        return node_name == os.getenv("SLURM_NODELIST_HET_GROUP_0", "")

    def run_checks(self) -> None:
        if "SLURM_MEM_PER_NODE" not in os.environ:
            msg = (
                "The SLURM_MEM_PER_NODE environment variable is not set. "
                "Please submit the Slurm job with --mem, such as --mem=240000, or set the variable."
            )
            raise ValueError(msg)

        if self.is_heterogeneous_slurm_job():
            het_size = int(os.getenv("SLURM_HET_SIZE", "0"))
            if het_size > 2:
                msg = f"A heterogeneous job can only have two groups: {het_size}"
                raise ValueError(msg)

            het_group_0 = int(os.getenv("SLURM_JOB_NUM_NODES_HET_GROUP_0", "0"))
            if het_group_0 != 1:
                msg = f"SLURM_JOBID_HET_GROUP_0 can only have one node: {het_group_0}"
                raise ValueError(msg)


def get_node_names(job_id: str) -> list[str]:
    # The squeue command will produce multiple lines if the job is heterogeneous.
    proc = subprocess.run(
        ["squeue", "-j", job_id, "--format", '"%5D %1000N"', "-h"], capture_output=True, check=True
    )
    host_lists = [x.strip().split()[1] for x in proc.stdout.decode("utf-8").splitlines() if x]
    final: list[str] = []
    for hosts in host_lists:
        proc = subprocess.run(
            ["scontrol", "show", "hostnames", hosts], capture_output=True, check=True
        )
        final += proc.stdout.decode("utf-8").split()
    return final
