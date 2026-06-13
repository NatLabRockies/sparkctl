import os
from pathlib import Path

import pytest

from sparkctl.exceptions import InvalidConfiguration
from sparkctl.models import BinaryLocations, ComputeEnvironment, ComputeParams, SparkConfig
from sparkctl.slurm_compute import SlurmCompute


@pytest.fixture
def slurm_compute() -> SlurmCompute:
    # Construct the config directly rather than through make_default_spark_config() so that the
    # test does not depend on a user settings file (~/.sparkctl.toml), which is absent in CI.
    config = SparkConfig(
        binaries=BinaryLocations(spark_path=Path("spark"), java_path=Path("java")),
        compute=ComputeParams(environment=ComputeEnvironment.SLURM),
    )
    return SlurmCompute(config)


def test_slurm_is_heterogeneous_job(slurm_compute):
    if "SLURM_HET_SIZE" not in os.environ:
        os.environ["SLURM_HET_SIZE"] = "2"
        set_var = True
    else:
        set_var = False
    try:
        assert slurm_compute.is_heterogeneous_slurm_job()
    finally:
        if set_var:
            os.environ.pop("SLURM_HET_SIZE")


def test_slurm_get_worker_num_cpus_not_het(slurm_compute):
    orig = os.getenv("SLURM_CPUS_ON_NODE")
    os.environ["SLURM_CPUS_ON_NODE"] = "104"
    try:
        assert slurm_compute.get_worker_num_cpus() == 104
    finally:
        if orig is None:
            os.environ.pop("SLURM_CPUS_ON_NODE")
        else:
            os.environ["SLURM_CPUS_ON_NODE"] = orig


def test_slurm_get_worker_num_cpus_het(slurm_compute):
    orig = os.getenv("SLURM_JOB_CPUS_PER_NODE_HET_GROUP_1")
    os.environ["SLURM_JOB_CPUS_PER_NODE_HET_GROUP_1"] = "104(x4)"
    orig_cpus = os.getenv("SLURM_CPUS_ON_NODE")
    os.environ["SLURM_CPUS_ON_NODE"] = "36"
    try:
        assert slurm_compute.get_worker_num_cpus() == 104
    finally:
        for key, orig_val in (
            ("SLURM_JOB_CPUS_PER_NODE_HET_GROUP_1", orig),
            ("SLURM_CPUS_ON_NODE", orig_cpus),
        ):
            if orig_val is None:
                os.environ.pop(key)
            else:
                os.environ[key] = orig_val


def test_check_gpu_allocation_uneven_raises(slurm_compute, monkeypatch):
    # 2 worker nodes, this node has 3 GPUs, but the job total is 4 -> 3 * 2 != 4, so the GPUs were
    # split unevenly (e.g. salloc --gpus=4 -N2 giving 3 + 1).
    monkeypatch.setattr(slurm_compute, "get_num_workers", lambda: 2)
    monkeypatch.setattr(slurm_compute, "get_worker_num_gpus", lambda: 3)
    monkeypatch.delenv("SLURM_HET_SIZE", raising=False)
    monkeypatch.delenv("SLURM_GPUS_PER_NODE", raising=False)
    monkeypatch.setenv("SLURM_GPUS", "4")
    with pytest.raises(InvalidConfiguration, match="evenly distributed"):
        slurm_compute.check_gpu_allocation()


def test_check_gpu_allocation_even_ok(slurm_compute, monkeypatch):
    # 2 worker nodes, 4 GPUs each, job total 8 -> evenly distributed.
    monkeypatch.setattr(slurm_compute, "get_num_workers", lambda: 2)
    monkeypatch.setattr(slurm_compute, "get_worker_num_gpus", lambda: 4)
    monkeypatch.delenv("SLURM_HET_SIZE", raising=False)
    monkeypatch.delenv("SLURM_GPUS_PER_NODE", raising=False)
    monkeypatch.setenv("SLURM_GPUS", "8")
    slurm_compute.check_gpu_allocation()


def test_check_gpu_allocation_per_node_request_skips_check(slurm_compute, monkeypatch):
    # --gpus-per-node guarantees a uniform count, so the arithmetic check is skipped even when
    # SLURM_GPUS would not divide evenly.
    monkeypatch.setattr(slurm_compute, "get_num_workers", lambda: 2)
    monkeypatch.setattr(slurm_compute, "get_worker_num_gpus", lambda: 4)
    monkeypatch.delenv("SLURM_HET_SIZE", raising=False)
    monkeypatch.setenv("SLURM_GPUS_PER_NODE", "4")
    monkeypatch.setenv("SLURM_GPUS", "4")
    slurm_compute.check_gpu_allocation()


def test_check_gpu_allocation_single_node_ok(slurm_compute, monkeypatch):
    # A single worker node cannot be uneven, so no check applies.
    monkeypatch.setattr(slurm_compute, "get_num_workers", lambda: 1)
    monkeypatch.setattr(slurm_compute, "get_worker_num_gpus", lambda: 3)
    monkeypatch.delenv("SLURM_GPUS_PER_NODE", raising=False)
    monkeypatch.setenv("SLURM_GPUS", "4")
    slurm_compute.check_gpu_allocation()


# TODO: get_node_names
