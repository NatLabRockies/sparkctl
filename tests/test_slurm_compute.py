import os
from pathlib import Path

import pytest

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


# TODO: get_node_names
