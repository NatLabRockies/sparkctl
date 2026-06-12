import socket
import time
from pathlib import Path

import psutil
import pytest

from sparkctl import (
    ClusterManager,
    SparkConfig,
)


def test_cluster_manager_workers(setup_local_env: tuple[SparkConfig, Path]):
    config, _ = setup_local_env
    mgr = ClusterManager.from_config(config)
    mgr.configure()
    workers = mgr.get_workers()
    assert workers == [socket.gethostname()]
    new_workers = ["worker1", "worker2"]
    mgr.set_workers(new_workers)
    assert mgr.get_workers() == new_workers


@pytest.mark.integration
def test_managed_start(setup_local_env: tuple[SparkConfig, Path]):
    config, output_dir = setup_local_env
    config.directories.base = output_dir
    config.directories.spark_scratch = output_dir / "spark_scratch"
    config.directories.metastore_dir = output_dir / "metastore_db"
    config.resource_monitor.enabled = True
    assert not is_rmon_running()
    mgr = ClusterManager.from_config(config)
    mgr.configure()
    with mgr.managed_cluster() as spark:
        df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        assert df.count() == 2
    assert wait_for_rmon_to_stop()


def wait_for_rmon_to_stop(timeout: int = 30):
    end = time.time() + timeout
    while time.time() < end:
        if not is_rmon_running():
            return True
        time.sleep(0.2)
    return False


def is_rmon_running() -> bool:
    for proc in psutil.process_iter(["name", "cmdline"]):
        if "python" in proc.info["name"] and any(("rmon" in x for x in proc.info["cmdline"])):
            return True
    return False
