import socket
import time
from pathlib import Path

import psutil
import pytest

from sparkctl import (
    ClusterManager,
    SparkConfig,
)
from sparkctl.exceptions import InvalidConfiguration


def test_cluster_manager_workers(setup_local_env: tuple[SparkConfig, Path]):
    config, _ = setup_local_env
    mgr = ClusterManager.from_config(config)
    mgr.configure()
    workers = mgr.get_workers()
    assert workers == [socket.gethostname()]
    new_workers = ["worker1", "worker2"]
    mgr.set_workers(new_workers)
    assert mgr.get_workers() == new_workers


def test_configure_reverse_proxy_and_prometheus(setup_local_env: tuple[SparkConfig, Path]):
    config, tmp_path = setup_local_env
    config.directories.spark_scratch = tmp_path / "spark_scratch"
    config.runtime.enable_reverse_proxy = True
    config.runtime.reverse_proxy_url = "http://login01:8080"
    config.runtime.enable_prometheus = True
    mgr = ClusterManager.from_config(config)
    mgr.configure()
    defaults = config.directories.get_spark_defaults_file().read_text(encoding="utf-8")
    assert "spark.ui.reverseProxy true" in defaults
    assert "spark.ui.reverseProxyUrl http://login01:8080" in defaults
    assert "spark.ui.prometheus.enabled true" in defaults
    metrics = config.directories.get_metrics_properties_file().read_text(encoding="utf-8")
    assert "PrometheusServlet" in metrics


def test_configure_gpus_without_detection_fails(setup_local_env: tuple[SparkConfig, Path]):
    config, tmp_path = setup_local_env
    config.directories.spark_scratch = tmp_path / "spark_scratch"
    # FakeCompute reports zero GPUs and no override is set.
    config.runtime.enable_gpus = True
    mgr = ClusterManager.from_config(config)
    with pytest.raises(InvalidConfiguration):
        mgr.configure()


def test_configure_gpus_with_override(setup_local_env: tuple[SparkConfig, Path]):
    config, tmp_path = setup_local_env
    config.directories.spark_scratch = tmp_path / "spark_scratch"
    config.runtime.enable_gpus = True
    config.runtime.gpus_per_node = 4
    mgr = ClusterManager.from_config(config)
    mgr.configure()
    defaults = config.directories.get_spark_defaults_file().read_text(encoding="utf-8")
    assert "spark.worker.resource.gpu.amount 4" in defaults
    assert "spark.executor.resource.gpu.discoveryScript" in defaults
    discovery_script = config.directories.get_gpu_discovery_script_file()
    assert discovery_script.exists()
    assert discovery_script.stat().st_mode & 0o100  # owner-executable


def test_configure_rapids_without_jar_fails(setup_local_env: tuple[SparkConfig, Path]):
    config, tmp_path = setup_local_env
    config.directories.spark_scratch = tmp_path / "spark_scratch"
    config.runtime.gpus_per_node = 2
    config.runtime.enable_rapids = True
    config.binaries.rapids_jar_file = None
    mgr = ClusterManager.from_config(config)
    with pytest.raises(InvalidConfiguration):
        mgr.configure()


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
