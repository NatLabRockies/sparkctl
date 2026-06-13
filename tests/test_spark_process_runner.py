import os
import signal
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from sparkctl.exceptions import ExecutionError
from sparkctl.models import (
    BinaryLocations,
    ComputeEnvironment,
    ComputeParams,
    RuntimeDirectories,
    SparkConfig,
)
from sparkctl.spark_process_runner import SparkProcessRunner


SPARK_URL = "spark://node1:7077"


def make_config(
    tmp_path: Path, environment: ComputeEnvironment, use_srun: bool = True
) -> SparkConfig:
    config = SparkConfig(
        compute=ComputeParams(environment=environment, use_srun=use_srun),
        binaries=BinaryLocations(spark_path=tmp_path / "spark", java_path=tmp_path / "java"),
        directories=RuntimeDirectories(base=tmp_path, spark_scratch=tmp_path / "spark_scratch"),
    )
    config.directories.get_spark_conf_dir().mkdir()
    return config


class FakePopen:
    def __init__(self, cmd, returncode=None, **kwargs):
        self.cmd = cmd
        self.pid = 12345
        self._returncode = returncode
        self.kwargs = kwargs

    def poll(self):
        return self._returncode


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)


def test_start_worker_processes_srun(tmp_path, monkeypatch, no_sleep):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["proc"] = FakePopen(cmd, **kwargs)
        return captured["proc"]

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.delenv("SLURM_HET_SIZE", raising=False)
    runner = SparkProcessRunner(config, SPARK_URL)
    runner.start_worker_processes(["node1", "node2"], 80, num_cpus_per_worker=104)

    cmd = captured["proc"].cmd
    assert cmd[0] == "srun"
    assert "--nodes=2" in cmd
    assert "--ntasks=2" in cmd
    assert "--ntasks-per-node=1" in cmd
    assert "--nodelist=node1,node2" in cmd
    assert "--cpus-per-task=104" in cmd
    assert not any(x.startswith("--het-group") for x in cmd)
    # The background srun must not read the terminal, or it steals the interactive shell's input.
    assert "--input=none" in cmd
    assert captured["proc"].kwargs["stdin"] == subprocess.DEVNULL

    content = Path(cmd[-1]).read_text(encoding="utf-8")
    assert "export SPARK_NO_DAEMONIZE=true" in content
    assert f"exec {config.binaries.spark_path}/sbin/start-worker.sh {SPARK_URL} -m 80g" in content

    pid_file = config.directories.base / "srun_workers.pid"
    assert pid_file.read_text(encoding="utf-8").strip() == "12345"


def test_start_worker_processes_srun_heterogeneous(tmp_path, monkeypatch, no_sleep):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    captured = {}

    def fake_popen(cmd, **kwargs):
        captured["proc"] = FakePopen(cmd, **kwargs)
        return captured["proc"]

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setenv("SLURM_HET_SIZE", "2")
    runner = SparkProcessRunner(config, SPARK_URL)
    runner.start_worker_processes(["node2", "node3"], 80)

    cmd = captured["proc"].cmd
    assert "--het-group=1" in cmd
    assert not any(x.startswith("--nodelist") for x in cmd)
    assert not any(x.startswith("--cpus-per-task") for x in cmd)


def test_start_worker_processes_srun_immediate_failure(tmp_path, monkeypatch, no_sleep):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kwargs: FakePopen(cmd, returncode=1))
    runner = SparkProcessRunner(config, SPARK_URL)
    with pytest.raises(ExecutionError):
        runner.start_worker_processes(["node1", "node2"], 80)
    assert not (config.directories.base / "srun_workers.pid").exists()


def test_start_worker_processes_native_uses_ssh(tmp_path, monkeypatch):
    config = make_config(tmp_path, ComputeEnvironment.NATIVE)
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    runner = SparkProcessRunner(config, SPARK_URL)
    runner.start_worker_processes(["node1", "node2"], 80)

    # The workers are started concurrently, so the command order is not deterministic.
    assert len(commands) == 2
    assert [cmd[0] for cmd in commands] == ["ssh", "ssh"]
    assert {cmd[1] for cmd in commands} == {"node1", "node2"}


def test_start_worker_processes_slurm_use_srun_disabled_uses_ssh(tmp_path, monkeypatch):
    config = make_config(tmp_path, ComputeEnvironment.SLURM, use_srun=False)
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0)

    def fail_popen(cmd, **kwargs):
        msg = "srun must not be used when compute.use_srun is disabled"
        raise AssertionError(msg)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(subprocess, "Popen", fail_popen)
    runner = SparkProcessRunner(config, SPARK_URL)
    runner.start_worker_processes(["node1", "node2"], 80, num_cpus_per_worker=104)

    assert [cmd[0] for cmd in commands] == ["ssh", "ssh"]
    assert not (config.directories.base / "srun_workers.pid").exists()


def test_stop_worker_processes_slurm_use_srun_disabled_uses_ssh(tmp_path, monkeypatch):
    config = make_config(tmp_path, ComputeEnvironment.SLURM, use_srun=False)
    commands = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    runner = SparkProcessRunner(config, SPARK_URL)
    assert runner.stop_worker_processes(["node1", "node2"]) == 0
    assert [cmd[0] for cmd in commands] == ["ssh", "ssh"]


def test_stop_worker_processes_srun(tmp_path, monkeypatch, no_sleep):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    pid_file = config.directories.base / "srun_workers.pid"
    pid_file.write_text("12345\n", encoding="utf-8")
    kill_calls = []

    def fake_kill(pid, sig):
        kill_calls.append((pid, sig))
        if sig == 0:
            raise ProcessLookupError

    def fake_waitpid(pid, flags):
        raise ChildProcessError

    monkeypatch.setattr(os, "kill", fake_kill)
    monkeypatch.setattr(os, "waitpid", fake_waitpid)
    runner = SparkProcessRunner(config, SPARK_URL)
    assert runner.stop_worker_processes(["node1", "node2"]) == 0
    assert (12345, signal.SIGTERM) in kill_calls
    assert not pid_file.exists()


def test_stop_worker_processes_srun_missing_pid_file(tmp_path):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    runner = SparkProcessRunner(config, SPARK_URL)
    assert runner.stop_worker_processes(["node1", "node2"]) == 1


def test_stop_worker_processes_srun_stops_rmon(tmp_path, monkeypatch, no_sleep):
    config = make_config(tmp_path, ComputeEnvironment.SLURM)
    config.resource_monitor.enabled = True
    pid_file = config.directories.base / "srun_workers.pid"
    pid_file.write_text("12345\n", encoding="utf-8")

    captured = {}
    kill_order = []

    def fake_run(cmd, **kwargs):
        # Record the rmon-stop command and the generated script before it is unlinked.
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        captured["script"] = Path(cmd[-1]).read_text(encoding="utf-8")
        kill_order.append("stop_rmon_srun")
        return SimpleNamespace(returncode=0)

    def fake_kill(pid, sig):
        if sig == signal.SIGTERM:
            kill_order.append("kill_worker_srun")
        elif sig == 0:
            raise ProcessLookupError

    def fake_waitpid(pid, flags):
        raise ChildProcessError

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(os, "kill", fake_kill)
    monkeypatch.setattr(os, "waitpid", fake_waitpid)
    monkeypatch.delenv("SLURM_HET_SIZE", raising=False)

    runner = SparkProcessRunner(config, SPARK_URL)
    assert runner.stop_worker_processes(["node1", "node2"]) == 0

    cmd = captured["cmd"]
    assert cmd[0] == "srun"
    assert "--overlap" in cmd
    assert "--nodelist=node1,node2" in cmd
    assert "--input=none" in cmd
    assert captured["kwargs"]["stdin"] == subprocess.DEVNULL
    assert "kill -TERM" in captured["script"]
    assert "rmon_$(hostname).pid" in captured["script"]
    # rmon must be stopped before the worker job step is torn down.
    assert kill_order == ["stop_rmon_srun", "kill_worker_srun"]
    assert not pid_file.exists()
    assert not (config.directories.get_spark_conf_dir() / "tmp_stop_rmon.sh").exists()
