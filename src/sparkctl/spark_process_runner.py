import os
import re
import shlex
import shutil
import signal
import stat
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from socket import gethostname
from typing import Any

from loguru import logger

from sparkctl.exceptions import ExecutionError
from sparkctl.models import ComputeEnvironment, SparkConfig


class SparkProcessRunner:
    """Runs Spark processes."""

    def __init__(self, config: SparkConfig, url: str) -> None:
        self._spark_path = config.binaries.spark_path
        self._java_path = config.binaries.java_path
        self._conf_dir = config.directories.get_spark_conf_dir()
        self._config = config
        self._url = url

    def start_master_process(self) -> None:
        """Start the Spark master process."""
        self._check_run_command(self._start_master_cmd())

    def stop_master_process(self) -> int:
        """Stop the Spark master process."""
        return self._run_command(self._stop_master_cmd())

    def start_connect_server(self) -> None:
        """Start the Spark connect server."""
        port = self._config.runtime.connect_server_port
        cmd = (
            f"{self._start_connect_server_cmd()} --master {self._url} "
            f"--conf spark.connect.grpc.binding.port={port}"
        )
        self._check_run_command(cmd)

    def stop_connect_server(self) -> int:
        """Stop the Spark connect server."""
        return self._run_command(self._stop_connect_server_cmd())

    def start_history_server(self) -> None:
        """Start the Spark history_server."""
        self._check_run_command(self._start_history_server_cmd())

    def stop_history_server(self) -> int:
        """Stop the Spark history_server."""
        return self._run_command(self._stop_history_server_cmd())

    def start_thrift_server(self) -> None:
        """Start the Apache Thrift server."""
        script = self._start_thrift_server_cmd()
        self._check_run_command(f"{script} --master {self._url}")

    def stop_thrift_server(self) -> int:
        """Stop the Apache Thrift server."""
        return self._run_command(self._stop_thrift_server_cmd())

    def start_jupyter_server(self) -> None:
        """Start a Jupyter server on the local node."""
        jupyter = shutil.which("jupyter")
        command = self._config.runtime.jupyter_command
        if jupyter is None:
            package = "jupyterlab" if command == "lab" else "notebook"
            msg = (
                "jupyter is not installed in the current environment. Install it with "
                f"`pip install {package}` (or `uv pip install {package}`)."
            )
            raise ExecutionError(msg)

        port = self._config.runtime.jupyter_port
        log_file = self._get_jupyter_log_file()
        pid_file = self._get_jupyter_pid_file()
        env = self._get_env()
        if self._config.runtime.start_connect_server:
            # Pre-wire notebooks to the Connect server so SparkSession.builder.getOrCreate()
            # connects to the running cluster without any extra configuration.
            env["SPARK_REMOTE"] = f"sc://localhost:{self._config.runtime.connect_server_port}"
        cmd = [
            jupyter,
            command,
            "--no-browser",
            f"--port={port}",
            f"--ip={self._config.runtime.jupyter_ip}",
            f"--notebook-dir={self._config.directories.base}",
            # Stop jupyter_lsp from probing for language servers that are not installed, which
            # otherwise floods the log with tracebacks. Passed as config (dotted form) so it is
            # ignored harmlessly when jupyter_lsp is not present.
            "--LanguageServerManager.autodetect=False",
        ]
        logger.info("Start Jupyter server: {}", " ".join(cmd))
        with open(log_file, "w", encoding="utf-8") as f_out:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=f_out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
        url = self._wait_for_jupyter_url(proc, log_file)
        pid_file.write_text(f"{proc.pid}\n", encoding="utf-8")
        self._log_jupyter_access(url, port, log_file)

    def _wait_for_jupyter_url(self, proc: "subprocess.Popen[bytes]", log_file: Path) -> str | None:
        """Wait for the server to report its access URL, returning it (or None on timeout).

        Also fails fast if the process exits before it is ready.
        """
        url_regex = re.compile(r"https?://\S+/(?:tree|lab)\?token=\w+")
        for _ in range(40):  # up to ~20 seconds
            ret = proc.poll()
            if ret is not None:
                msg = f"The Jupyter server exited with return code {ret}. See {log_file}."
                raise ExecutionError(msg)
            if log_file.exists():
                match = url_regex.search(log_file.read_text(encoding="utf-8"))
                if match:
                    return match.group(0)
            time.sleep(0.5)
        return None

    def _log_jupyter_access(self, url: str | None, port: int, log_file: Path) -> None:
        if url is None:
            logger.warning(
                "Started the Jupyter server, but did not detect its access URL within the timeout. "
                "Find the URL (with token) in {}.",
                log_file,
            )
            return

        host = gethostname()
        token_match = re.search(r"token=(\w+)", url)
        token = f"?token={token_match.group(1)}" if token_match else ""
        path = "lab" if self._config.runtime.jupyter_command == "lab" else "tree"
        logger.info(
            "Jupyter is running on {} (port {}). From your laptop, open an SSH tunnel:\n"
            "    ssh -L {}:{}:{} <your-hpc-login-host>\n"
            "then browse to:\n"
            "    http://localhost:{}/{}{}",
            host,
            port,
            port,
            host,
            port,
            port,
            path,
            token,
        )

    def stop_jupyter_server(self) -> int:
        """Stop the Jupyter server."""
        pid_file = self._get_jupyter_pid_file()
        if not pid_file.exists():
            logger.error("Cannot stop Jupyter server: {} does not exist", pid_file)
            return 1
        pid = int(pid_file.read_text(encoding="utf-8").strip())
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            logger.info("The Jupyter server has already exited")
            pid_file.unlink()
            return 0
        if self._wait_for_process_exit(pid, timeout_s=30):
            logger.info("Stopped the Jupyter server")
            pid_file.unlink()
            return 0
        logger.error("The Jupyter server process {} did not exit within the timeout", pid)
        return 1

    def _get_jupyter_pid_file(self) -> Path:
        return self._config.directories.base / "jupyter.pid"

    def _get_jupyter_log_file(self) -> Path:
        return self._config.directories.base / "jupyter.log"

    def start_worker_process(self, memory_gb: int) -> None:
        """Start one Spark worker process."""
        tmp_script = self._make_start_worker_script(self._start_worker_cmd(), memory_gb)
        try:
            self._check_run_command(str(tmp_script))
        finally:
            tmp_script.unlink()

    def start_worker_processes(
        self, workers: list[str], memory_gb: int, num_cpus_per_worker: int | None = None
    ) -> None:
        """Start the Spark worker processes."""
        if self._use_srun():
            self._start_worker_processes_srun(workers, memory_gb, num_cpus_per_worker)
            return

        # Calling Spark's start-workers.sh doesn't work because there is no way to forward
        # SPARK_CONF_DIR and JAVA_HOME through ssh in their scripts.
        start_script = self._sbin_cmd("start-worker.sh")
        tmp_script = self._make_start_worker_script(start_script, memory_gb)
        try:
            # Start the workers concurrently. ssh to each node is independent and otherwise
            # serializes cluster startup across all worker nodes.
            failures = self._run_ssh_commands(workers, str(tmp_script))
        finally:
            tmp_script.unlink()
        if failures:
            nodes = ", ".join(f"{worker} (rc={rc})" for worker, rc in failures.items())
            msg = f"Failed to start Spark workers on the following node(s): {nodes}"
            raise ExecutionError(msg)

    def stop_worker_process(self) -> int:
        """Stop the Spark workers."""
        tmp_script = self._make_stop_worker_script(self._config.resource_monitor.enabled)
        return self._run_command(str(tmp_script))

    def stop_worker_processes(self, workers: list[str]) -> int:
        """Stop the Spark workers."""
        if self._use_srun():
            return self._stop_worker_processes_srun(workers)

        tmp_script = self._make_stop_worker_script(self._config.resource_monitor.enabled)
        try:
            # Stop the workers concurrently; ssh to each node is independent.
            failures = self._run_ssh_commands(workers, str(tmp_script))
        finally:
            tmp_script.unlink()
        for worker, rc in failures.items():
            logger.error("Failed to stop worker on {}: {}", worker, rc)
        return next(iter(failures.values()), 0)

    @staticmethod
    def _run_ssh_commands(workers: list[str], script: str) -> dict[str, int]:
        """Run the script on each worker over ssh concurrently.

        Returns a mapping of worker node name to non-zero return code for any node that failed.
        """

        def run_one(worker: str) -> int:
            return subprocess.run(["ssh", worker, script]).returncode

        failures: dict[str, int] = {}
        # Cap concurrency so a large allocation does not spawn thousands of ssh threads at once;
        # the bounded pool keeps most of the speedup without exhausting local resources.
        max_workers = min(32, max(1, len(workers)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(run_one, worker): worker for worker in workers}
            for future in as_completed(futures):
                worker = futures[future]
                rc = future.result()
                if rc != 0:
                    failures[worker] = rc
        return failures

    def _use_srun(self) -> bool:
        if self._config.compute.environment != ComputeEnvironment.SLURM:
            return False
        if not self._config.compute.use_srun:
            logger.info("Launch Spark workers with ssh because compute.use_srun is disabled.")
            return False
        return True

    def _start_worker_processes_srun(
        self, workers: list[str], memory_gb: int, num_cpus_per_worker: int | None
    ) -> None:
        # Unlike ssh, srun forwards the full submission environment to the worker nodes
        # (environment modules, virtual environments, LD_LIBRARY_PATH).
        tmp_script = self._make_start_worker_script(
            self._start_worker_cmd(), memory_gb, daemonize=False
        )
        log_dir = self._config.directories.spark_scratch.absolute() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        num_workers = len(workers)
        cmd = [
            "srun",
            "--job-name=sparkctl-worker",
            "--export=ALL",
            # Do not connect the worker task's stdin to this process's stdin. Otherwise this
            # long-lived background srun competes with the interactive shell for the terminal
            # and steals keystrokes.
            "--input=none",
            *self._srun_node_args(workers),
            f"--output={log_dir}/spark-worker-%N.out",
        ]
        if num_cpus_per_worker is not None:
            cmd.append(f"--cpus-per-task={num_cpus_per_worker}")
        cmd.append(str(tmp_script))
        logger.info("Start Spark workers: {}", " ".join(cmd))
        with open(self._get_srun_log_file(), "w", encoding="utf-8") as f_out:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=f_out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        # The job step must stay alive for the lifetime of the workers, so srun runs in
        # the background and tmp_script is not deleted here. configure() recreates the
        # conf directory on the next run.
        time.sleep(1)
        ret = proc.poll()
        if ret is not None:
            msg = (
                f"The srun command that starts Spark workers exited immediately with "
                f"return code {ret}. See {self._get_srun_log_file()}."
            )
            raise ExecutionError(msg)
        self._get_srun_pid_file().write_text(f"{proc.pid}\n", encoding="utf-8")
        logger.info(
            "Started Spark workers on {} node(s) through srun with pid {}",
            num_workers,
            proc.pid,
        )

    def _srun_node_args(self, workers: list[str]) -> list[str]:
        num_workers = len(workers)
        args = [
            f"--nodes={num_workers}",
            f"--ntasks={num_workers}",
            "--ntasks-per-node=1",
        ]
        if "SLURM_HET_SIZE" in os.environ:
            # The master node is heterogeneous group 0; the workers are group 1.
            args.append("--het-group=1")
        else:
            args.append(f"--nodelist={','.join(workers)}")
        return args

    def _stop_worker_processes_srun(self, workers: list[str]) -> int:
        # rmon runs inside the worker srun job step, so it must be stopped gracefully before
        # the step is torn down, otherwise it is killed before it can write its stats and plots.
        ret = 0
        if self._config.resource_monitor.enabled:
            ret = self._stop_rmon_srun(workers)

        pid_file = self._get_srun_pid_file()
        if not pid_file.exists():
            logger.error("Cannot stop Spark workers: {} does not exist", pid_file)
            return ret or 1
        pid = int(pid_file.read_text(encoding="utf-8").strip())
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            logger.info("The srun process running Spark workers has already exited")
            pid_file.unlink()
            return ret
        if self._wait_for_process_exit(pid, timeout_s=30):
            logger.info("Stopped the srun process running Spark workers")
            pid_file.unlink()
            return ret
        logger.error(
            "The srun process {} running Spark workers did not exit within the timeout", pid
        )
        return ret or 1

    def _stop_rmon_srun(self, workers: list[str]) -> int:
        tmp_script = self._make_stop_rmon_script()
        # --overlap is required because the worker srun job step is still holding the
        # allocation's resources; without it this step would block until the workers exit,
        # which is the opposite of what we need.
        cmd = [
            "srun",
            "--job-name=sparkctl-stop-rmon",
            "--export=ALL",
            "--overlap",
            # Do not read from the terminal; see _start_worker_processes_srun.
            "--input=none",
            *self._srun_node_args(workers),
        ]
        cmd.append(str(tmp_script))
        logger.info("Stop rmon on workers: {}", " ".join(cmd))
        try:
            proc = subprocess.run(cmd, stdin=subprocess.DEVNULL, env=self._get_env())
        finally:
            tmp_script.unlink()
        if proc.returncode != 0:
            logger.error("Failed to stop rmon on workers: {}", proc.returncode)
        return proc.returncode

    @staticmethod
    def _wait_for_process_exit(pid: int, timeout_s: int) -> bool:
        for _ in range(timeout_s):
            try:
                # Reap the process if it is a child of this process, as happens with
                # ClusterManager.managed_cluster. Otherwise, it would remain a zombie
                # and appear to be running in the check below.
                wpid, _ = os.waitpid(pid, os.WNOHANG)
                if wpid == pid:
                    return True
            except ChildProcessError:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return True
            time.sleep(1)
        return False

    def _get_srun_pid_file(self) -> Path:
        return self._config.directories.base / "srun_workers.pid"

    def _get_srun_log_file(self) -> Path:
        return self._config.directories.base / "srun_workers.log"

    def _start_workers(self, script: str, memory_gb: int | None) -> None:
        cmd = f"{script} {self._url}"
        if memory_gb is not None:
            cmd += f" -m {memory_gb}G"
        self._check_run_command(cmd)

    def _start_master_cmd(self) -> str:
        return self._sbin_cmd("start-master.sh")

    def _stop_master_cmd(self) -> str:
        return self._sbin_cmd("stop-master.sh")

    def _start_connect_server_cmd(self) -> str:
        return self._sbin_cmd("start-connect-server.sh")

    def _stop_connect_server_cmd(self) -> str:
        return self._sbin_cmd("stop-connect-server.sh")

    def _start_history_server_cmd(self) -> str:
        return self._sbin_cmd("start-history-server.sh")

    def _stop_history_server_cmd(self) -> str:
        return self._sbin_cmd("stop-history-server.sh")

    def _start_thrift_server_cmd(self) -> str:
        return self._sbin_cmd("start-thriftserver.sh")

    def _stop_thrift_server_cmd(self) -> str:
        return self._sbin_cmd("stop-thriftserver.sh")

    def _start_worker_cmd(self) -> str:
        return self._sbin_cmd("start-worker.sh")

    def _stop_worker_cmd(self) -> str:
        return self._sbin_cmd("stop-worker.sh")

    def _start_workers_cmd(self) -> str:
        return self._sbin_cmd("start-workers.sh")

    def _stop_workers_cmd(self) -> str:
        return self._sbin_cmd("stop-workers.sh")

    def _sbin_cmd(self, name: str) -> str:
        return str(self._spark_path / "sbin" / name)

    def _check_run_command(self, cmd: str) -> None:
        subprocess.run(shlex.split(cmd), env=self._get_env(), check=True)

    def _run_command(self, cmd: str) -> int:
        return subprocess.run(shlex.split(cmd), env=self._get_env()).returncode

    def _get_env(self) -> dict[str, Any]:
        env = {k: v for k, v in os.environ.items()}
        env["SPARK_CONF_DIR"] = str(self._conf_dir)
        env["JAVA_HOME"] = str(self._java_path)
        return env

    def _make_start_worker_script(
        self,
        start_script: str,
        memory_gb: int,
        daemonize: bool = True,
    ) -> Path:
        conf_dir = self._config.directories.get_spark_conf_dir()
        content = f"""#!/bin/bash
export SPARK_CONF_DIR={conf_dir}
export JAVA_HOME={self._java_path}
"""
        worker_cmd = f"{start_script} {self._url} -m {memory_gb}g"
        if daemonize:
            content += f"{worker_cmd}\n"
            if self._config.resource_monitor.enabled:
                content += self._get_rmon_commands()
        else:
            # Slurm kills daemonized processes when the srun job step completes, so keep
            # the worker in the foreground for the lifetime of the step.
            content += "export SPARK_NO_DAEMONIZE=true\n"
            if self._config.resource_monitor.enabled:
                content += self._get_rmon_commands()
            content += f"exec {worker_cmd}\n"
        tmp_script = self._conf_dir / "tmp_start_worker.sh"
        tmp_script.write_text(content, encoding="utf-8")
        os.chmod(tmp_script, os.stat(tmp_script).st_mode | stat.S_IXUSR)
        return tmp_script

    def _get_rmon_commands(self) -> str:
        rmon = self._config.resource_monitor
        options = []
        for field in ("cpu", "disk", "memory", "network"):
            if getattr(rmon, field):
                options.append(f"--{field}")
            else:
                options.append(f"--no-{field}")
        rmon_exec = shutil.which("rmon")
        opts = " ".join(options)
        output_dir = self._config.directories.base / "stats-output"
        return f"""
{rmon_exec} collect {opts} --interval {rmon.interval} --output {output_dir} --overwrite --plots --daemon &
echo $! > {self._config.directories.base}/rmon_$(hostname).pid
"""

    def _make_stop_worker_script(self, kill_rmon: bool) -> Path:
        content = f"""#!/bin/bash
export SPARK_CONF_DIR={self._conf_dir}
export JAVA_HOME={self._java_path}
{self._stop_worker_cmd()}
"""
        if kill_rmon:
            content += self._get_rmon_stop_snippet()
        tmp_script = self._conf_dir / "tmp_stop_worker.sh"
        tmp_script.write_text(content, encoding="utf-8")
        os.chmod(tmp_script, os.stat(tmp_script).st_mode | stat.S_IXUSR)
        return tmp_script

    def _make_stop_rmon_script(self) -> Path:
        content = f"""#!/bin/bash
{self._get_rmon_stop_snippet()}"""
        tmp_script = self._conf_dir / "tmp_stop_rmon.sh"
        tmp_script.write_text(content, encoding="utf-8")
        os.chmod(tmp_script, os.stat(tmp_script).st_mode | stat.S_IXUSR)
        return tmp_script

    def _get_rmon_stop_snippet(self) -> str:
        # Send SIGTERM to the rmon process recorded on this node, then wait for it to exit so
        # that it can flush its stats and write its plots. This matters most on the srun path,
        # where the worker job step (and thus rmon) is torn down immediately afterward.
        return f"""
rmon_pid_file={self._config.directories.base}/rmon_$(hostname).pid
if [ -f "$rmon_pid_file" ]; then
    pid=$(cat "$rmon_pid_file")
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid"
        for _ in $(seq 1 30); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
    fi
    rm -f "$rmon_pid_file"
fi
"""
