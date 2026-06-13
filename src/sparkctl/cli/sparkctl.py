import sys
import time
from pathlib import Path
from typing import Any, Callable

import rich_click as click
import toml
from loguru import logger

from sparkctl.config import (
    DEFAULT_SETTINGS_FILENAME,
    RUNTIME,
    SETTINGS_FILE_ENV_VAR,
    get_binaries,
    sparkctl_settings,
)
from sparkctl.cluster_manager import ClusterManager
from sparkctl.exceptions import SparkctlBaseException
from sparkctl.loggers import setup_logging
from sparkctl.models import (
    BinaryLocations,
    ComputeEnvironment,
    ComputeParams,
    SparkConfig,
    SparkRuntimeParams,
    RuntimeDirectories,
)


@click.group("sparkctl")
@click.option(
    "-c",
    "--console-level",
    default=sparkctl_settings.app.console_level,
    show_default=True,
    help="Console log level",
)
@click.option(
    "-f",
    "--file-level",
    default=sparkctl_settings.app.file_level,
    show_default=True,
    help="File log level",
)
@click.option(
    "-r",
    "--reraise-exceptions",
    is_flag=True,
    default=sparkctl_settings.app.reraise_exceptions,
    show_default=True,
    help="Reraise unhandled sparkctl exceptions.",
)
@click.pass_context
def cli(ctx: click.Context, console_level: str, file_level: str, reraise_exceptions: bool) -> None:
    """sparkctl comands"""


_default_config_epilog = """
\b
Examples:\n
$ sparkctl default-config \\ \n
    /datasets/images/apache-spark/spark-4.1.2-bin-hadoop3 \\ \n
    /datasets/images/apache-spark/jdk-21.0.7 \\ \n
    -e slurm \\ \n
$ sparkctl default-config ~/apache-spark/spark-4.1.2-bin-hadoop3 ~/jdk-21.0.8 -e native\n
"""


@click.command(epilog=_default_config_epilog)
@click.argument("spark_path", type=click.Path(exists=True, path_type=Path))
@click.argument("java_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "-d",
    "--directory",
    default=Path.home(),
    show_default=True,
    help="Directory in which to create the sparkctl config file.",
    type=click.Path(path_type=Path),
)
@click.option(
    "-e",
    "--compute-environment",
    # FAKE is intended for tests only, so it is not offered as a user-facing choice.
    type=click.Choice([x.value for x in ComputeEnvironment if x != ComputeEnvironment.FAKE]),
    default=ComputeEnvironment.SLURM.value,
    help="Compute environment",
    callback=lambda *x: ComputeEnvironment(x[2]),
)
@click.option(
    "-H",
    "--hadoop-path",
    help="Directory containing Hadoop binaries.",
    type=click.Path(path_type=Path),
)
@click.option(
    "-h",
    "--hive-tarball",
    help="File containing Hive binaries.",
    type=click.Path(path_type=Path),
)
@click.option(
    "-p",
    "--postgresql-jar-file",
    help="Path to PostgreSQL jar file.",
    type=click.Path(path_type=Path),
)
@click.option(
    "-R",
    "--rapids-jar-file",
    help=BinaryLocations.model_fields["rapids_jar_file"].description,
    type=click.Path(path_type=Path),
)
def default_config(
    spark_path: Path,
    java_path: Path,
    directory: Path,
    compute_environment: ComputeEnvironment,
    hadoop_path: Path | None,
    hive_tarball: Path | None,
    postgresql_jar_file: Path | None,
    rapids_jar_file: Path | None,
):
    """Create a sparkctl config file that defines paths to Spark binaries.
    This is a one-time requirement when installing sparkctl in a new environment."""
    config = _create_default_config(spark_path, java_path, directory, compute_environment)
    if hadoop_path is not None:
        config.binaries.hadoop_path = hadoop_path
    if hive_tarball is not None:
        config.binaries.hive_tarball = hive_tarball
    if postgresql_jar_file is not None:
        config.binaries.postgresql_jar_file = postgresql_jar_file
    if rapids_jar_file is not None:
        config.binaries.rapids_jar_file = rapids_jar_file
    data = config.model_dump(mode="json", exclude={"directories"})
    # Don't hard-code the password globally.
    data["runtime"].pop("postgres_password")
    filename = directory / DEFAULT_SETTINGS_FILENAME
    with open(filename, "w", encoding="utf-8") as f_out:
        toml.dump(data, f_out)
    print(f"Wrote sparkctl settings to {filename}")

    # sparkctl only auto-discovers settings files in the home directory and the current working
    # directory. A file written anywhere else must be pointed to explicitly, otherwise later
    # commands silently fall back to defaults.
    resolved = filename.resolve().parent
    if resolved not in (Path.home().resolve(), Path.cwd().resolve()):
        print(
            f"\nNote: this location is not auto-discovered. Set {SETTINGS_FILE_ENV_VAR}={filename} "
            "in your environment so sparkctl can find it."
        )


def _create_default_config(
    spark_path: Path, java_path: Path, directory: Path, compute_environment: ComputeEnvironment
) -> SparkConfig:
    """Create the default Spark configuration."""
    return SparkConfig(
        compute=ComputeParams(environment=compute_environment),
        binaries=BinaryLocations(spark_path=spark_path, java_path=java_path),
        directories=RuntimeDirectories(base=directory),
        runtime=SparkRuntimeParams(**RUNTIME),
    )


_configure_epilog = """
Examples:\n
$ sparkctl configure --start\n
$ sparkctl configure --shuffle-partition-multiplier 4 --local-storage\n
$ sparkctl configure --local-storage --thrift-server\n
"""


@click.command(epilog=_configure_epilog)
@click.option(
    "-d",
    "--directory",
    default=Path(),
    show_default=True,
    help="Base directory for the cluster configuration",
    type=click.Path(path_type=Path),
)
@click.option(
    "-s",
    "--spark-scratch",
    default=Path("spark_scratch"),
    show_default=True,
    help=RuntimeDirectories.model_fields["spark_scratch"].description,
    type=click.Path(path_type=Path),
)
@click.option(
    "-e",
    "--executor-cores",
    default=sparkctl_settings.runtime.get("executor_cores"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["executor_cores"].description,
)
@click.option(
    "-E",
    "--executor-memory-gb",
    default=sparkctl_settings.runtime.get("executor_memory_gb"),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["executor_memory_gb"].description,
)
@click.option(
    "-M",
    "--driver-memory-gb",
    default=sparkctl_settings.runtime.get("driver_memory_gb"),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["driver_memory_gb"].description,
)
@click.option(
    "-o",
    "--node-memory-overhead-gb",
    default=sparkctl_settings.runtime.get("node_memory_overhead_gb"),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["node_memory_overhead_gb"].description,
)
@click.option(
    "--dynamic-allocation/--no-dynamic-allocation",
    is_flag=True,
    default=sparkctl_settings.runtime.get("enable_dynamic_allocation"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_dynamic_allocation"].description,
)
@click.option(
    "-m",
    "--shuffle-partition-multiplier",
    default=sparkctl_settings.runtime.get("shuffle_partition_multiplier"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["shuffle_partition_multiplier"].description,
)
@click.option(
    "-t",
    "--spark-defaults-template-file",
    help=SparkRuntimeParams.model_fields["spark_defaults_template_file"].description,
    type=click.Path(path_type=Path),
)
@click.option(
    "--local-storage/--no-local-storage",
    is_flag=True,
    default=sparkctl_settings.runtime.get("use_local_storage"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["use_local_storage"].description,
)
@click.option(
    "--connect-server/--no-connect-server",
    is_flag=True,
    default=sparkctl_settings.runtime.get("start_connect_server"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["start_connect_server"].description,
)
@click.option(
    "--connect-server-port",
    # Fall back to the model default so the option still works for users whose settings file
    # predates this field (a missing key would otherwise bind the default to None).
    default=sparkctl_settings.runtime.get(
        "connect_server_port",
        SparkRuntimeParams.model_fields["connect_server_port"].default,
    ),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["connect_server_port"].description,
)
@click.option(
    "--history-server/--no-history-server",
    is_flag=True,
    default=sparkctl_settings.runtime.get("start_history_server"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["start_history_server"].description,
)
@click.option(
    "--thrift-server/--no-thrift-server",
    is_flag=True,
    default=sparkctl_settings.runtime.get("start_thrift_server"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["start_thrift_server"].description,
)
@click.option(
    "--jupyter/--no-jupyter",
    is_flag=True,
    # Fall back to the model default so options added after a user's settings file was written
    # still work (a missing key would otherwise bind the default to None).
    default=sparkctl_settings.runtime.get(
        "start_jupyter", SparkRuntimeParams.model_fields["start_jupyter"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["start_jupyter"].description,
)
@click.option(
    "--jupyter-command",
    default=sparkctl_settings.runtime.get(
        "jupyter_command", SparkRuntimeParams.model_fields["jupyter_command"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["jupyter_command"].description,
)
@click.option(
    "--jupyter-ip",
    default=sparkctl_settings.runtime.get(
        "jupyter_ip", SparkRuntimeParams.model_fields["jupyter_ip"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["jupyter_ip"].description,
)
@click.option(
    "--jupyter-port",
    default=sparkctl_settings.runtime.get(
        "jupyter_port", SparkRuntimeParams.model_fields["jupyter_port"].default
    ),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["jupyter_port"].description,
)
@click.option(
    "--reverse-proxy/--no-reverse-proxy",
    is_flag=True,
    default=sparkctl_settings.runtime.get(
        "enable_reverse_proxy", SparkRuntimeParams.model_fields["enable_reverse_proxy"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_reverse_proxy"].description,
)
@click.option(
    "--reverse-proxy-url",
    default=sparkctl_settings.runtime.get(
        "reverse_proxy_url", SparkRuntimeParams.model_fields["reverse_proxy_url"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["reverse_proxy_url"].description,
)
@click.option(
    "--prometheus/--no-prometheus",
    is_flag=True,
    default=sparkctl_settings.runtime.get(
        "enable_prometheus", SparkRuntimeParams.model_fields["enable_prometheus"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_prometheus"].description,
)
@click.option(
    "--metrics-csv/--no-metrics-csv",
    is_flag=True,
    default=sparkctl_settings.runtime.get(
        "enable_metrics_csv", SparkRuntimeParams.model_fields["enable_metrics_csv"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_metrics_csv"].description,
)
@click.option(
    "--metrics-csv-period",
    default=sparkctl_settings.runtime.get(
        "metrics_csv_period", SparkRuntimeParams.model_fields["metrics_csv_period"].default
    ),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["metrics_csv_period"].description,
)
@click.option(
    "--gpus/--no-gpus",
    is_flag=True,
    default=sparkctl_settings.runtime.get(
        "enable_gpus", SparkRuntimeParams.model_fields["enable_gpus"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_gpus"].description,
)
@click.option(
    "--gpus-per-node",
    default=sparkctl_settings.runtime.get(
        "gpus_per_node", SparkRuntimeParams.model_fields["gpus_per_node"].default
    ),
    show_default=True,
    type=int,
    help=SparkRuntimeParams.model_fields["gpus_per_node"].description,
)
@click.option(
    "--rapids/--no-rapids",
    is_flag=True,
    default=sparkctl_settings.runtime.get(
        "enable_rapids", SparkRuntimeParams.model_fields["enable_rapids"].default
    ),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_rapids"].description,
)
@click.option(
    "-l",
    "--spark-log-level",
    default=sparkctl_settings.runtime.get("spark_log_level"),
    type=click.Choice(["debug", "info", "warn", "error"]),
    show_default=True,
    help=SparkRuntimeParams.model_fields["spark_log_level"].description,
)
@click.option(
    "--hive-metastore/--no-hive-metastore",
    is_flag=True,
    default=sparkctl_settings.runtime.get("enable_hive_metastore"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_hive_metastore"].description,
)
@click.option(
    "--postgres-hive-metastore/--no-postgres-hive-metastore",
    is_flag=True,
    default=sparkctl_settings.runtime.get("enable_postgres_hive_metastore"),
    show_default=True,
    help=SparkRuntimeParams.model_fields["enable_postgres_hive_metastore"].description,
)
@click.option(
    "-w",
    "--metastore-dir",
    default=Path(),
    show_default=True,
    help=RuntimeDirectories.model_fields["metastore_dir"].description,
    type=click.Path(path_type=Path),
)
@click.option(
    "-P",
    "--python-path",
    help=SparkRuntimeParams.model_fields["python_path"].description,
)
@click.option(
    "--resource-monitor/--no-resource-monitor",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable resource monitoring.",
)
@click.option(
    "--start/--no-start",
    is_flag=True,
    show_default=True,
    default=False,
    help="Start the cluster after configuration.",
)
@click.option(
    "--use-current-python/--no-use-current-python",
    is_flag=True,
    default=True,
    show_default=True,
    help="Use the Python executable in the current environment for Spark workers. "
    "--python-path takes precedence.",
)
@click.pass_context
def configure(
    ctx: click.Context,
    start: bool,
    directory: Path,
    spark_scratch: Path,
    executor_cores: int,
    executor_memory_gb: int,
    driver_memory_gb: int,
    node_memory_overhead_gb: int,
    dynamic_allocation: bool,
    shuffle_partition_multiplier: int,
    spark_defaults_template_file: Path | None,
    local_storage: bool,
    connect_server: bool,
    connect_server_port: int,
    history_server: bool,
    thrift_server: bool,
    jupyter: bool,
    jupyter_command: str,
    jupyter_ip: str,
    jupyter_port: int,
    reverse_proxy: bool,
    reverse_proxy_url: str | None,
    prometheus: bool,
    metrics_csv: bool,
    metrics_csv_period: int,
    gpus: bool,
    gpus_per_node: int | None,
    rapids: bool,
    spark_log_level: str | None,
    hive_metastore: bool,
    postgres_hive_metastore: bool,
    metastore_dir: Path,
    python_path: str | None,
    resource_monitor: bool,
    use_current_python: bool,
):
    """Create a Spark cluster configuration."""
    setup_logging(
        filename="sparkctl.log",
        console_level=ctx.find_root().params["console_level"],
        file_level=ctx.find_root().params["file_level"],
        mode="a",
    )
    if python_path is None and use_current_python:
        logger.info("Use the current Python executable for Spark workers.")
        python_path = sys.executable

    def build_config() -> SparkConfig:
        # Build inside the handled scope so that a missing binaries configuration
        # (raised by get_binaries) is reported cleanly rather than as a traceback.
        config = SparkConfig(
            binaries=get_binaries(),
            runtime=SparkRuntimeParams(
                executor_cores=executor_cores,
                executor_memory_gb=executor_memory_gb,
                driver_memory_gb=driver_memory_gb,
                node_memory_overhead_gb=node_memory_overhead_gb,
                enable_dynamic_allocation=dynamic_allocation,
                shuffle_partition_multiplier=shuffle_partition_multiplier,
                spark_defaults_template_file=spark_defaults_template_file,
                use_local_storage=local_storage,
                start_connect_server=connect_server,
                connect_server_port=connect_server_port,
                start_history_server=history_server,
                start_thrift_server=thrift_server,
                start_jupyter=jupyter,
                jupyter_command=jupyter_command,
                jupyter_ip=jupyter_ip,
                jupyter_port=jupyter_port,
                enable_reverse_proxy=reverse_proxy,
                reverse_proxy_url=reverse_proxy_url,
                enable_prometheus=prometheus,
                enable_metrics_csv=metrics_csv,
                metrics_csv_period=metrics_csv_period,
                enable_gpus=gpus,
                gpus_per_node=gpus_per_node,
                enable_rapids=rapids,
                spark_log_level=spark_log_level,
                enable_hive_metastore=hive_metastore,
                enable_postgres_hive_metastore=postgres_hive_metastore,
                python_path=python_path,
            ),
            directories=RuntimeDirectories(
                base=directory,
                spark_scratch=spark_scratch,
                metastore_dir=metastore_dir,
            ),
            compute=sparkctl_settings.get("compute", {"environment": "slurm"}),
        )
        config.resource_monitor.enabled = resource_monitor
        return config

    res = handle_sparkctl_exception(ctx, _configure, build_config, start)
    if res[1] != 0:
        ctx.exit(res[1])


def _configure(build_config: Callable[[], SparkConfig], start: bool) -> ClusterManager:
    mgr = ClusterManager(build_config())
    mgr.configure()
    if start:
        mgr.start()
    return mgr


_start_epilog = """
Examples:\n
$ sparkctl start\n
$ sparkctl start --directory ./my-spark-config\n
$ sparkctl start --wait\n
"""


@click.command(epilog=_start_epilog)
@click.option(
    "--wait/--no-wait",
    is_flag=True,
    default=False,
    show_default=True,
    help="If True, wait until the user presses Ctrl-C or timeout is reached and then stop the "
    "cluster. If False, start the cluster and exit.",
)
@click.option(
    "-d",
    "--directory",
    default=Path(),
    show_default=True,
    help="Base directory for the cluster configuration",
    type=click.Path(path_type=Path),
)
@click.option(
    "-t",
    "--timeout",
    type=float,
    help="If --wait is set, timeout in minutes. Defaults to no timeout.",
)
@click.pass_context
def start(ctx: click.Context, wait: bool, directory: Path, timeout: float | None) -> None:
    """Start a Spark cluster with an existing configuration."""
    setup_logging(
        filename="sparkctl.log",
        console_level=ctx.find_root().params["console_level"],
        file_level=ctx.find_root().params["file_level"],
        mode="a",
    )
    mgr = ClusterManager.load(directory)
    mgr.start()
    if wait:
        if timeout is None:
            msg = "Press Ctrl-C to shut down all Spark processes."
            end = sys.maxsize
        else:
            msg = f"Wait until Ctrl-C is detected or {timeout} minutes"
            end = int(time.time() + timeout * 60)
        logger.info(msg)
        interval = min((end - time.time(), 3600))
        try:
            while time.time() < end:
                time.sleep(interval)
            logger.info("Timeout expired, shutting down the cluster.")
        except KeyboardInterrupt:
            logger.info("Detected Ctrl-c, shutting down the cluster.")
        finally:
            mgr.stop()


_stop_epilog = """
Examples:\n
$ sparkctl stop\n
$ sparkctl stop --directory ./my-spark-config\n
"""


@click.command(epilog=_stop_epilog)
@click.option(
    "-d",
    "--directory",
    default=Path(),
    show_default=True,
    help="Base directory for the cluster configuration",
    type=click.Path(path_type=Path),
)
@click.pass_context
def stop(ctx: click.Context, directory: Path) -> None:
    """Stop a Spark cluster."""
    setup_logging(
        filename="sparkctl.log",
        console_level=ctx.find_root().params["console_level"],
        file_level=ctx.find_root().params["file_level"],
        mode="a",
    )
    mgr = ClusterManager.load(directory)
    mgr.stop()


@click.command()
@click.argument("directory", type=click.Path(path_type=Path))
@click.option(
    "--force/--no-force",
    is_flag=True,
    default=False,
    show_default=True,
    help="Clean even if a cluster appears to be running. By default clean refuses in that case "
    "because it would delete the files needed to stop the cluster.",
)
@click.pass_context
def clean(ctx: click.Context, directory: Path, force: bool) -> None:
    """Delete all Spark runtime files in the directory.

    Stop the cluster before cleaning. By default this refuses to run while a cluster appears to be
    running, since it deletes the state needed to stop it; pass --force to override.

    This also deletes the configured spark_scratch directory recursively, even when it is located
    outside the base configuration directory. Point spark_scratch at a dedicated directory.
    """
    setup_logging(
        filename="sparkctl.log",
        console_level=ctx.find_root().params["console_level"],
        file_level=ctx.find_root().params["file_level"],
        mode="a",
    )
    res = handle_sparkctl_exception(ctx, lambda: ClusterManager.load(directory).clean(force=force))
    if res[1] != 0:
        ctx.exit(res[1])


def handle_sparkctl_exception(ctx: click.Context, func, *args, **kwargs) -> Any:
    """Handle any sparkctl exceptions as specified by the CLI parameters."""
    res = None
    try:
        res = func(*args, **kwargs)
        return res, 0
    except SparkctlBaseException:
        exc_type, exc_value, exc_tb = sys.exc_info()
        filename = exc_tb.tb_frame.f_code.co_filename  # type: ignore
        line = exc_tb.tb_lineno  # type: ignore
        msg = f'{func.__name__} failed: exception={exc_type.__name__} message="{exc_value}" {filename=} {line=}'  # type: ignore
        logger.error(msg)
        if ctx.find_root().params["reraise_exceptions"]:
            raise
        return res, 1


cli.add_command(default_config)
cli.add_command(configure)
cli.add_command(start)
cli.add_command(stop)
cli.add_command(clean)
