import os
from pathlib import Path

from dynaconf import Dynaconf, Validator  # type: ignore
from rich import print

from sparkctl.exceptions import InvalidConfiguration
from sparkctl.models import (
    AppParams,
    BinaryLocations,
    ComputeParams,
    SparkConfig,
    SparkRuntimeParams,
)

DEFAULT_SETTINGS_FILENAME = ".sparkctl.toml"
SETTINGS_FILE_ENV_VAR = "SPARKCTL_SETTINGS_FILE"


def _build_settings_files() -> list[Path]:
    """Return the settings files to load, in increasing order of precedence.

    Order: the home-directory file, then an explicit file named by the
    ``SPARKCTL_SETTINGS_FILE`` environment variable (used by shared/HPC
    deployments such as an environment module), then a project-local file in the
    current working directory. Later files override earlier ones, so a user can
    still drop a ``.sparkctl.toml`` in their working directory to override a
    shared deployment.

    All paths are absolute on purpose. A relative settings file would only be
    found when running from $HOME because Dynaconf resolves relative paths
    against its root_path (the first file's directory), not $HOME.
    """
    files = [Path.home() / DEFAULT_SETTINGS_FILENAME]
    explicit = os.environ.get(SETTINGS_FILE_ENV_VAR)
    if explicit:
        files.append(Path(explicit).expanduser().absolute())
    files.append(Path.cwd() / DEFAULT_SETTINGS_FILENAME)
    return files


RUNTIME = {
    "executor_cores": SparkRuntimeParams.model_fields["executor_cores"].default,
    "executor_memory_gb": SparkRuntimeParams.model_fields["executor_memory_gb"].default,
    "driver_memory_gb": SparkRuntimeParams.model_fields["driver_memory_gb"].default,
    "node_memory_overhead_gb": SparkRuntimeParams.model_fields["node_memory_overhead_gb"].default,
    "use_local_storage": SparkRuntimeParams.model_fields["use_local_storage"].default,
    "start_connect_server": SparkRuntimeParams.model_fields["start_connect_server"].default,
    "connect_server_port": SparkRuntimeParams.model_fields["connect_server_port"].default,
    "start_history_server": SparkRuntimeParams.model_fields["start_history_server"].default,
    "start_thrift_server": SparkRuntimeParams.model_fields["start_thrift_server"].default,
    "spark_log_level": SparkRuntimeParams.model_fields["spark_log_level"].default,
    "enable_dynamic_allocation": SparkRuntimeParams.model_fields[
        "enable_dynamic_allocation"
    ].default,
    "shuffle_partition_multiplier": SparkRuntimeParams.model_fields[
        "shuffle_partition_multiplier"
    ].default,
    "enable_hive_metastore": SparkRuntimeParams.model_fields["enable_hive_metastore"].default,
    "enable_postgres_hive_metastore": SparkRuntimeParams.model_fields[
        "enable_postgres_hive_metastore"
    ].default,
    "postgres_password": None,
    "spark_defaults_template_file": None,
}
APP = {
    "console_level": AppParams.model_fields["console_level"].default,
    "file_level": AppParams.model_fields["file_level"].default,
    "reraise_exceptions": AppParams.model_fields["reraise_exceptions"].default,
}

sparkctl_settings = Dynaconf(
    envvar_prefix="SPARKCTL",
    settings_files=_build_settings_files(),
    validators=[
        # There is intentionally no default for BINARIES. Binary paths are environment-specific
        # and must come from the user's settings file (created by `sparkctl default-config`). A
        # built-in default would silently produce a config for the wrong environment whenever the
        # settings file is missing or not found.
        Validator("RUNTIME", default=SparkRuntimeParams(**RUNTIME).model_dump(mode="json")),
        Validator("COMPUTE", default=ComputeParams().model_dump(mode="json")),
        Validator("APP", default=AppParams().model_dump(mode="json")),
    ],
)


def get_binaries() -> BinaryLocations:
    """Return the binary locations from the user's settings file.

    Raises
    ------
    InvalidConfiguration
        Raised if no settings file with binary paths has been loaded.
    """
    binaries = sparkctl_settings.get("binaries")
    if not binaries:
        settings_file = Path.home() / DEFAULT_SETTINGS_FILENAME
        msg = (
            "No sparkctl binary paths are configured. Run `sparkctl default-config` to create a "
            f"settings file at {settings_file} (or in the current directory) before running this "
            "command."
        )
        raise InvalidConfiguration(msg)
    return BinaryLocations(**binaries)


def make_default_spark_config() -> SparkConfig:
    """Return a SparkConfig created from the user's config file."""
    return SparkConfig(
        binaries=get_binaries(),
        runtime=SparkRuntimeParams(**sparkctl_settings.runtime),
        compute=ComputeParams(**sparkctl_settings.compute),
    )


def print_settings() -> None:
    """Print the current sparkctl settings."""
    print(sparkctl_settings.to_dict())
