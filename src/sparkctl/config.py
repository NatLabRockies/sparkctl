from sparkctl.models import AppParams, ComputeParams
from pathlib import Path

from dynaconf import Dynaconf, Validator  # type: ignore
from rich import print

from sparkctl.exceptions import InvalidConfiguration
from sparkctl.models import BinaryLocations, SparkRuntimeParams, SparkConfig

DEFAULT_SETTINGS_FILENAME = ".sparkctl.toml"
RUNTIME = {
    "executor_cores": SparkRuntimeParams.model_fields["executor_cores"].default,
    "executor_memory_gb": SparkRuntimeParams.model_fields["executor_memory_gb"].default,
    "driver_memory_gb": SparkRuntimeParams.model_fields["driver_memory_gb"].default,
    "node_memory_overhead_gb": SparkRuntimeParams.model_fields["node_memory_overhead_gb"].default,
    "use_local_storage": SparkRuntimeParams.model_fields["use_local_storage"].default,
    "start_connect_server": SparkRuntimeParams.model_fields["start_connect_server"].default,
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
    settings_files=[
        # default-config writes to the home directory by default, so load that file
        # explicitly with an absolute path. A relative path would only be found when running
        # from $HOME because Dynaconf resolves relative settings files against its root_path
        # (the first file's directory), not $HOME.
        Path.home() / DEFAULT_SETTINGS_FILENAME,
        # Allow a project-local file in the current working directory to override the home
        # settings. This must also be absolute for the same root_path reason as above.
        Path.cwd() / DEFAULT_SETTINGS_FILENAME,
    ],
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
