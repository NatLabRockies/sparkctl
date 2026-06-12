import json
import subprocess
import sys
from pathlib import Path

import pytest
import toml
from click.testing import CliRunner

import sparkctl
from sparkctl.cli.sparkctl import cli


@pytest.mark.integration
def test_default_config(tmp_path, spark_binaries):
    spark_path = spark_binaries[0]["dir_path"]
    hadoop_path = spark_binaries[1]["dir_path"]
    hive_tarball = spark_binaries[2]["dir_path"]
    java_path = spark_binaries[3]["dir_path"]
    postgres_jar = spark_binaries[4]["dir_path"]
    cmd = [
        "default-config",
        "-e",
        "slurm",
        "-d",
        str(tmp_path),
        "-H",
        str(hadoop_path),
        "-h",
        str(hive_tarball),
        "-p",
        str(postgres_jar),
        str(spark_path),
        str(java_path),
    ]
    filename = tmp_path / ".sparkctl.toml"
    assert not filename.exists()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert filename.exists()

    data = toml.load(filename)
    assert data["binaries"]["spark_path"] == str(spark_path.absolute())
    assert data["binaries"]["java_path"] == str(java_path.absolute())
    assert data["binaries"]["hadoop_path"] == str(hadoop_path.absolute())
    assert data["binaries"]["hive_tarball"] == str(hive_tarball.absolute())
    assert data["binaries"]["postgresql_jar_file"] == str(postgres_jar.absolute())


@pytest.mark.integration
def test_configure_start_stop(setup_local_env):
    config, tmp_path = setup_local_env
    cmd = [
        "configure",
        "--directory",
        str(tmp_path),
        "--node-memory-overhead-gb",
        "5",
        "--dynamic-allocation",
        "--connect-server",
        "--no-thrift-server",
    ]
    filename = tmp_path / "config.json"
    assert not filename.exists()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert filename.exists()
    config = json.loads(filename.read_text(encoding="utf-8"))
    assert config["runtime"]["node_memory_overhead_gb"] == 5
    assert config["runtime"]["enable_dynamic_allocation"]
    assert config["runtime"]["start_connect_server"]
    assert not config["runtime"]["start_thrift_server"]
    assert config["directories"]["base"] == str(tmp_path)

    result = runner.invoke(cli, ["start", "--directory", str(tmp_path)])
    assert result.exit_code == 0
    try:
        subprocess.run([sys.executable, "tests/run_query.py"], check=True)
    finally:
        result = runner.invoke(cli, ["stop", "--directory", str(tmp_path)])
        assert result.exit_code == 0

    result = runner.invoke(
        cli, ["start", "--directory", str(tmp_path), "--wait", "--timeout", "0.01667"]
    )
    assert result.exit_code == 0


def test_invalid_executor_memory(setup_local_env):
    _, tmp_path = setup_local_env
    cmd = [
        "configure",
        "--directory",
        str(tmp_path),
        "--executor-memory-gb",
        "1000",
    ]
    filename = tmp_path / "config.json"
    assert not filename.exists()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 1
    assert "cannot be more than worker_memory_gb" in result.stderr


def test_spark_defaults_template(setup_local_env):
    _, tmp_path = setup_local_env
    template_file = Path(next(iter(sparkctl.__path__))) / "conf" / "spark-defaults.conf.template"
    new_template_file = tmp_path / "spark-defaults.conf"
    lines: list[str] = []
    found_timestamp_setting = False
    for line in template_file.read_text(encoding="utf-8").splitlines():
        if "TIMESTAMP_MICROS" in line:
            found_timestamp_setting = True
            continue
        lines.append(line)
    assert found_timestamp_setting
    new_template_file.write_text("\n".join(lines), encoding="utf-8")
    cmd = [
        "configure",
        "--directory",
        str(tmp_path),
        "--spark-defaults-template-file",
        str(new_template_file),
    ]
    filename = tmp_path / "conf" / "spark-defaults.conf"
    assert not filename.exists()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert filename.exists()

    found_timestamp_setting = False
    for line in filename.read_text(encoding="utf-8").splitlines():
        if "TIMESTAMP_MICROS" in line:
            found_timestamp_setting = True
    assert not found_timestamp_setting


def test_custom_spark_level(setup_local_env):
    _, tmp_path = setup_local_env
    log_level = "warn"
    cmd = [
        "configure",
        "--directory",
        str(tmp_path),
        "--spark-log-level",
        log_level,
    ]
    filename = tmp_path / "conf" / "log4j2.properties"
    assert not filename.exists()
    runner = CliRunner()
    result = runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert filename.exists()

    found_log_level = False
    for line in filename.read_text(encoding="utf-8").splitlines():
        if f"rootLogger.level = {log_level}" in line:
            found_log_level = True
    assert found_log_level
