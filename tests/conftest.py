import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any

import pytest
import requests

from sparkctl.cli.sparkctl import _create_default_config
from sparkctl.config import sparkctl_settings
from sparkctl.models import ComputeEnvironment


SPARK_DIR_NAME = "spark-4.1.2-bin-hadoop3"
SPARK_GZ_NAME = f"{SPARK_DIR_NAME}.tgz"
SPARK_URL = f"https://archive.apache.org/dist/spark/spark-4.1.2/{SPARK_GZ_NAME}"
SPARK_DIR_GZ = Path("tests") / "data" / SPARK_GZ_NAME
SPARK_DIR = Path("tests") / "data" / SPARK_DIR_NAME

HADOOP_DIR_NAME = "hadoop-3.4.1"
HADOOP_GZ_NAME = f"{HADOOP_DIR_NAME}.tar.gz"
HADOOP_URL = f"https://archive.apache.org/dist/hadoop/common/{HADOOP_DIR_NAME}/{HADOOP_GZ_NAME}"
HADOOP_DIR_GZ = Path("tests") / "data" / HADOOP_GZ_NAME
HADOOP_DIR = Path("tests") / "data" / HADOOP_DIR_NAME

HIVE_DIR_NAME = "apache-hive-4.0.1-bin"
HIVE_GZ_NAME = f"{HIVE_DIR_NAME}.tar.gz"
HIVE_URL = f"https://archive.apache.org/dist/hive/hive-4.0.1/{HIVE_GZ_NAME}"
HIVE_DIR_GZ = Path("tests") / "data" / HIVE_GZ_NAME
HIVE_DIR = Path("tests") / "data" / HIVE_DIR_NAME

POSTGRES_JAR_NAME = "postgresql-42.7.4.jar"
POSTGRES_JAR_URL = f"https://jdbc.postgresql.org/download/{POSTGRES_JAR_NAME}"
POSTGRES_JAR_FILE = Path("tests") / "data" / POSTGRES_JAR_NAME


TARBALLS: list[dict[str, Any]] = [
    {
        "name": "spark",
        "dir_path": SPARK_DIR,
        "dir_gz": SPARK_DIR_GZ,
        "url": SPARK_URL,
        "extract": True,
    },
    {
        "name": "hadoop",
        "dir_path": HADOOP_DIR,
        "dir_gz": HADOOP_DIR_GZ,
        "url": HADOOP_URL,
        "extract": True,
    },
    {
        "name": "hive",
        "dir_path": HIVE_DIR,
        "dir_gz": HIVE_DIR_GZ,
        "url": HIVE_URL,
        "extract": False,
    },
]

if sys.platform == "linux":
    JAVA_DIR_NAME = "jdk-21.0.7"
    JAVA_GZ_NAME = "jdk-21.0.7_linux-x64_bin.tar.gz"
    JAVA_URL = f"https://download.oracle.com/java/21/archive/{JAVA_GZ_NAME}"
    JAVA_DIR_GZ = Path("tests") / "data" / JAVA_GZ_NAME
    JAVA_DIR = Path("tests") / "data" / JAVA_DIR_NAME
elif sys.platform == "darwin":
    JAVA_DIR_NAME = "jdk-21.0.7.jdk"
    JAVA_GZ_NAME = "jdk-21.0.7_macos-aarch64_bin.tar.gz"
    JAVA_URL = f"https://download.oracle.com/java/21/archive/{JAVA_GZ_NAME}"
    JAVA_DIR_GZ = Path("tests") / "data" / JAVA_GZ_NAME
    JAVA_DIR = Path("tests") / "data" / JAVA_DIR_NAME
else:
    msg = f"Only Linux and MacOS are supported: {sys.platform}"
    raise Exception(msg)

TARBALLS.append(
    {
        "name": "java",
        "dir_path": JAVA_DIR,
        "dir_gz": JAVA_DIR_GZ,
        "url": JAVA_URL,
        "extract": True,
    },
)


def pytest_sessionstart(session):
    # The large Spark/Java/Hadoop/Hive downloads are only needed by the integration tests
    # (those that download real binaries or start a real cluster). Skip them when integration
    # tests are deselected, such as the unit-only run in CI (pytest -m "not integration").
    if "not integration" in (session.config.option.markexpr or ""):
        return

    base_dir = Path("tests") / "data"
    base_dir.mkdir(exist_ok=True)
    in_ci = os.getenv("CI", "false") == "true"
    for config in TARBALLS:
        if in_ci and config["name"] in ("hadoop", "hive"):
            # We are not currently testing these in CI.
            continue
        dir_path: Path = config["dir_path"]
        dir_gz: Path = config["dir_gz"]
        url: str = config["url"]
        extract: bool = config["extract"]
        if not dir_path.exists():
            if not dir_gz.exists():
                try:
                    download_file(url, dir_gz)
                except Exception:
                    subprocess.run(["wget", url, "-O", str(dir_gz)], check=True)
            if extract:
                extract_tarball(dir_gz, base_dir)

    if not in_ci and not POSTGRES_JAR_FILE.exists():
        try:
            download_file(POSTGRES_JAR_URL, POSTGRES_JAR_FILE)
        except Exception:
            subprocess.run(["wget", POSTGRES_JAR_URL, "-O", str(POSTGRES_JAR_FILE)], check=True)


@pytest.fixture
def spark_binaries() -> list[dict[str, Any]]:
    return TARBALLS + [{"name": "postgres", "dir_path": POSTGRES_JAR_FILE}]


def download_file(url: str, download_path: Path) -> None:
    print(f"Download {url}")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(download_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=65536):
                file.write(chunk)


def extract_tarball(src_file: Path, extract_dir: Path) -> None:
    print(f"Extract {src_file}")
    with tarfile.open(src_file, "r:gz") as tar:
        tar.extractall(path=extract_dir)


@pytest.fixture
def setup_local_env(tmp_path):
    config = _create_default_config(SPARK_DIR, JAVA_DIR, tmp_path, ComputeEnvironment.FAKE)
    config.binaries.spark_path = SPARK_DIR
    match sys.platform:
        case "linux":
            config.binaries.java_path = JAVA_DIR
        case "darwin":
            config.binaries.java_path = JAVA_DIR / "Contents" / "Home"
        case _:
            assert False
    config.binaries.hadoop_path = HADOOP_DIR
    config.binaries.hive_tarball = HIVE_DIR
    config.binaries.postgresql_jar_file = POSTGRES_JAR_FILE
    data = config.model_dump(mode="json", exclude={"directories"})
    # Inject the configuration into the Dynaconf singleton so that the CLI and
    # make_default_spark_config() use it instead of any real ~/.sparkctl.toml. The singleton is
    # built at import time, so setting an environment variable here would not take effect.
    originals = {key: sparkctl_settings.get(key) for key in ("BINARIES", "RUNTIME", "COMPUTE")}
    sparkctl_settings.set("BINARIES", data["binaries"])
    sparkctl_settings.set("RUNTIME", data["runtime"])
    sparkctl_settings.set("COMPUTE", data["compute"])
    try:
        yield config, tmp_path
    finally:
        for key, value in originals.items():
            sparkctl_settings.set(key, value)
        shutil.rmtree(tmp_path, ignore_errors=True)
