import os
import shutil
import subprocess
import tarfile
from pathlib import Path

from sparkctl.models import SparkConfig


def setup_postgres_metastore(config: SparkConfig) -> None:
    """Setup a PostgreSQL-based Hive metastore."""
    pg_data_dir = config.directories.base / "pg_data"
    pg_exists = bool(list(pg_data_dir.iterdir()))
    setup_script = config.compute.postgres.get_script_path("setup_metastore")
    assert config.runtime.postgres_password is not None
    # Pass the password through the environment rather than the command line so that it does not
    # appear in process listings (ps) on the node.
    env = {**os.environ, "SPARKCTL_PG_PASSWORD": config.runtime.postgres_password}
    subprocess.run(
        ["bash", str(setup_script), str(pg_exists).lower()],
        env=env,
        check=True,
    )
    if not pg_exists:
        init_hive(config)


def init_hive(config: SparkConfig):
    """Initialize Apache Hive."""
    for field in ("hadoop_path", "hive_tarball", "postgresql_jar_file"):
        val = getattr(config.binaries, field)
        if val is None:
            msg = f"{field} cannot be None"
            raise ValueError(msg)
    if config.runtime.postgres_password is None:
        msg = "postgres_password cannot be None"
        raise ValueError(msg)

    assert config.binaries.hadoop_path is not None
    assert config.binaries.hive_tarball is not None
    assert config.binaries.postgresql_jar_file is not None

    hive_home = config.directories.base.absolute() / config.binaries.hive_tarball.name.replace(
        ".tar.gz", ""
    )
    if hive_home.exists():
        shutil.rmtree(hive_home)
    with tarfile.open(config.binaries.hive_tarball, "r:gz") as tar:
        tar.extractall(path=config.directories.base, filter="data")
    hive_conf = hive_home / "conf"

    shutil.copyfile(
        config.binaries.postgresql_jar_file,
        hive_home / "lib" / config.binaries.postgresql_jar_file.name,
    )
    write_postgres_hive_site_file(config.runtime.postgres_password, hive_conf / "hive-site.xml")
    cwd = os.getcwd()
    os.chdir(hive_conf)
    try:
        env = {k: v for k, v in os.environ.items()}
        env.update(
            {
                "HADOOP_HOME": str(config.binaries.hadoop_path),
                "HIVE_HOME": str(hive_home),
                "HIVE_CONF": str(hive_conf),
            }
        )
        subprocess.run(
            [f"{hive_home}/bin/schematool", "-dbType", "postgres", "-initSchema"],
            env=env,
            check=True,
        )
    finally:
        os.chdir(cwd)


def write_postgres_hive_site_file(postgres_password: str, filename: Path) -> None:
    hive_site_contents = f"""<configuration>
   <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:postgresql://localhost:5432/hive_metastore</value>
      <description>Postgres JDBC connection URL</description>
   </property>
   <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>org.postgresql.Driver</value>
      <description>Driver class name</description>
   </property>
   <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>postgres</value>
      <description>Postgres username</description>
   </property>
   <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>{postgres_password}</value>
      <description>Postgres password</description>
   </property>
   <property>
      <name>datanucleus.autoCreateSchema</name>
      <value>true</value>
   </property>
   <property>
      <name>datanucleus.fixedDatastore</name>
      <value>true</value>
   </property>
   <property>
      <name>datanucleus.autoCreateTables</name>
      <value>True</value>
   </property>
</configuration>
"""
    filename.write_text(hive_site_contents, encoding="utf-8")
