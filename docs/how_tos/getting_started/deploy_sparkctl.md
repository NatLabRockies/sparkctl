(deploy-sparkctl)=

# Deploy sparkctl in an HPC environment
This is a one-time operation to be performed by an administrator or a user with write access to a
common location on the shared filesystem.

If you are a sparkctl user, you should not need to perform this step unless you want to use a
custom version of Spark.

------------------------

Sparkctl requires that Apache Spark and all dependent software are installed on the shared
filesystem, accessible by all compute nodes.

At minimum, this includes Spark and Java. If your users want to use a PostgreSQL-based Hive
metastore, you must also download Apache Hive, Hadoop, and an integration jar file for postgres.

Here is an example filesystem layout:

```
/datasets/images/apache_spark
├── apache-hive-4.0.1-bin.tar.gz
├── hadoop-3.4.1/
├── jdk-21.0.7/
├── postgresql-42.7.4.jar
├── spark-4.1.2-bin-hadoop3/
```

## URLs
Download locations will vary over time. Here is a set of URLs with software tested with Apache
Spark v4.1.2:

- https://dlcdn.apache.org/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz
- https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz
- https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
- https://downloads.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz
- https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

## sparkctl configuration file
This command will create a default sparkctl configuration file given this filesystem layout:
```console
$ sparkctl default-config \
    /datasets/images/apache_spark/spark-4.1.2-bin-hadoop3 \
    /datasets/images/apache_spark/jdk-21.0.7 \
    --hadoop-path /datasets/images/apache_spark/hadoop-3.4.1 \
    --hive-tarball /datasets/images/apache_spark/apache-hive-4.0.1-bin.tar.gz \
    --postgresql-jar-file /datasets/images/apache_spark/postgresql-42.7.4.jar \
    --compute-environment slurm
```

By default `sparkctl` reads this file from `~/.sparkctl.toml`. For a shared
deployment, place it in a common location and point users at it with the
`SPARKCTL_SETTINGS_FILE` environment variable:

```console
$ export SPARKCTL_SETTINGS_FILE=/datasets/images/apache_spark/sparkctl.toml
```

`sparkctl` loads settings files in increasing order of precedence:
`~/.sparkctl.toml`, then the file named by `SPARKCTL_SETTINGS_FILE`, then a
`.sparkctl.toml` in the current working directory. This lets a user override any
site-wide default locally without touching the shared deployment.

## Environment module (recommended)
On HPC systems that use [Lmod](https://lmod.readthedocs.io/) or Environment
Modules, you can wrap the steps above in an environment module so that users
only need:

```console
$ module load sparkctl
$ sparkctl configure --start
```

A ready-to-deploy modulefile (TCL and Lua flavors), an example shared settings
file, and step-by-step instructions are provided in the
[`hpc/environment_module`](https://github.com/NatLabRockies/sparkctl/tree/main/hpc/environment_module)
directory of the repository. The module activates the shared virtual
environment, sets `SPARKCTL_SETTINGS_FILE`, and puts `spark-submit`/`pyspark` on
the user's `PATH`.
