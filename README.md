# sparkctl
This package implements configuration and orchestration of Spark clusters with standalone cluster
managers. This is useful in environments like HPCs where the infrastructure implemented by cloud
providers, such as AWS, is not available. It is particularly helpful when users want to deploy Spark
but do not have administrative control of the servers.

## Example usage
There are two main ways to use this package:

First, allocate compute nodes. For example, with Slurm (1 compute node for the Spark master and
4 compute nodes for Spark workers):
   
```console
$ salloc -t 01:00:00 -n4 --partition=shared --mem=30G : -N4 --account=<your-account> --mem=240G
```
  
1. Configure a Spark cluster and run Spark jobs with `spark-submit` or `pyspark`.
```console
$ sparkctl configure
$ sparkctl start
$ spark-submit --master spark://$(hostname):7077 my-job.py
$ sparkctl stop
```

2. Run Spark jobs in a Python script using the `sparkctl` library to manage the cluster.
```python
from sparkctl import ClusterManager, make_default_spark_config

config = make_default_spark_config()
mgr = ClusterManager(config)
with mgr.managed_cluster() as spark:
    df = spark.createDataFrame([(x, x + 1) for x in range(1000)], ["a", "b"])
    df.show()
```

Refer to the [user documentation](https://nrel.github.io/sparkctl/) for a description of features
and detailed usage instructions.

## Project Status
The package is actively maintained and used at the National Laboratory of the Rockies  (NLR).
The software is primarily geared toward HPCs that use Slurm. It also supports a generic list of
servers as long as the servers have access to a shared filesystem and are accessible via SSH without
password login.

It would be straightforward to extend the functionality to support other HPC resource managers.
Please submit an issue or idea or discussion if you have interest in this package but need that
support.

Contributions are welcome.

## Development
This project uses [uv](https://docs.astral.sh/uv/) for environment management. Install the
package with its development dependencies:
```console
$ uv sync --extra dev
```

Lint, format, and type-check the code with [ruff](https://docs.astral.sh/ruff/) and
[ty](https://github.com/astral-sh/ty):
```console
$ uv run ruff check .
$ uv run ruff format --check .
$ uv run ty check
```

These checks also run as Git hooks via [prek](https://github.com/j178/prek). Install the hooks
once and then run them on demand:
```console
$ uv run prek install
$ uv run prek run --all-files
```

Run the unit tests. These are fast, require no special resources, and are what CI runs:
```console
$ uv run pytest -m "not integration"
```

The integration tests download a real Spark and Java distribution into `tests/data/` and start a
real single-node Spark cluster, so they are slower and require network access and sufficient
memory. They are excluded from CI; run them locally with:
```console
$ uv run pytest -m integration
```

Run the complete suite (unit and integration tests) with `uv run pytest`.

## License
sparkctl is released under a BSD 3-Clause [license](https://github.com/NatLabRockies/sparkctl/blob/main/LICENSE).

## Software Record
This package is developed under NLR Software Record SWR-25-109.
