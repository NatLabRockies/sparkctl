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

## License
sparkctl is released under a BSD 3-Clause [license](https://github.com/NREL/sparkctl/blob/main/LICENSE).

## Software Record
This package is developed under NLR Software Record SWR-25-109.
