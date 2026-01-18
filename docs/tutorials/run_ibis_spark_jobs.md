# Use Ibis with sparkctl

In this tutorial you will learn how to use [Ibis](https://ibis-project.org/) with a sparkctl-managed
Spark cluster on HPC. Ibis provides a unified Python dataframe API that can target multiple backends,
including PySpark.

This tutorial covers three approaches:

- **Option A: Managed Cluster** - Use sparkctl as a Python library with Spark Connect (recommended)
- **Option B: CLI with Spark Connect** - Start the cluster via CLI, connect with Spark Connect
- **Option C: CLI with Traditional PySpark** - Start the cluster via CLI, connect with full PySpark

## Prerequisites

Install Ibis with the PySpark backend in your Python environment. You must pin `pyspark` to
version 4.0.0 to match sparkctl's requirements:

```console
$ pip install 'ibis-framework[pyspark]' 'pyspark==4.0.0'
```

This installs Ibis with PySpark support at the version compatible with sparkctl.

## Option A: Managed Cluster (Spark Connect)

This approach uses sparkctl as a Python library to manage the entire cluster lifecycle within your
script. The cluster starts when you enter the context manager and stops automatically when you exit.
This approach uses Spark Connect under the hood.

1. Allocate compute nodes, such as with Slurm. This example acquires 4 CPUs and 30 GB of memory
   for the Spark master process and user application + Spark driver and 2 complete nodes for Spark
   workers.

   ```console
   $ salloc -t 01:00:00 -n4 --partition=shared --mem=30G : -N2 --account=<your-account> --mem=240G
   ```

2. Activate the Python environment that contains sparkctl and ibis-pyspark.

   ```console
   $ module load python
   $ source ~/python-envs/sparkctl
   ```

3. Add the code below to a Python script. This will configure and start the Spark cluster, create
   an Ibis connection, run your queries, and then stop the cluster.

   ```python
   import ibis
   from sparkctl import ClusterManager, make_default_spark_config

   # Load your global sparkctl configuration file (~/.sparkctl.toml).
   config = make_default_spark_config()
   # Set runtime options as desired.
   # config.runtime.driver_memory_gb = 20
   # config.runtime.use_local_storage = True
   mgr = ClusterManager(config)

   with mgr.managed_cluster() as spark:
       # Create an Ibis connection and set it as the default backend
       con = ibis.pyspark.connect(session=spark)
       ibis.set_backend(con)

       # Create an in-memory table
       t = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})

       # Query using Ibis
       result = t.filter(t.a > 1).select(t.a, t.b, c=t.a + t.b)
       print(result.execute())
   ```

4. Run the script.

   ```console
   $ python my_ibis_job.py
   ```

## Option B: CLI with Spark Connect

This approach uses the sparkctl CLI to start the cluster with Spark Connect Server, then connects
from a separate Python session. This is useful when you want to start the cluster once and run
multiple scripts or interactive sessions against it.

1. Allocate compute nodes, such as with Slurm. This example acquires 4 CPUs and 30 GB of memory
   for the Spark master process and user application + Spark driver and 2 complete nodes for Spark
   workers.

   ```console
   $ salloc -t 01:00:00 -n4 --partition=shared --mem=30G : -N2 --account=<your-account> --mem=240G
   ```

2. Activate the Python environment that contains sparkctl.

   ```console
   $ module load python
   $ source ~/python-envs/sparkctl
   ```

3. Configure the Spark cluster with the Connect Server enabled. The sparkctl code will detect
   the compute nodes based on Slurm environment variables.

   ```console
   $ sparkctl configure --connect-server
   ```

4. Start the cluster.

   ```console
   $ sparkctl start
   ```

5. Set the environment variables `SPARK_CONF_DIR` and `JAVA_HOME`. This ensures that your
   application uses the Spark settings created in step 3. Instructions will be printed to the
   console. For example:

   ```console
   $ export SPARK_CONF_DIR=$(pwd)/conf
   $ export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
   ```

6. Connect with Ibis. You can do this interactively or in a script.

   ```console
   $ python
   ```
   ```python
   >>> import ibis
   >>> from pyspark.sql import SparkSession
   >>> spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
   >>> con = ibis.pyspark.connect(session=spark)
   >>> ibis.set_backend(con)
   >>> # Create and query tables
   >>> t = ibis.memtable({"x": [10, 20, 30], "y": [1.1, 2.2, 3.3]})
   >>> t.filter(t.x > 15).execute()
   ```

7. When finished, stop the cluster.

   ```console
   $ sparkctl stop
   ```

## Option C: CLI with Traditional PySpark

This approach uses the sparkctl CLI to start the cluster, then connects using traditional PySpark
(not Spark Connect). Use this if you need full PySpark functionality or encounter compatibility
issues with Spark Connect.

1. Allocate compute nodes, such as with Slurm. This example acquires 4 CPUs and 30 GB of memory
   for the Spark master process and user application + Spark driver and 2 complete nodes for Spark
   workers.

   ```console
   $ salloc -t 01:00:00 -n4 --partition=shared --mem=30G : -N2 --account=<your-account> --mem=240G
   ```

2. Activate the Python environment that contains sparkctl.

   ```console
   $ module load python
   $ source ~/python-envs/sparkctl
   ```

3. Configure the Spark cluster without the Connect Server.

   ```console
   $ sparkctl configure --no-connect-server
   ```

4. Start the cluster.

   ```console
   $ sparkctl start
   ```

5. Set the environment variables. Instructions will be printed to the console. For example:

   ```console
   $ export SPARK_CONF_DIR=$(pwd)/conf
   $ export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
   $ export PYSPARK_PYTHON=$(which python)
   ```

6. Connect with Ibis using a traditional SparkSession.

   ```console
   $ python
   ```
   ```python
   >>> import ibis
   >>> from pyspark.sql import SparkSession
   >>> import socket
   >>> master_url = f"spark://{socket.gethostname()}:7077"
   >>> spark = SparkSession.builder.master(master_url).appName("ibis").getOrCreate()
   >>> con = ibis.pyspark.connect(session=spark)
   >>> ibis.set_backend(con)
   >>> # Create and query tables
   >>> t = ibis.memtable({"x": [10, 20, 30], "y": [1.1, 2.2, 3.3]})
   >>> t.filter(t.x > 15).execute()
   ```

7. When finished, stop the cluster.

   ```console
   $ sparkctl stop
   ```

## Working with Data Files

Ibis with PySpark can read data files directly. Here are examples of common operations:

```python
import ibis
from sparkctl import ClusterManager, make_default_spark_config

config = make_default_spark_config()
mgr = ClusterManager(config)

with mgr.managed_cluster() as spark:
    con = ibis.pyspark.connect(session=spark)
    ibis.set_backend(con)

    # Read a Parquet file
    t = con.read_parquet("/path/to/data.parquet")

    # Read a CSV file
    t = con.read_csv("/path/to/data.csv")

    # Perform Ibis operations
    result = (
        t.group_by("category")
        .agg(
            count=t.count(),
            total=t.value.sum(),
            avg_value=t.value.mean(),
        )
        .order_by(ibis.desc("total"))
    )

    # Execute and get results
    print(result.execute())

    # Write results back to Parquet
    result.to_parquet("/path/to/output.parquet")
```

## Choosing Between Approaches

| Approach | Connection Type | Best For |
|----------|-----------------|----------|
| Option A: Managed Cluster | Spark Connect | Scripts with defined start/end, automated pipelines, automatic cleanup |
| Option B: CLI + Spark Connect | Spark Connect | Interactive exploration, multiple scripts against same cluster |
| Option C: CLI + Traditional | Full PySpark | Full PySpark API access, Spark Connect compatibility issues |

**Spark Connect vs Traditional PySpark:**

- **Spark Connect** is a lightweight client protocol. The driver runs on the cluster, and your
  Python code communicates with it over gRPC. This is the default for sparkctl's `managed_cluster()`.
- **Traditional PySpark** runs the driver in your Python process. This gives you access to the full
  PySpark API but requires `PYSPARK_PYTHON` to be set correctly.

Option A (Managed Cluster) is recommended for most workflows because it ensures the cluster is
properly stopped even if your script encounters an error.
