# How to run Spark jobs in Python
These instructions assume that have allocated compute nodes and started a Spark cluster.

For all steps below, set the environment variable `SPARK_CONF_DIR`, pointed at your configuration directory.

```console
$ export SPARK_CONF_DIR=$(pwd)/conf
```
   
## Interactive session with pyspark

1. Start a Spark process.

   If using pyspark-client (via Spark Connect Server):
   ```console
   $ python
   >>> from pyspark.sql import SparkSession
   >>> spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
   ```

   If using pyspark:
   ```console
   $ pyspark --master spark://$(hostname):7077
   ```
   
2. Optional: check your environment to ensure that all configuration settings are correct.
Most importantly, ensure that you connected to the Spark cluster master and are not in local mode.
pyspark prints the connection information during startup. For example:
    ```
    Spark context available as 'sc' (master = spark://r2i7n35:7077, app id = app-20221202224041-0000).
    ```
    You can dump all configuration settings with this command:
    ```
    >>> spark.sparkContext.getConf().getAll()
    ```

3. Create or load dataframes with the global `SparkSession` variable named `spark`:
   ```
   >>> df = spark.read.parquet("my_data.parquet")
   >>> df.show()
   ```

## Jupyter notebook
The command below will allow you to connect to the Spark cluster in a Jupyter notebook.

Start the notebook server

```console
$ jupyter notebook --no-browser --port=8889 --ip=0.0.0.0
```

The Jupyter process will print a URL to the terminal. You can access it from your laptop after
you forward the ports through an ssh tunnel.

```console
$ ssh -L 8889:<your-compute-node-name>:8889 $USER@kestrel.hpc.nrel.gov
```

## Script execution with spark-submit

```{eval-rst}
.. warning:: spark-submit is available if you install `pyspark` but not `pyspark-client`.
```

1. Set the environment variable `SPARK_CONF_DIR`, pointed at your configuration directory.

   ```console
   $ export SPARK_CONF_DIR=$(pwd)/conf
   ```

2. Start a Spark process.

   ```console
   $ spark-submit --master spark://$(hostname):7077 my_job.py
   ```

   where `my_job.py` contains this code to connect to Spark:
   
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("my_app").getOrCreate()
   ```
