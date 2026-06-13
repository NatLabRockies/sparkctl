# Run jobs interactively with sparkctl

In this tutorial you will learn how to start a Spark cluster on HPC compute nodes and then run
Spark jobs interactively through `pyspark-client` with the Spark Connect Server.

Unlike the tutorials that call for starting Spark on the command line, sparkctl sets
environment variables for you.

1. Allocate compute nodes, such as with Slurm. This example acquires 4 CPUs and 30 GB of memory
   for the Spark master process and user application + Spark driver and 2 complete nodes for Spark
   workers.

   ```console
   $ salloc -t 01:00:00 -n4 --partition=shared --mem=30G : -N2 --account=<your-account> --mem=240G
   ```

2. Activate the Python environment that contains sparkctl.

   ```console
   $ module load python
   $ source ~/python-envs/sparkctl/bin/activate
   ```

3. Configure and start the Spark cluster.

   ```{eval-rst}
   .. note:: This workflow requires that you enable the Spark Connect Server.
   ```
   
   ```python
   from sparkctl import ClusterManager, make_default_spark_config
   
   # This loads your global sparkctl configuration file (~/.sparkctl.toml).
   config = make_default_spark_config()
   config.runtime.start_connect_server = True
   # Set other options as desired.
   mgr = ClusterManager(config)
   mgr.configure()
   ```
   ```console
   2025-07-12 13:00:24.327 | INFO     | sparkctl.cluster_manager:_add_spark_settings_to_defaults_file:281 - Set driver memory to 10 GB
   2025-07-12 13:00:24.328 | INFO     | sparkctl.cluster_manager:_config_executors:352 - Configured Spark to start 2 executors
   2025-07-12 13:00:24.328 | INFO     | sparkctl.cluster_manager:_config_executors:353 - Set spark.sql.shuffle.partitions=10 and spark.executor.memory=2g
   2025-07-12 13:00:24.328 | INFO     | sparkctl.cluster_manager:configure:100 - Configured Spark workers to use /scratch/dthom/sparkctl/spark_scratch for shuffle data.
   2025-07-12 13:00:24.329 | INFO     | sparkctl.cluster_manager:_write_workers:456 - Wrote worker 1 to /scratch/dthom/sparkctl/conf/workers
   2025-07-12 13:00:24.329 | INFO     | sparkctl.cluster_manager:configure:108 - Wrote sparkctl configuration to /scratch/dthom/repos/sparkctl/config.json
   ```
   ```python
   mgr.start()
   ```
   ```console
   starting org.apache.spark.deploy.master.Master, logging to /scratch/dthom/sparkctl/spark_scratch/logs/spark-dthom-org.apache.spark.deploy.master.Master-1-dthom-39537s.out
   2025-07-12 13:00:32.052 | INFO     | sparkctl.cluster_manager:_start:176 - Started Spark master processes on dthom-39537s
   starting org.apache.spark.sql.connect.service.SparkConnectServer, logging to /scratch/dthom/repos/sparkctl/spark_scratch/logs/spark-dthom-org.apache.spark.sql.connect.service.SparkConnectServer-1-dthom-39537s.out
   2025-07-12 13:00:34.764 | INFO     | sparkctl.cluster_manager:_start:181 - Started Spark connect server
   starting org.apache.spark.deploy.worker.Worker, logging to /scratch/dthom/sparkctl/spark_scratch/logs/spark-dthom-org.apache.spark.deploy.worker.Worker-1-dthom-39537s.out
   2025-07-12 13:00:37.648 | INFO     | sparkctl.cluster_manager:_start:200 - Spark worker memory = 4 GB
   ```
   
4. Run a Spark job.

   ```python
   spark = mgr.get_spark_session()
   df = spark.createDataFrame([(x, x + 1) for x in range(1000)], ["a","b"])
   df.show(n=5)
   ```
   ```console
   +---+---+
   |  a|  b|
   +---+---+
   |  0|  1|
   |  1|  2|
   |  2|  3|
   |  3|  4|
   |  4|  5|
   +---+---+
   only showing top 5 rows 
   ```

5. Shut down the cluster.

   ```python
   mgr.stop()
   ```
