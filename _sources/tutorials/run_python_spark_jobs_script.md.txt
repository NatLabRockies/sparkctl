# Run jobs in a Python script

In this tutorial you will learn how to run Spark jobs in a Python script with a sparkctl-managed
Spark cluster.

The key difference between this and other tutorials is that this tutorial uses `sparkctl` as a
Python library to hide the details of starting the cluster and setting environment variables.

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

3. Add the code below to a Python script named `my_job.py`. This code block will configure and
   start the Spark cluster, run your Spark job, and then stop the cluster.

   ```python
   from sparkctl import ClusterManager, make_default_spark_config

   # This loads your global sparkctl configuration file (~/.sparkctl.toml).
   config = make_default_spark_config()
   # Set runtime options as desired.
   # config.runtime.driver_memory_gb = 20
   # config.runtime.use_local_storage = True
   mgr = ClusterManager(config)
   with mgr.managed_cluster() as spark:
       df = spark.createDataFrame([(x, x + 1) for x in range(1000)], ["a", "b"])
       df.show()
   ```

4. Run the script.

   ```console
   $ python my_job.py
   ```
