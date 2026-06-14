# Run jobs with Spark Connect

In this tutorial you will learn how to start a Spark cluster on HPC compute nodes with the sparkctl
command-line interface and then run Spark jobs in Python through `pyspark-client` with the Spark
Connect Server.

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

3. Configure the Spark cluster. The sparkctl code will detect the compute nodes based on
   Slurm environment variables.

   ```console
    $ sparkctl configure
    ```
    
4. Optional, inspect the Spark configuration in `./conf`.
    
5. Start the cluster.

    ```console
    $ sparkctl start
    ```

6. Set the environment variables `SPARK_CONF_DIR` and `JAVA_HOME`. This will ensure that your
   application uses the Spark settings created in step 2. Instructions will be printed to the
   console. For example:

   ```console
   $ export SPARK_CONF_DIR=$(pwd)/conf
   $ export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
   ```
   
7. Connect to the Spark Connect Server and run a job.

   ```console
   $ python
   ```
   ```python
   >>> from pyspark.sql import SparkSession
   >>> spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
   >>> df = spark.createDataFrame([(x, x + 1) for x in range(1000)], ["a","b"])
   >>> df.show()
   ```
