# Start a Spark Cluster

This page assumes that you have allocated compute nodes via Slurm.

1. Activate the Python environment that contains sparkctl.

   ```console
   $ module load python
   $ source ~/python-envs/sparkctl/bin/activate
   ```

2. Configure and start the Spark cluster. The sparkctl code will detect the compute nodes based on
   Slurm environment variables.

   ```console
    $ sparkctl configure
    ```

3. Optional, inspect the Spark configuration in `./conf`.
    
4. Start the cluster.

    ```console
    $ sparkctl start
    ```
    
5. Set the environment variable `SPARK_CONF_DIR`. This will ensure that your application uses the
   Spark settings created in step 2. Instructions will be printed to the console. By default, it
   will be

   ```console
   $ export SPARK_CONF_DIR=$(pwd)/conf
   ```

6. Set the `JAVA_HOME` environment variable to be the same as the Java used by Spark. This should
   be in your `~/.sparkctl.toml` configuration file.

   ```console
   $ export JAVA_HOME=/datasets/images/apache_spark/jdk-21.0.7
   ```

7. Run your application.

8. Shut down the cluster.

   ```console
   $ sparkctl stop
   ```
