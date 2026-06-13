(how-tos-debugging)=
# Debugging
This page describes how to debug certain Spark errors when using sparkctl.

(spark-web-ui)=
## Spark web UI
The web UI is a good first place to look for problems. Connect to ports 8080 and 4040 on the nodes
running Spark. You may need to create an ssh tunnel. This is an example that creates a tunnel
between a user's Mac laptop and NLR's Kestrel HPC (adjust as necessary for Windows):

```console
$ export COMPUTE_NODE=<your-compute-node-name>
$ ssh -L 4040:$COMPUTE_NODE:4040 -L 8080:$COMPUTE_NODE:8080 $USER@kestrel.hpc.nrel.gov
```

Open your browser to http://localhost:4040 after configuring the tunnel to access the web UI.

## Log files
sparkctl configures Spark to record log files in the base directory. Spark master, worker, connect
server, etc, will be in `./spark_scratch/logs`. Executor logs will be in
`./spark_scratch/workers/app-*/*/stderr`

For example,
```console
$ tree spark_scratch
spark_scratch
├── local
├── logs
│   ├── spark-dthom-org.apache.spark.deploy.master.Master-1-dthom-39537s.out
│   ├── spark-dthom-org.apache.spark.deploy.worker.Worker-1-dthom-39537s.out
│   ├── spark-dthom-org.apache.spark.sql.connect.service.SparkConnectServer-1-dthom-39537s.out
└── workers
    └── app-20250712164208-0000
        ├── 0
        │   ├── stderr
        │   └── stdout
        └── 1
            ├── stderr
            └── stdout
```

Connection errors are often logged in the master/worker/connect server log files. Problems with
queries are often visible in the executor `stderr` files. For example, if a job seems stalled,
you can tail the `stderr` files to see what is happening:

```console
$ tail -f spark_scratch/workers/*/*/stderr
```

If you have many executors, you may want to tail only the most recent ones. Identify them with
```console
$ find spark_scratch -type f -name stderr -exec stat -c '%Y %y %n' {} + 2>/dev/null | sort -n
```

## Spark shuffle partitions
A common performance issue when running complex queries is due to a non-ideal setting for
`spark.sql.shuffle.partitions`. The default Spark value is 200. Some online sources recommend
setting it to 1-4x the total number of CPUs in your cluster.

sparkctl tries to help by automatically detecting the number of CPUs in your cluster and setting
this value to that number. You can give sparkctl a multiplier to increase this. For example, if your
cluster has 800 CPUs and you want 4x that, set this option when you configure the cluster:

```console
$ sparkctl configure --shuffle-partition-multiplier 4 
```

Now you will get 3200 shuffle partitions. You can set a specific value by running
`sparkctl configure`, then modify `conf/spark-defaults.conf` with your value, then running
`sparkctl start`.

This
[video](https://www.youtube.com/watch?v=daXEp4HmS-E&t=4251s) by a Spark developer offers a
recommendation worked out very well for us.

Use this formula:

```console
  num_partitions = max_shuffle_write_size / target_partition_size
```

You will have to run your job once to determine `max_shuffle_write_size`. You can find it on the
Spark UI `Stages` tab in the `Shuffle Write` column. Your `target_partition_size` should be between
128 - 200 MB.

The minimum partitions value should be the total number of cores in the cluster unless you want to
leave some cores available for other jobs that may be running simultaneously.

## Slow local storage
Spark will write lots of temporary data to local storage during shuffle
operations. If your joins cause lots of shuffling, it is very important that
your local storage be fast. If you direct Spark to use the shared filesystem (e.g., Lustre) for
local storage, your mileage will vary. If the system is idle, it might work. If
it is saturated, your job may fail. A lot depends on the backend storage of your shared filesystem.

If you suspect that jobs are timing out because of slow shuffle write and reads, use local storage
instead. You can do this by acquiring compute nodes with local storage and then set this option
when you configure your Spark cluster:

```console
$ sparkctl configure --local-storage
```

(spilling-to-disk)=
## Executors are spilling to disk
If you observe that your Spark job is running very slowly, you may have a problem with executors
spilling to disk. This occurs when the executor memory is too small to hold all of the data that it
is processing. When this happens, Spark will spill data to disk, which is much slower than
processing in memory.

One way to detect this condition is to look at the executor `stderr` log files. You will see this 
log message over and over:

```console
25/07/15 14:24:06 INFO UnsafeExternalSorter: Thread 60 spilling sort data of 4.6 GiB to disk (201  times so far)
```

If this has occurred 201 times, then you might be better off canceling the job and re-running with a
different configuration.

First, ensure that you have set spark.sql.shuffle.partitions to a reasonable value, as discussed
above.

Second, your executor memory may be too small. For example, your compute node's CPU-to-memory ratio
may be too high. Suppose that in the example above where the executor had to spill 4.6 GiB of data
to disk, the executor was configured with 7 GB of memory. You could reconfigure the cluster to
double the executor memory. This can be done in the `sparkctl configure` command indirectly by
doubling the executor cores (`--executor-cores=10`) or directly by setting `--executor-memory-gb`.
You can also set the `spark.executor.memory` value in the `conf/spark-defaults.conf` file in between
running `sparkctl configure` and `sparkctl start`.

Third, you may be experiencing data skew. This has happened frequently for us when we are
perform disaggregation or duplication mappings that explode data sizes. Refer to the next section.

## Data skew
This topic is addressed by many online sources. This is a technique that has worked for us.

If your query will produce high data skew, you can use a salting technique to balance the data.
For example,

```python
df.withColumn("salt_column", F.lit(F.rand() * (num_partitions - 1))) \
    .groupBy("county", "salt_column") \
    .agg(F.sum("county")) \
    .drop("salt_column")
```

Here is a
[resource](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage-page)
from DataBricks with more information.

## Performance monitoring
If your job is running slowly, check CPU utilization on your compute nodes.

Here is an example of how to run htop on multiple nodes simultaneously with `tmux`.

Download this [script](https://raw.githubusercontent.com/johnko/ssh-multi/master/bin/ssh-multi)

Run it like this:

```console
$ ./ssh-multi node1 node2 ... nodeN
```
It will start tmux with one pane for each node and synchonize mode enabled. Typing in one pane types
everywhere.

```console
$ htop
```

```{eval-rst}
.. note:: The file `./conf/workers` contains the node names in your cluster.
```

### Automated resource monitoring
sparkctl integrates with [rmon](https://github.com/NatLabRockies/resource_monitor) to collect resource
utilization stats from all compute nodes in your cluster. You can enable this monitoring when
you configure the cluster:

```console
$ sparkctl configure --resource-monitor
```

The stats will be summarized and plotted in HTML files in `./stats-output` when you stop the
cluster.

### Spark tuning resources
- Spark tuning guide from Apache: https://spark.apache.org/docs/latest/tuning.html
- Spark tuning guide from Amazon: https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
- Performance recommendations: https://www.youtube.com/watch?v=daXEp4HmS-E&t=4251s
