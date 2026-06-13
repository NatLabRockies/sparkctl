# How to enable GPU acceleration (experimental)

```{eval-rst}
.. warning:: GPU support is **experimental and untested**. The options below configure Spark's
   GPU-aware scheduling and, optionally, the NVIDIA RAPIDS Accelerator, but they have not been
   validated on a real GPU cluster. Treat the generated settings as a starting point and expect to
   tune them for your site.
```

## GPU-aware scheduling

Enable GPU scheduling so Spark workers advertise their GPUs and executors/tasks request them:

```console
$ sparkctl configure --gpus
```

sparkctl detects the number of GPUs per node from the compute environment (Slurm GPU variables such
as `SLURM_GPUS_ON_NODE`, or `nvidia-smi` in a native environment). Override the count when detection
is unavailable or incorrect:

```console
$ sparkctl configure --gpus --gpus-per-node 4
```

This generates a GPU discovery script in the cluster's `conf` directory and writes these settings to
`spark-defaults.conf`:

- `spark.worker.resource.gpu.amount` and `spark.worker.resource.gpu.discoveryScript`
- `spark.executor.resource.gpu.amount` and `spark.executor.resource.gpu.discoveryScript`
- `spark.task.resource.gpu.amount`

By default each executor is assigned one GPU and tasks share that GPU
(`task.resource.gpu.amount = executor_gpu_amount / executor_cores`). Tune these through your
settings file:

```toml
[runtime]
enable_gpus = true
gpus_per_node = 4
executor_gpu_amount = 1
task_gpu_amount = 0.25
```

## RAPIDS Accelerator

The [NVIDIA RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/) offloads
SQL and DataFrame operations to GPUs.

1. Download the RAPIDS Accelerator jar from the
   [RAPIDS download page](https://nvidia.github.io/spark-rapids/docs/download.html).

2. Record its path when creating your settings file:

   ```console
   $ sparkctl default-config --rapids-jar-file /path/to/rapids-4-spark_2.13-<version>.jar \
       /path/to/spark /path/to/java
   ```

3. Enable RAPIDS (this implies `--gpus`):

   ```console
   $ sparkctl configure --rapids
   ```

This adds the plugin jar with `spark.jars`, sets `spark.plugins com.nvidia.spark.SQLPlugin`, and
enables `spark.rapids.sql.enabled`.

```{eval-rst}
.. note:: ``spark.jars`` is used instead of ``spark.{driver,executor}.extraClassPath`` so the
   RAPIDS jar does not conflict with the classpath entries the PostgreSQL Hive metastore sets.
```
