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
(`spark.task.resource.gpu.amount = executor_gpu_amount / executor_cores`). Tune these through your
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

## What your application needs to do

What you change in your application depends on *how* you intend to use the GPUs.

### SQL / DataFrame workloads with RAPIDS

For SQL and DataFrame queries, the RAPIDS Accelerator is mostly transparent: once the plugin is
enabled (above), supported operators run on the GPU with no code changes. The important caveat is
that **not every operator is GPU-accelerated** — unsupported expressions, data types, and many
Python/Scala UDFs silently fall back to the CPU, and a query that bounces between CPU and GPU can be
slower than staying on the CPU.

Before assuming a query is GPU-accelerated, ask RAPIDS what it actually placed on the GPU:

```python
spark.conf.set("spark.rapids.sql.explain", "NOT_ON_GPU")  # log every operator that fell back
df.explain()  # "GPU" nodes ran on the GPU; "Project"/"Filter" without "Gpu" fell back to CPU
```

Useful tuning knobs (set in your settings file's `spark_defaults` or at runtime):

- `spark.rapids.sql.concurrentGpuTasks` (sparkctl defaults to `1`) — how many tasks share a GPU at
  once. Raising it to `2`–`4` can improve throughput if GPU memory allows.
- `spark.sql.files.maxPartitionBytes` — larger partitions (e.g. `512m`) give the GPU more work per
  task, which it prefers over many tiny tasks.
- Keep Adaptive Query Execution on (`spark.sql.adaptive.enabled true`, the Spark default).

### Custom GPU code (no RAPIDS)

If you call GPU libraries directly (e.g. CuPy, PyTorch, RAPIDS cuDF, or XGBoost) inside your tasks,
RAPIDS does not apply. You still enable GPU-aware scheduling with `--gpus` so Spark assigns GPUs to
tasks, then read the assigned GPU address from the task context and pin your library to it:

```python
from pyspark import TaskContext

def run_on_gpu(rows):
    ctx = TaskContext.get()
    gpu = ctx.resources()["gpu"].addresses[0]  # address(es) assigned to this task
    import cupy
    with cupy.cuda.Device(int(gpu)):
        ...  # your GPU work here

rdd.mapPartitions(run_on_gpu).collect()
```

Pinning to the assigned address is what keeps two tasks on the same node from fighting over the same
device. The `spark.task.resource.gpu.amount` value sparkctl writes controls how many tasks Spark
will co-schedule on each GPU.

## When are GPUs worth it?

GPUs are not a blanket speedup for Spark — they help some workloads dramatically and slow others
down. Reach for GPUs when most of these hold:

- **Large data and heavy compute.** Multi-GB-to-TB scans with joins, aggregations, sorts, window
  functions, or `expand`/`hash` heavy plans. The GPU's advantage grows with data volume; small jobs
  are dominated by launch and transfer overhead.
- **Columnar formats.** Parquet/ORC/CSV at scale, where RAPIDS can read and process columns
  directly on the GPU.
- **Operations RAPIDS supports.** Standard SQL/DataFrame expressions and supported types. Check with
  `spark.rapids.sql.explain` (above) — a plan full of CPU fallbacks will not benefit.
- **ML training/inference** with GPU-native libraries (XGBoost, deep learning, RAPIDS cuML).

GPUs usually do **not** help, and can be slower or more expensive per result, when:

- The dataset is small or the job is short — fixed GPU overhead dominates.
- The work is dominated by **Python/Scala UDFs**, complex regex, or other operators that fall back
  to the CPU (data must round-trip between CPU and GPU memory).
- The job is I/O- or shuffle-network-bound rather than compute-bound.
- Per-partition working sets exceed GPU memory, forcing spills.

```{eval-rst}
.. tip:: Before committing a workload to GPUs, run NVIDIA's
   `Spark RAPIDS qualification tool <https://docs.nvidia.com/spark-rapids/user-guide/latest/qualification/quickstart.html>`_
   against the CPU run's event logs. It estimates the speedup (and flags unsupported operators)
   from a real run, which is more reliable than guessing — especially given that GPU support in
   sparkctl is still experimental and unvalidated.
```
