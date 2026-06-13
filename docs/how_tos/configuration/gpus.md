# How to enable GPU acceleration

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
(`spark.task.resource.gpu.amount = executor_gpu_amount / executor_cores`).

### Executor sizing

When GPUs are enabled and you do not set `executor_cores`, sparkctl follows NVIDIA's recommended
layout: **one executor per GPU**, with the node's usable cores divided evenly among them. For
example, on a node with 4 GPUs and 64 cores you get 4 executors with ~15 cores each, so every GPU is
used and each has a healthy pool of CPU cores to feed it (I/O, decompression, shuffle). To use all
*N* GPUs you therefore need at least *N* cores in the allocation; request cores generously (e.g.
Slurm `--cpus-per-task` or `--exclusive`). If CPUs or memory only allow fewer executors than there
are GPUs, sparkctl logs a warning that some GPUs will sit idle.

Setting `executor_cores` explicitly overrides this and is honored as-is. Tune the GPU assignment
through your settings file:

```toml
[runtime]
enable_gpus = true
gpus_per_node = 4
executor_gpu_amount = 1
task_gpu_amount = 0.25
# executor_cores = 16   # optional; omit to auto-size one executor per GPU
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

## Monitor GPU usage while a job runs

sparkctl's built-in resource monitor (`--resource-monitor`) only collects CPU, memory, disk, and
network stats — **it does not capture GPU utilization**. Use NVIDIA's tools directly.

GPU work happens on the **worker/executor nodes**, so monitor there, not on the node where you
launched the driver. From a login node, attach a second shell to the same Slurm allocation:

```console
$ srun --overlap --jobid=$SLURM_JOB_ID --nodes=1 --pty bash
```

Then use any of:

```console
$ nvidia-smi -l 1                     # full table, refreshed every second
$ nvidia-smi dmon -s pucvmet -d 1     # scrolling per-GPU metrics; best for watching a live job
$ nvtop                               # htop-style TUI, incl. per-process GPU memory (if available)
```

To log the whole run to a CSV for later analysis:

```console
$ nvidia-smi --query-gpu=timestamp,index,utilization.gpu,utilization.memory,memory.used,power.draw \
    --format=csv -l 1 > gpu_$(hostname).csv
```

To watch every node at once on a multi-node cluster (node names are in `conf/workers`):

```console
$ srun --overlap --jobid=$SLURM_JOB_ID --ntasks-per-node=1 nvidia-smi dmon -c 120 -d 1
```

Watch `utilization.gpu` while a query runs. Sustained high utilization means operators really are
executing on the GPU; near-zero utilization while CPUs are busy means the work is falling back to
the CPU — cross-check with `spark.rapids.sql.explain` (see above).

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
   from a real run, which is more reliable than guessing.
```
