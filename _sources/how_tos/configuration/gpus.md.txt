# How to use GPU acceleration

## RAPIDS Accelerator

The [NVIDIA RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/) is the
recommended way to accelerate Spark workloads on NVIDIA GPUs. It offloads SQL and DataFrame
operations to GPUs with no code changes — supported operators run on the GPU automatically, and
unsupported ones fall back to CPUs.

1. Download the RAPIDS Accelerator jar from the
    [RAPIDS download page](https://nvidia.github.io/spark-rapids/docs/download.html).
    (This may have already been done by your local sparkctl administrator.)

2. If needed, record its path when creating your settings file:

    ```console
    $ sparkctl default-config \
        --rapids-jar-file /path/to/rapids-4-spark_2.13-<version>.jar \
        /path/to/spark \
        /path/to/java
    ```

3. Enable RAPIDS:

    ```console
    $ sparkctl configure --rapids
    ```

This enables GPU-aware scheduling behind the scenes and writes these settings to
`spark-defaults.conf`:

- Plugin jar via `spark.jars`, plugin registration via `spark.plugins com.nvidia.spark.SQLPlugin`
- GPU-aware scheduling (`spark.worker.resource.gpu.*`, `spark.executor.resource.gpu.*`,
  `spark.task.resource.gpu.*`)
- `spark.rapids.sql.enabled`

```{eval-rst}
.. note:: ``spark.jars`` is used instead of ``spark.{driver,executor}.extraClassPath`` so the
   RAPIDS jar does not conflict with the classpath entries the PostgreSQL Hive metastore sets.
```

### Verifying GPU acceleration

Not every operator is GPU-accelerated. Unsupported expressions, data types, and Python/Scala UDFs
fall back to the CPU, and a query that bounces between CPU and GPU can be slower than staying on
the CPU. Before assuming a query is GPU-accelerated, ask RAPIDS what it actually placed on the GPU:

```python
spark.conf.set("spark.rapids.sql.explain", "NOT_ON_GPU")  # log every operator that fell back
df.explain()  # "GPU" nodes ran on the GPU; "Project"/"Filter" without "Gpu" fell back to CPU
```

### Tuning RAPIDS

Useful tuning knobs (set in your settings file's `spark_defaults` or at runtime):

- `spark.rapids.sql.concurrentGpuTasks` (sparkctl defaults to `1`) — how many tasks share a GPU at
  once. Raising it to `2`–`4` can improve throughput if GPU memory allows.
- `spark.sql.files.maxPartitionBytes` — larger partitions (e.g. `512m`) give the GPU more work per
  task, which it prefers over many tiny tasks.
- Keep Adaptive Query Execution on (`spark.sql.adaptive.enabled true`, the Spark default).

### Custom GPU code (advanced)

If you call GPU libraries directly (e.g. CuPy, PyTorch, RAPIDS cuDF, or XGBoost) inside your tasks,
RAPIDS does not apply. The section below explains how GPU scheduling works; once enabled, pin each
task to its assigned GPU using the task context:

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

Pin to the assigned address to keep two tasks on the same node from fighting over the same device.
The `spark.task.resource.gpu.amount` value sparkctl writes controls how many tasks Spark will
co-schedule on each GPU.

## Under the hood: GPU scheduling

### Executor sizing

`sparkctl configure --rapids` enables GPU-aware scheduling automatically. When GPU scheduling is
enabled and you do not set `executor_cores` explicitly, sparkctl follows NVIDIA's recommended layout:
**one executor per GPU**, with the node's usable cores divided evenly among them. On a node with 4
GPUs and 64 cores you get 4 executors with ~15 cores each, so every GPU is used and each has a
healthy pool of CPU cores to feed it (I/O, decompression, shuffle). To use all *N* GPUs you
therefore need at least *N* cores in the allocation; request cores generously (e.g. Slurm
`--cpus-per-task` or `--exclusive`). If CPUs or memory only allow fewer executors than there are
GPUs, sparkctl logs a warning that some GPUs will sit idle.

Set `executor_cores` explicitly to override this. Tune the GPU assignment through your settings file:

```toml
[runtime]
enable_gpus = true
gpus_per_node = 4
executor_gpu_amount = 1
task_gpu_amount = 0.25
# executor_cores = 16   # optional; omit to auto-size one executor per GPU
```

### GPU discovery and placement

sparkctl detects the number of GPUs per node from the compute environment (Slurm GPU variables such
as `SLURM_GPUS_ON_NODE`, or `nvidia-smi` in a native environment). Override the count when detection
is unavailable or incorrect:

```console
$ sparkctl configure --gpus --gpus-per-node 4
```

sparkctl writes a GPU discovery script ($SPARK_CONF_DIR/get_gpus_resources.sh) that Spark calls on
each worker. The script reports the GPUs visible to Spark as JSON
(`{"name": "gpu", "addresses": ["0", "1", ...]}`), preferring `CUDA_VISIBLE_DEVICES` when set
(becomes essential when executors run in containers where `nvidia-smi` may be unavailable) and
falling back to `nvidia-smi`.

```{eval-rst}
.. note:: On a multi-node Slurm job every worker node must have the **same** number of GPUs, because
   sparkctl writes a single ``spark.worker.resource.gpu.amount`` for all workers. Request GPUs
   per node (``--gpus-per-node=4``) rather than as a job-wide total (``--gpus=4``), which Slurm can
   split unevenly across nodes. ``sparkctl configure`` fails fast if it detects a non-uniform
   distribution.
```

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
