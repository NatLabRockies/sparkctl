(sparkctl-faqs)=
# Frequently Asked Questions

## Installation and Setup

### Why do I need both sparkctl and pyspark?

sparkctl manages the Spark cluster (starting/stopping processes, generating configuration). pyspark
provides the Spark client libraries you use to submit jobs and interact with the cluster. They
serve different purposes:

- **sparkctl**: Cluster lifecycle management
- **pyspark**: Spark application development and submission

You can install both together with `pip install "sparkctl[pyspark]"`.

### What's the difference between `~/.sparkctl.toml` and `./conf/`?

- **`~/.sparkctl.toml`**: Your personal configuration that persists across sessions. Contains paths
  to Spark/Java and HPC-specific settings. Set this once per user.
- **`./conf/`**: Generated fresh each time you run `sparkctl configure`. Contains Spark
  configuration specific to your current HPC allocation and working directory.

See {ref}`configuration-files` for details.

## Running Jobs

### My job is running in Spark local mode and did not attach to the cluster master.
Ensure `SPARK_CONF_DIR` is set correctly before running your application:

```console
$ export SPARK_CONF_DIR=/path/to/spark/conf
```

### My job fails with "Java not found" or "JAVA_HOME not set"

Ensure `JAVA_HOME` is set correctly before running your application:

```console
$ export JAVA_HOME=/path/to/java
```

The Java path should match what's in your `~/.sparkctl.toml`. sparkctl uses this for Spark
processes, but your application also needs it set.

### I'm getting an error about mismatched Python versions.

The cluster may be running a different Python version than your application. Set this
environment variable before running your application:

```console
$ export PYSPARK_PYTHON=$(which python)
```

### My workers fail to find a shared library (e.g. `libpython3.11.so`).

This happens when the Python (or other dependency) you loaded before allocating nodes relies on an
environment that is not present on the worker nodes by default. In a Slurm environment, sparkctl
launches workers with `srun`, which forwards your full submission environment — environment
modules, virtual environments, and `LD_LIBRARY_PATH` — to the worker nodes. This usually resolves
such errors without extra configuration.

If your site's Slurm configuration does not work with sparkctl's `srun` invocation, you can fall
back to launching workers over `ssh` by setting `use_srun = false` in the `[compute]` section of
your `~/.sparkctl.toml`. Note that `ssh` does not forward your environment, so you may then need to
export the relevant variables yourself, for example through `spark-env.sh` in the generated
`./conf/` directory.

### I'm getting an error about mismatched Spark versions.

If you are running pyspark/spark-submit after installing via `pip install sparkctl[pyspark]`,
your version of pyspark must match the cluster version exactly. Client version 4.1.3 is
incompatible with cluster version 4.1.2.

### Why can't my workers connect to the master?

Common causes:

1. **High-bandwidth nodes**: Some NLR Kestrel compute nodes have two network cards, which Spark
   cannot deal with. Set `--constaint lbw` when allocating nodes.

Check the Spark master logs in `./spark_scratch/logs/` for connection errors.

### How do I connect to the Spark Web UI?

The Spark master runs a web UI on port 4040 (driver) or 8080 (master). Since HPC compute nodes
aren't directly accessible, use SSH tunneling:

```console
$ ssh -L 8080:$(hostname):8080 user@hpc-login-node
```

Then open `http://localhost:8080` or `http://localhost:4040` in your browser.

See {ref}`spark-web-ui` for more details.

### Why is my job slow?

Common causes:

1. **Shuffle spilling**: Executors are writing shuffle data to disk. Increase executor memory or
   use faster local storage.
2. **Data skew**: Some partitions have much more data than others. Consider salting keys.
3. **Too few partitions**: Increase `spark.sql.shuffle.partitions`.
4. **Too many partitions**: Decrease partitions if you have many small tasks.
5. **Slow storage**: Ensure shuffle storage uses fast local SSDs, not shared filesystem.
7. **Query too complex**: If you are trying to run a very complex query where subtasks have
   different data sizes and partitioning needs, consider breaking the query into smaller parts with
   different settings. Persist intermediate results to the filesystem so that you can checkpoint and
   make incremental progress.
6. **Non-ideal partitioning**: If you are trying to partition-by-column in the same query as your
   main work, especially where you significantly increased the shuffle partitions, persist your
   main work first. Then repartition in a second task.

See the {ref}`how-tos-debugging` for performance troubleshooting.

## Configuration

### How do I increase executor memory?

sparkctl calculates executor memory automatically based on available RAM. To override:

Edit the `./conf/spark-defaults.conf` after running `sparkctl configure` but before
starting the cluster.

### Can I use YARN or Kubernetes instead of Standalone mode?

No. sparkctl is designed specifically for Standalone mode because:

- It requires no cluster-wide services
- It works without admin access
- It's designed for ephemeral, user-controlled clusters

If you have YARN or Kubernetes available, you likely don't need sparkctl.

## Slurm Integration

### What Slurm allocation should I use?

A typical allocation for sparkctl:

```console
$ salloc -t 01:00:00 -n4 --mem=30G : -N2 --mem=240G
```

This creates:
- A "master" allocation (4 cores, 30GB) for the Spark master and your driver
- Worker nodes (2 full nodes) for Spark executors

See {ref}`heterogeneous-slurm-jobs` for details.

### Can I use sparkctl in a batch script (sbatch)?

Yes. Example sbatch script:

```bash
#!/bin/bash
#SBATCH --account=my_account
#SBATCH --nodes=3
#SBATCH --time=02:00:00
#SBATCH --mem=240G

source ~/python-envs/sparkctl/bin/activate
sparkctl configure
sparkctl start
export SPARK_CONF_DIR=$(pwd)/conf
export JAVA_HOME=/path/to/java
spark-submit --master spark://$(hostname):7077 my-job.py
sparkctl stop
```

### What happens if a compute node fails?

By default, Slurm kills the entire job if any node fails. For long-running jobs, consider
{ref}`compute-node-failures`.

## Spark Connect

### When should I use Spark Connect?

Use Spark Connect when:

- You want a minimal client installation (`pyspark-client` is much smaller than `pyspark`)
- You're connecting from a remote machine
- You want better isolation between client and cluster Python environments

### What are the limitations of Spark Connect?

Some Spark APIs behave differently with Spark Connect. See the [official documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html#how-spark-connect-client-applications-differ-from-classic-spark-applications) for details.

### How do I enable Spark Connect?

```console
$ sparkctl configure --connect-server
$ sparkctl start
```

Then connect with:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://hostname:15002").getOrCreate()
```

## Troubleshooting

### Where are the log files?

- **Spark master/worker logs**: `./spark_scratch/logs/`
- **sparkctl log**: `./sparkctl.log`
- **Executor logs**: Available via Spark Web UI or in worker directories

```console
  $ find spark_scratch/workers/ -name stderr -exec ls -lh {} \;
```

### How do I clean up after a failed job?

```console
$ sparkctl stop  # Stop any running Spark processes
$ rm -rf ./conf ./spark_scratch/logs  # Remove generated files
```

### sparkctl hangs during startup

Check if Spark processes from a previous job are still running:

```console
$ ps aux | grep $USER
$ sparkctl stop
```

Also verify your Slurm allocation is still active:

```console
$ squeue --me
```
