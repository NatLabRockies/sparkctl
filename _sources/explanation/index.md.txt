(explanation)=
# Explanation

This section provides conceptual background on sparkctl's design, architecture, and the reasoning behind key decisions.

## Why sparkctl Exists

High-Performance Computing (HPC) environments present unique challenges for running Apache Spark:

1. **No cloud infrastructure**: HPCs lack the managed services (EMR, Dataproc, HDInsight) that handle Spark cluster lifecycle in cloud environments.
2. **No administrative access**: Users typically cannot install system-wide services or configure YARN/Kubernetes clusters.
3. **Ephemeral allocations**: Compute nodes are allocated temporarily through job schedulers like Slurm, not provisioned permanently.

sparkctl bridges this gap by automating Spark cluster configuration and lifecycle management within the constraints of HPC environments.

## Architecture Overview

sparkctl manages a complete Spark standalone cluster within your Slurm allocation:

```{mermaid}
flowchart TB
    subgraph allocation["Your Slurm Allocation"]
        subgraph master["Slurm Head Node"]
            spark_master["Spark Master"]
            driver["Your Driver Application"]
        end
        subgraph workers["Worker Nodes"]
            subgraph worker1["Worker Node 1"]
                sw1["Spark Worker"]
                exec1["Executors"]
            end
            subgraph worker2["Worker Node 2"]
                sw2["Spark Worker"]
                exec2["Executors"]
            end
        end
        driver --> spark_master
        spark_master --> sw1
        spark_master --> sw2
        sw1 --> exec1
        sw2 --> exec2
    end
```

**Key components**:
- **Spark Master**: Coordinates the cluster, tracks available workers
- **Spark Workers**: Run on each compute node, manage local resources
- **Executors**: JVM processes that run your tasks, spawned by workers
- **Driver**: Your application, which submits tasks to the cluster

## Why Standalone Mode?

Spark supports three cluster managers: Standalone, YARN, and Kubernetes. sparkctl uses **Standalone mode** exclusively because:

| Factor | Standalone | YARN/Kubernetes |
|--------|------------|-----------------|
| Setup complexity | Minimal | Requires cluster-wide services |
| Admin access needed | No | Yes |
| Ephemeral clusters | Natural fit | Designed for persistent clusters |
| Resource isolation | Per-allocation | Cluster-wide scheduling |

For ephemeral, user-controlled clusters on HPCs, Standalone mode is the pragmatic choice. YARN and Kubernetes add complexity without benefit when your cluster exists only for the duration of a single Slurm job.

## Resource Calculation

sparkctl automatically calculates optimal Spark settings based on your allocation:

1. **Executors per node**: Based on available CPU cores and memory, leaving headroom for the worker process
2. **Memory per executor**: Based on available memory
3. **Shuffle partitions**: Based on total executor cores across the cluster

These calculations follow Spark best practices for memory management and parallelism. You can
override any setting via `spark-defaults.conf` if needed.

## The Cluster Lifecycle

sparkctl manages four phases:

1. **Configure** (`sparkctl configure`): Analyzes your Slurm allocation, generates Spark configuration files in `./conf/`
2. **Start** (`sparkctl start`): Launches Spark Master on the head node, Workers on all nodes
3. **Run**: Your applications connect to the cluster and execute
4. **Stop** (`sparkctl stop`): Gracefully shuts down all Spark processes

This explicit lifecycle gives you full control and visibility, unlike managed services that hide these details.

(configuration-files)=
## Configuration Files

sparkctl uses two configuration mechanisms:

### User Configuration (`~/.sparkctl.toml`)

This TOML file stores environment-specific settings that rarely change:

```toml
[binaries]
spark_path = "/path/to/spark"
java_path = "/path/to/java"
```

These settings tell sparkctl where to find Spark and Java. You can also set global settings that
apply every time you run `sparkctl configure`. For example, if you always want to use Spark Connect,
you can set `start_connect_server = true` in a `[runtime]` section and avoid having to set it each
time you configure:

```toml
[runtime]
start_connect_server = true
```

### Runtime Configuration (`./conf/`)

Each time you run `sparkctl configure`, it generates a `./conf/` directory containing:

- `spark-defaults.conf`: Spark settings (executors, memory, shuffle partitions)
- `spark-env.sh`: Environment variables for Spark processes
- `workers`: List of worker hostnames
- `log4j2.properties`: Logging configuration

These files are specific to your current allocation and working directory.

### Custom Spark Settings

To customize Spark settings beyond what sparkctl generates:

1. Create a template file (e.g., `spark-defaults.conf.template`)
2. Add your custom settings
3. Run `sparkctl configure --spark-defaults-template-file spark-defaults.conf.template`

sparkctl will include your settings and append its calculated values.

## Spark Cluster Overview

For general Spark architecture concepts, refer to the official [Spark Cluster Overview](https://spark.apache.org/docs/latest/cluster-overview.html).

## Submitting Applications

Please refer to Spark's [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
documentation for details on `spark-submit` options.

To get all submission tools in a Python environment:
```console
$ pip install pyspark
```

Clients for other languages are available at the main Spark [downloads page](https://spark.apache.org/downloads.html).

## Spark Connect

Spark Connect is a relatively new feature that simplifies client installation and configuration. It
provides a thin client that communicates with the cluster via gRPC, avoiding the need to install
the full Spark distribution.

Benefits:
- Smaller client installation (`pyspark-client` instead of full `pyspark`)
- Better isolation between client and cluster Python environments
- Simpler remote connectivity

Enable the Spark Connect server:
```console
$ sparkctl configure --connect-server
```

Install only the client:
```console
$ pip install pyspark-client
```

Note the [caveats](https://spark.apache.org/docs/latest/spark-connect-overview.html#how-spark-connect-client-applications-differ-from-classic-spark-applications) around Spark Connect - some APIs behave differently.
