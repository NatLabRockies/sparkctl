# NLR Kestrel
This package was originally developed using NLR's Kestrel HPC, which uses Slurm to manage resources.

This page contains information specific to Kestrel that users must consider.

## Spark software
Spark is installed on Kestrel. Use these locations when your configure your environment.
```
/datasets/images/apache_spark
├── apache-hive-4.2.0-bin.tar.gz
├── hadoop-3.4.1/
├── jdk-21.0.7/
├── postgresql-42.7.4.jar
├── rapids-4-spark_2.13-26.04.2.jar
├── spark-4.1.1-bin-hadoop3/
```

```console
$ sparkctl default-config \
    /datasets/images/apache_spark/spark-4.1.1-bin-hadoop3 \
    /datasets/images/apache_spark/jdk-21.0.7 \
    --hadoop-path /datasets/images/apache_spark/hadoop-3.4.1 \
    --hive-tarball /datasets/images/apache_spark/apache-hive-4.2.0-bin.tar.gz \
    --rapids-jar-file /datasets/images/apache_spark/rapids-4-spark_2.13-26.04.2.jar \
    --postgresql-jar-file /datasets/images/apache_spark/postgresql-42.7.4.jar \
    --compute-environment slurm
```

## Compute nodes

### Low-bandwith compute nodes
Users must use only low-bandwidth compute nodes. The "high-bandwidth" nodes are very problematic for
Spark. They contain two network interface cards (NICs) which have different IP addresses. An
application addressing a hostname may get routed to either NIC. However, Spark will only be
listening on one address. The result is that many intra-cluster network messages will be dropped,
causing the cluster to fail.

Solution: Specify the constraint `lbw` (for low bandwidth) whenever you request nodes with salloc/
srun/sbatch.

Example:
```console
$ salloc --account <my-account> -t 01:00:00 --constraint lbw
```

### Heterogeneous Slurm jobs
Kestrel offers a shared partition where you can request a subset of a compute node's CPUs and
memory. Prefer this partition for your Spark master process and user application, as discussed at
{ref}`heterogeneous-slurm-jobs`.

### Local storage for Spark shuffle writes
A small portion of Kestrel compute nodes have local storage (~256). The drives are ~1.6 TB. Prefer
these nodes for Spark workers if your jobs will shuffle lots of data. The local drives are much
faster than Lustre and you'll get better results.

```console
$ salloc --account <my-account> -t 01:00:00 --partition nvme
```

Enable local storage for shuffle writes with
```console
$ sparkctl configure --local-storage
```

Lustre will likely be sufficient for most jobs. If you will shuffle a large amount of data (TBs)
and are unsure about how many nodes you'll need to have enough room, start with Lustre (which will
happen by default).

### Lustre file striping
If you use Lustre for shuffle writes, consider enabling custom stripe settings to optimize
performance. Run this command in your runtime directory **before** you write any data to it.

```console
$ lfs setstripe -E 256K -L mdt -E 8M -c -1 -S 1M -E -1 -c -1 -p flash /scratch/$USER/work-dir
```
