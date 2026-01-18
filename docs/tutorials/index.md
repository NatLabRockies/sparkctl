(tutorials)=
# Tutorials

These tutorials guide you through running Spark jobs on HPC clusters using sparkctl. Each tutorial covers a different workflow - choose the one that best fits your needs.

## Which Tutorial Should I Use?

| Tutorial | Best For | Client Install | Interface |
|----------|----------|----------------|-----------|
| [Python Library](run_python_spark_jobs_script.md) | Programmatic control, automation scripts | `sparkctl` | Python |
| [Interactive Development](run_python_spark_jobs_interactively.md) | Exploratory analysis, debugging | `sparkctl` | Python REPL |
| [Ibis](run_ibis_spark_jobs.md) | Ibis users, portable dataframe code | `sparkctl` + `ibis-framework[pyspark]` | Python |
| [Spark Connect CLI](run_python_spark_jobs_spark_connect.md) | Lightweight client, remote connectivity | `sparkctl` | CLI |
| [spark-submit / pyspark](run_spark_jobs.md) | Traditional Spark users, production jobs | `sparkctl[pyspark]` (full) | CLI |

### Decision Guide

**Start here if you're new to sparkctl**: [Python Library](run_python_spark_jobs_script.md) - this uses sparkctl's managed cluster to handle the Spark lifecycle automatically.

**Choose by use case**:

- **"I want to control the cluster from Python code"** → [Python Library](run_python_spark_jobs_script.md)
- **"I want to explore data interactively"** → [Interactive Development](run_python_spark_jobs_interactively.md)
- **"I use Ibis and want portable dataframe code"** → [Ibis](run_ibis_spark_jobs.md)
- **"I want a minimal client installation"** → [Spark Connect CLI](run_python_spark_jobs_spark_connect.md)
- **"I want to submit batch jobs"** → [spark-submit / pyspark](run_spark_jobs.md)

```{eval-rst}
.. toctree::
    :maxdepth: 2
    :caption: Contents:

    run_python_spark_jobs_script
    run_python_spark_jobs_interactively
    run_ibis_spark_jobs
    run_python_spark_jobs_spark_connect
    run_spark_jobs
```
