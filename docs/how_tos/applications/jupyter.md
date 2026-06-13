# How to run a JupyterLab notebook against the cluster

sparkctl can start a JupyterLab server on the master node so you can run interactive notebooks
against the Spark cluster. When the Spark Connect server is enabled, the notebook's `SparkSession`
connects to the cluster automatically.

## Prerequisites

Install JupyterLab in the same environment as sparkctl:

```console
$ pip install jupyterlab    # or: uv pip install jupyterlab
```

## Start JupyterLab with the Connect server

The recommended setup enables the Spark Connect server so the notebook connects remotely without
any extra configuration:

```console
$ sparkctl configure --connect-server --jupyter --start
```

sparkctl sets `SPARK_REMOTE` for the JupyterLab process, so inside a notebook you can simply do:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"]).show()
```

## Find the access URL

The JupyterLab access URL, including its login token, is written to `jupyter.log` in the cluster
base directory:

```console
$ grep -m1 'http' jupyter.log
```

JupyterLab listens on port 8889 by default. Change it with `--jupyter-port`, and forward it to your
laptop over SSH (replacing `master-node` with the node running the server):

```console
$ ssh -L 8889:master-node:8889 <hpc-login-host>
```

Then open the URL from `jupyter.log`, replacing the host with `localhost`.

## Stopping

`sparkctl stop` shuts the JupyterLab server down along with the rest of the cluster.

```{eval-rst}
.. note:: If you enable ``--jupyter`` without ``--connect-server``, JupyterLab still starts, but
   the notebook is responsible for creating its own ``SparkSession`` (for example, a local driver
   that connects to ``spark://<master>:7077``). The Connect server path is recommended.
```
