# How to run a Jupyter notebook against the cluster

sparkctl can start a Jupyter server on the master node so you can run interactive notebooks
against the Spark cluster. When the Spark Connect server is enabled, the notebook's `SparkSession`
connects to the cluster automatically.

By default sparkctl launches the classic notebook (`jupyter notebook`), which is a good fit for a
single-user cluster. Use `--jupyter-command lab` if you prefer JupyterLab.

## Prerequisites

Install Jupyter in the same environment as sparkctl. The `jupyter` extra pulls in the classic
notebook frontend:

```console
$ pip install "sparkctl[jupyter]"    # or: uv pip install "sparkctl[jupyter]"
```

If you want JupyterLab instead, install `jupyterlab` and pass `--jupyter-command lab`.

## Start Jupyter with the Connect server

The recommended setup enables the Spark Connect server so the notebook connects remotely without
any extra configuration:

```console
$ sparkctl configure --connect-server --jupyter --start
```

sparkctl sets `SPARK_REMOTE` for the Jupyter process, so inside a notebook you can simply do:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"]).show()
```

## Find the access URL

The Jupyter access URL, including its login token, is written to `jupyter.log` in the cluster base
directory:

```console
$ grep -m1 'http' jupyter.log
```

Jupyter listens on `127.0.0.1:8889` by default. Binding to localhost keeps the server off the
cluster network; reach it with an SSH tunnel. From your laptop, forward the port to the master node
(use `-J <hpc-login-host>` to hop through the login node, and replace `master-node` with the node
running the server):

```console
$ ssh -J <hpc-login-host> -L 8889:localhost:8889 master-node
```

Then open the URL from `jupyter.log`, replacing the host with `localhost`. Change the port with
`--jupyter-port`.

```{eval-rst}
.. note:: If you cannot reach the master node directly, ``--jupyter-ip 0.0.0.0`` makes Jupyter
   listen on all interfaces so you can tunnel through the login node to the master's hostname. This
   exposes the server to the cluster network (it is still protected by Jupyter's access token), so
   prefer the localhost default when possible.
```

## Stopping

`sparkctl stop` shuts the Jupyter server down along with the rest of the cluster.

```{eval-rst}
.. note:: If you enable ``--jupyter`` without ``--connect-server``, Jupyter still starts, but the
   notebook is responsible for creating its own ``SparkSession`` (for example, a local driver that
   connects to ``spark://<master>:7077``). The Connect server path is recommended.
```
