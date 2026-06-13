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

## Connect to the server

When the server starts, sparkctl logs the node it is running on, a ready-to-use SSH tunnel
command, and the access URL (with token). It looks like:

```
Jupyter is running on x1000c0s0b0n0 (port 8889). From your laptop, open an SSH tunnel:
    ssh -L 8889:x1000c0s0b0n0:8889 <your-hpc-login-host>
then browse to:
    http://localhost:8889/tree?token=<token>
```

Run the `ssh` command from your laptop (replacing `<your-hpc-login-host>` with your cluster's login
host), then open the `http://localhost:8889/...` URL in your browser. The same information is always
available in `jupyter.log` in the cluster base directory. Change the port with `--jupyter-port`.

Jupyter listens on all interfaces (`0.0.0.0`) by default so it is reachable by tunneling to the
compute node's hostname through a login node, which is the portable HPC pattern. Access is protected
by Jupyter's token.

```{eval-rst}
.. note:: To bind to localhost only, pass ``--jupyter-ip 127.0.0.1``. The server is then off the
   cluster network, but you must tunnel directly into the compute node, e.g.
   ``ssh -J <hpc-login-host> -L 8889:localhost:8889 <node>``.
```

## Reducing log noise

If `jupyter.log` is noisy, note that the most verbose tracebacks come from optional integrations,
not from sparkctl: language-server probing is disabled automatically, but a `jupyterlab` package
installed in your environment runs a build check at startup that can log a Node/yarn error. It is
harmless. Installing only the classic notebook (`pip install "sparkctl[jupyter]"`, without
`jupyterlab`) avoids it.

## Stopping

`sparkctl stop` shuts the Jupyter server down along with the rest of the cluster.

```{eval-rst}
.. note:: If you enable ``--jupyter`` without ``--connect-server``, Jupyter still starts, but the
   notebook is responsible for creating its own ``SparkSession`` (for example, a local driver that
   connects to ``spark://<master>:7077``). The Connect server path is recommended.
```
