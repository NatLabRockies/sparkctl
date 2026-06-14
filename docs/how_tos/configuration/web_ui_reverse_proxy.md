# How to access the Spark web UIs through a reverse proxy

On an HPC cluster the compute nodes that run the Spark workers and your application driver are
usually not directly reachable from your laptop. That makes the worker and application web UIs,
whose links point at those nodes, hard to open.

Enabling the reverse proxy tells the Spark master to proxy the worker and application UIs through
itself, so you only need to reach the master node (for example, through a single SSH tunnel).

## Enable the reverse proxy

```console
$ sparkctl configure --reverse-proxy
```

This sets `spark.ui.reverseProxy true`. Open the master web UI (default port 8080) and follow the
links to the worker and application UIs; they are served through the master.

## Reach the master from your laptop

Forward the master web UI port over SSH, replacing `master-node` with the node running the master
(typically your submission node):

```console
$ ssh -L 8080:master-node:8080 <hpc-login-host>
```

Then browse to `http://localhost:8080`.

## When the master is itself behind another proxy

If you put your own front-end proxy in front of the master, give Spark the externally visible URL
so it can rewrite links correctly:

```console
$ sparkctl configure --reverse-proxy --reverse-proxy-url https://my-proxy.example.com/spark
```

```{eval-rst}
.. note:: Leave ``--reverse-proxy-url`` unset when you reach the master directly through an SSH
   tunnel. Spark then serves relative links, which work through the tunnel without extra
   configuration.
```
