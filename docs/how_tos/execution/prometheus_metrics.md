# How to expose Spark metrics in Prometheus format

sparkctl can configure Spark's built-in `PrometheusServlet` so that Spark's internal metrics
(JVM, scheduler, shuffle, executor, and task metrics) are exposed in Prometheus format. This
complements [resource monitoring](resource_monitoring.md), which captures host-level CPU, memory,
disk, and network utilization.

The servlet reuses the existing web UI ports, so no additional ports are opened.

## Enable Prometheus metrics

```console
$ sparkctl configure --prometheus
```

This writes a `metrics.properties` file into the cluster's `conf` directory (Spark loads it
automatically) and sets `spark.ui.prometheus.enabled true`.

## Scrape endpoints

| Component | Endpoint |
| --------- | -------- |
| Master | `http://<master>:8080/metrics/master/prometheus` |
| Worker | `http://<worker>:8081/metrics/prometheus` |
| Driver / application | `http://<driver>:4040/metrics/executors/prometheus` |

Point a Prometheus scraper at these endpoints, or fetch them directly with `curl` for a quick look:

```console
$ curl http://localhost:4040/metrics/executors/prometheus
```

```{eval-rst}
.. tip:: Combine this with the :doc:`reverse proxy <../configuration/web_ui_reverse_proxy>` to
   reach the worker and application endpoints through the master node on an HPC cluster.
```
