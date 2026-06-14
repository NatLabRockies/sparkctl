# How to to monitor Spark resource utilization

sparkctl integrates with [rmon](https://github.com/NatLabRockies/resource_monitor) to monitor CPU, memory,
disk, and network utilization of Spark.

This page shows how to enable it.

```console
$ sparkctl configure --resource-monitor
```

By default, this enables monitoring of all resource types at a 5-second interval. Edit the resulting
`config.json` to customize resource types or interval.

Start the Spark cluster as normal.

```console
$ sparkctl start
```

sparkctl will enable resource monitoring with rmon in the background on all compute nodes. Whenever
you
stop the cluster after running your jobs:

```console
$ sparkctl stop
```

sparkctl will stop rmon on all compute nodes, collect a summary of utilization stats, and produce
interactive HTML plots. Those will be in `./stats-output/html` by default.

```{eval-rst}
.. warning:: If you don't run `sparkctl stop`, rmon will keep running on all compute nodes
   indefinitely.
```

## Managed execution
sparkctl offers a managed execution mode to help prevent cases where you forget to shut down Spark
workers and rmon.

Add the `--wait` option to the start command as follows:

```console
$ sparkctl start --wait
```

sparkctl will print `Press Ctrl-C to shut down all Spark processes.` to the console and then go to
sleep without returning control to the shell.

Connect to the head node of the allocation and run your Spark jobs. When you're done, press Ctrl-c
in the original terminal to shut everything down.
