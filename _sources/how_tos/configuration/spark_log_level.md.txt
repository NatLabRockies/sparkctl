# How to Set a Custom Spark Log Level

By default, Spark logs messages at the `INFO` level. This can be very verbose when running jobs through `spark-submit`, flooding your console with messages about task scheduling, shuffle operations, and internal Spark events.

## Reducing Log Verbosity

To reduce the verbosity, specify a higher log level when configuring:

```console
$ sparkctl configure --spark-log-level warn
```

This sets the root logger to `WARN`, so you'll only see warnings and errors.

## Available Log Levels

The `--spark-log-level` option accepts these values:

| Level | Description | Use Case |
|-------|-------------|----------|
| `debug` | Detailed debugging | Troubleshooting specific issues |
| `info` | General information (default) | Development, understanding job progress |
| `warn` | Warnings only | Production jobs, cleaner output |
| `error` | Errors only | When you only care about failures |

```{eval-rst}
.. note:: Log4j 2 also defines ``trace``, ``fatal``, and ``off`` levels, but ``--spark-log-level``
   does not accept them. To use one of those, edit ``log4j2.properties`` in ``./conf/`` after
   running ``sparkctl configure``, as described in the Per-Logger Configuration section below.
```

## Recommendations

- **Interactive development**: Use `info` (default) to see job progress
- **Batch jobs**: Use `warn` to reduce noise while catching problems
- **Debugging failures**: Use `debug` to see detailed execution flow

## Example Output Comparison

With `INFO` (default), you'll see messages like:
```
INFO SparkContext: Running Spark version 4.1.1
INFO ResourceUtils: Resources for spark.driver: ...
INFO SparkContext: Submitted application: My Job
INFO Executor: Starting executor ID driver on host ...
INFO ShuffleBlockFetcherIterator: Getting 100 blocks ...
```

With `WARN`, only warnings and errors appear:
```
WARN TaskSetManager: Lost task 0.0 in stage 1.0 (executor 3): ...
```

## Per-Logger Configuration

For finer control, you can edit the `log4j2.properties` file in `./conf/` after running `sparkctl configure`. This allows you to set different levels for different Spark components:

```properties
# Reduce shuffle noise specifically
logger.shuffle.name = org.apache.spark.shuffle
logger.shuffle.level = warn

# Keep executor logs at info
logger.executor.name = org.apache.spark.executor
logger.executor.level = info
```
