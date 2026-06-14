# How to use a custom spark-defaults.conf file
sparkctl sets custom settings in the `spark-defaults.conf` file. For example, it sets

```
spark.sql.parquet.outputTimestampType TIMESTAMP_MICROS
```

so that Spark writes Parquet files with timestamps that can be properly interpreted by other
libraries like DuckDB and Pandas.

If you don't want these settings or want to add your own settings every time you configure
`sparkctl`, here is how to do it.

1. Create a file named `spark-defaults.conf.template`. Spark's defaults are stored
   [here](https://github.com/apache/spark/blob/master/conf/spark-defaults.conf.template).
   
2. Add your custom settings to the file.

3. Pass the file to `sparkctl configure` with the `--spark-defaults-template-file` option:

   ```console
   $ sparkctl configure --spark-defaults-template-file spark-defaults.conf.template
   ```
   
```{eval-rst}
.. note:: sparkctl will still append Spark driver, executor, and other settings to the runtime
   version of the spark-defaults.conf file.
```
