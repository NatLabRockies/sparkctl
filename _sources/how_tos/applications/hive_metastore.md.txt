(hive-metastore)=
# How to configure a Hive metastore
Spark supports reading and writing data from [Apache Hive](https://hive.apache.org) as described
[here](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).

This is useful if you want to access data in SQL tables through a JDBC/ODBC client
instead of Parquet files through Python/R or `spark-sql`. It also provides access to Spark with
[SQLAlchemy](https://www.sqlalchemy.org) through
[PyHive](https://kyuubi.readthedocs.io/en/v1.8.0/client/python/pyhive.html).

You can configure Spark with a Hive Metastore by running this command:
```console
$ sparkctl configure --thrift-server --hive-metastore --metastore-dir /path/to/my/metastore
```

By default Spark uses [Apache Derby](https://db.apache.org/derby/) as the database for the metastore.
This has a limitation: only one client can be connected to the metastore at a time.

If you need multiple simultaneous connections to the metastore, you can use
[PostgreSQL](https://www.postgresql.org) as the backend instead by running the following command:
```
$ sparkctl configure --thrift-server --hive-metastore --postgres-hive-metastore --metastore-dir /path/to/my/metastore
```
This takes a few extra minutes to start the first time, as it has to download a container and
start the server. Apptainer will cache the container image and you can reuse the database data
across Slurm allocations.

**Note**: The metadata about your tables will be stored in Derby or Postgres. Your tables will
be stored on the filesystem (Parquet files by default) in a directory called `spark-warehouse`,
which gets created in the directory passed to `--metastore-dir` (current directory by default).
Postgres data, if enabled, will be in the same directory (`pg_data`).
