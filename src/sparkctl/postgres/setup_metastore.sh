#!/bin/bash
pg_exists=$1
# The password is passed through the environment so that it does not appear in process listings.
if [ -z "${SPARKCTL_PG_PASSWORD}" ]; then
    echo "SPARKCTL_PG_PASSWORD must be set and non-empty" >&2
    exit 1
fi
pg_password=${SPARKCTL_PG_PASSWORD}
module load apptainer

if [ "${pg_exists}" != "true" ]; then
    apptainer exec instance://pg-server initdb
fi
set -e
apptainer exec instance://pg-server \
    pg_ctl \
        -D /var/lib/postgresql/data \
        -l pg_logfile \
        start
if [ "${pg_exists}" != "true" ]; then
    apptainer exec instance://pg-server createdb hive_metastore
    apptainer exec instance://pg-server \
        psql \
            -c "CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD '${pg_password}'" \
            hive_metastore
fi
