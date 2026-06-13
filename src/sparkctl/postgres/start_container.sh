#!/bin/bash
pg_data_dir=$1
pg_run_dir=$2
# The password is passed through the environment so that it does not appear in process listings.
if [ -z "${SPARKCTL_PG_PASSWORD}" ]; then
    echo "SPARKCTL_PG_PASSWORD must be set and non-empty" >&2
    exit 1
fi
pg_password=${SPARKCTL_PG_PASSWORD}

# TODO: Make these configurable.
lustre_bind_mounts=" -B /nopt:/nopt \
    -B /projects:/projects \
    -B /scratch:/scratch \
    -B /datasets:/datasets \
    -B /kfs2:/kfs2 \
    -B /kfs3:/kfs3"

# TODO: Make docker vs apptainer configurable.

module load apptainer
mkdir -p ${pg_data_dir} ${pg_run_dir}
apptainer instance start \
  --env POSTGRES_PASSWORD=${pg_password} \
  ${lustre_bind_mounts} \
  -B ${pg_data_dir}:/var/lib/postgresql/data \
  -B ${pg_run_dir}:/var/run/postgresql \
  docker://postgres:17 \
  pg-server
