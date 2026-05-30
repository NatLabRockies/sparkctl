# sparkctl environment module

This directory contains a ready-to-deploy [environment module](https://lmod.readthedocs.io/)
for `sparkctl`. It lets HPC users run Spark without installing Spark, Java, or
the `sparkctl` package themselves:

```console
$ module load sparkctl
$ sparkctl configure --start
$ spark-submit --master spark://$(hostname):7077 my-job.py
$ sparkctl stop
```

The module assumes a **shared deployment** that you (the administrator) set up
once: a Python virtual environment with `sparkctl` installed, a set of
pre-downloaded binaries, and a settings file that points at them.

## Files

| File            | Purpose                                                         |
| --------------- | -------------------------------------------------------------- |
| `sparkctl.tcl`  | TCL modulefile. Works on **both** Lmod and Environment Modules. |
| `sparkctl.lua`  | Lua modulefile, for Lmod-only sites. Use this *or* the TCL one. |
| `sparkctl.toml` | Example shared settings file consumed by `sparkctl`.           |

## What the module sets

When loaded, the module:

- Prepends the virtual environment's `bin/` to `PATH` (provides `sparkctl` and
  `python`) and sets `VIRTUAL_ENV`.
- Sets `SPARKCTL_SETTINGS_FILE` to the shared settings file, so `sparkctl`
  resolves the Spark/Java/etc. binary paths automatically.
- Reads `spark_path`/`java_path`/`hadoop_path` from that settings file and uses
  them to set `JAVA_HOME` and `SPARK_HOME` (and optionally `HADOOP_HOME`), and
  puts their `bin/` directories on `PATH` so interactive `spark-submit`,
  `pyspark`, and `java` work too.

The settings file (`sparkctl.toml`) is the **single source of truth** for the
binary locations. The modulefile parses it at load time, so you set those paths
in exactly one place. The TCL flavor parses the file natively; the Lua flavor
uses `awk` (always available on HPC) since Lmod's sandbox does not reliably
expose Lua file I/O.

## Deployment steps

### 1. Download the binaries

Pick a shared location all users can read, e.g. `/datasets/images/spark`, and
extract the binaries there (Spark, a JDK 17+, and optionally Hadoop/Hive). See
`docs/how_tos/getting_started/deploy_sparkctl.md` for download links.

### 2. Create the Python virtual environment

Create a venv (or conda env) on the shared filesystem and install `sparkctl`:

```console
$ python -m venv /datasets/images/spark/venv
$ /datasets/images/spark/venv/bin/pip install "sparkctl[pyspark]"
```

`conda`/`mamba` environments work equally well; just point the modulefile at the
env directory whose `bin/sparkctl` exists. Use the `[pyspark]` extra if users
will run `spark-submit`/`pyspark` (the base install does not include them).

### 3. Create the shared settings file

Copy `sparkctl.toml` from this directory to the shared location and edit the
paths to match the binaries you downloaded:

```console
$ cp sparkctl.toml /datasets/images/spark/sparkctl.toml
$ $EDITOR /datasets/images/spark/sparkctl.toml
```

Only the `[binaries]` section is required.

### 4. Install the modulefile

Choose **one** flavor and copy it into your modulefiles tree under a versioned
name. For Lmod:

```console
$ mkdir -p <MODULEPATH>/sparkctl
$ cp sparkctl.lua <MODULEPATH>/sparkctl/0.4.1.lua
```

For Environment Modules (Tmod), or for a portable install that works with both:

```console
$ mkdir -p <MODULEPATH>/sparkctl
$ cp sparkctl.tcl <MODULEPATH>/sparkctl/0.4.1
```

Find your `MODULEPATH` with `module use` / `echo $MODULEPATH`, or ask your
site's module maintainer.

### 5. Edit the site paths in the modulefile

Open the installed modulefile and set the three variables in its **SITE
CONFIGURATION** block:

- `sparkctl_root` — the shared deployment root.
- `sparkctl_venv` — the virtual environment directory (its `bin/` has `sparkctl`).
- `sparkctl_settings` — the shared `sparkctl.toml` from step 3.

The Spark/Java/Hadoop install directories are **not** set here — the modulefile
reads them from `sparkctl.toml`, so they are defined in exactly one place.

### 6. Verify

```console
$ module load sparkctl
$ which sparkctl
$ sparkctl --version
$ echo $SPARKCTL_SETTINGS_FILE
```

## How binary resolution works

`sparkctl` loads settings (via Dynaconf) from, in increasing order of precedence:

1. `~/.sparkctl.toml`
2. the file named by `SPARKCTL_SETTINGS_FILE` (what this module sets)
3. `./.sparkctl.toml` in the current working directory

So a user can override any site-wide default by dropping a `.sparkctl.toml` in
their working directory, without touching the shared deployment.

## Versioning

Name the modulefile after the deployed `sparkctl` version (e.g. `0.4.1`). To
roll out a new version, create a new venv + settings file and add a new
modulefile alongside the old one so users can pin a version.
