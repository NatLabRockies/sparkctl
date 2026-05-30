-- sparkctl environment module (Lmod modulefile)
--
-- For sites running Lmod. A portable TCL version is provided alongside this
-- file as sparkctl.tcl; use whichever fits your stack. To deploy, copy this
-- file into your modulefiles tree, for example:
--
--     <MODULEPATH>/sparkctl/0.4.1.lua
--
-- Then edit the three site-specific paths in the "SITE CONFIGURATION" block
-- below. The Spark/Java/Hadoop install directories are NOT configured here --
-- they are read from the shared sparkctl settings file (sparkctl.toml) so that
-- there is a single source of truth.
--
-- A user then runs:
--
--     module load sparkctl
--     sparkctl configure --start
--
-- See README.md in this directory for full deployment instructions.

-- ---------------------------------------------------------------------------
-- SITE CONFIGURATION -- edit these values for your deployment.
-- ---------------------------------------------------------------------------

-- Root of the shared deployment that holds the venv, binaries, and settings.
local sparkctl_root = "/datasets/images/spark"

-- Pre-installed Python virtual environment containing the sparkctl package.
-- Works for both `python -m venv`/`uv venv` and conda envs -- point it at the
-- directory whose `bin/` subdirectory contains the `sparkctl` executable.
local sparkctl_venv = pathJoin(sparkctl_root, "venv")

-- Shared sparkctl settings file (TOML). This is the single source of truth for
-- the Spark/Java/Hadoop binary paths -- they are parsed from its [binaries]
-- section below. See sparkctl.toml.
local sparkctl_settings = pathJoin(sparkctl_root, "sparkctl.toml")

-- ---------------------------------------------------------------------------
-- Helpers and binary resolution (no edits needed below this line).
-- ---------------------------------------------------------------------------

-- Parse the [binaries] section of the sparkctl TOML settings file into a table
-- of key -> value. Lmod's sandbox does not reliably expose io.open, so we shell
-- out to awk (universally available on HPC) via Lmod's capture(). Only simple
-- `key = "value"` lines are supported, which is all sparkctl writes.
local function read_binaries(path)
    local awk_prog = [==[
/^[ \t]*#/ { next }
/^[ \t]*\[/ { inb = ($0 ~ /\[binaries\]/); next }
inb && /=/ {
    key = $0; sub(/[ \t]*=.*/, "", key); gsub(/[ \t]/, "", key)
    val = $0; sub(/^[^=]*=[ \t]*/, "", val); gsub(/"/, "", val)
    sub(/[ \t]*#.*/, "", val); sub(/[ \t]+$/, "", val)
    if (key != "") print key "=" val
}
]==]
    local out = capture("awk '" .. awk_prog .. "' '" .. path .. "'")
    local binaries = {}
    for line in out:gmatch("[^\n]+") do
        local k, v = line:match("^([%w_]+)=(.*)$")
        if k then
            binaries[k] = v
        end
    end
    return binaries
end

whatis("Orchestrate standalone Apache Spark clusters on HPC with sparkctl.")

help([[
sparkctl configures and orchestrates standalone Apache Spark clusters,
e.g. inside a Slurm allocation. After loading:

    sparkctl configure --start
    spark-submit --master spark://$(hostname):7077 my-job.py
    sparkctl stop
]])

-- Fail clearly if the deployment is incomplete rather than half-loading.
if not isDir(sparkctl_venv) then
    LmodError("sparkctl: venv not found at " .. sparkctl_venv)
end
if not isFile(sparkctl_settings) then
    LmodError("sparkctl: settings file not found at " .. sparkctl_settings)
end

-- Derive the binary locations from the settings file (single source of truth).
local binaries = read_binaries(sparkctl_settings)
local spark_home = binaries["spark_path"]
local java_home = binaries["java_path"]
local hadoop_home = binaries["hadoop_path"]
if not spark_home or not java_home then
    LmodError("sparkctl: spark_path and java_path must be set in " .. sparkctl_settings)
end

-- Python virtual environment (gives `sparkctl` and `python`).
prepend_path("PATH", pathJoin(sparkctl_venv, "bin"))
setenv("VIRTUAL_ENV", sparkctl_venv)

-- Tell sparkctl where to find the shared binary paths.
setenv("SPARKCTL_SETTINGS_FILE", sparkctl_settings)

-- Java (for sparkctl's managed processes and the user's interactive commands).
setenv("JAVA_HOME", java_home)
prepend_path("PATH", pathJoin(java_home, "bin"))

-- Spark binaries (so `spark-submit` and `pyspark` work in the user's shell).
setenv("SPARK_HOME", spark_home)
prepend_path("PATH", pathJoin(spark_home, "bin"))

-- Optional Hadoop.
if hadoop_home ~= nil and hadoop_home ~= "" then
    setenv("HADOOP_HOME", hadoop_home)
    prepend_path("PATH", pathJoin(hadoop_home, "bin"))
end
