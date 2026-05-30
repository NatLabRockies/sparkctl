#%Module1.0
#
# sparkctl environment module (TCL modulefile)
#
# Portable across both Lmod and Environment Modules (Tmod). To deploy, copy
# this file into your modulefiles tree, for example:
#
#     <MODULEPATH>/sparkctl/0.4.1.tcl    (Lmod, versioned)
#     <MODULEPATH>/sparkctl/0.4.1        (Environment Modules, versioned)
#
# Then edit the three site-specific paths in the "SITE CONFIGURATION" block
# below. The Spark/Java/Hadoop install directories are NOT configured here --
# they are read from the shared sparkctl settings file (sparkctl.toml) so that
# there is a single source of truth.
#
# A user then runs:
#
#     module load sparkctl
#     sparkctl configure --start
#
# See README.md in this directory for full deployment instructions.

# ---------------------------------------------------------------------------
# SITE CONFIGURATION -- edit these values for your deployment.
# ---------------------------------------------------------------------------

# Root of the shared deployment that holds the venv, binaries, and settings.
set sparkctl_root     "/datasets/images/spark"

# Pre-installed Python virtual environment containing the sparkctl package.
# Works for both `python -m venv`/`uv venv` and conda envs -- point it at the
# directory whose `bin/` subdirectory contains the `sparkctl` executable.
set sparkctl_venv     "$sparkctl_root/venv"

# Shared sparkctl settings file (TOML). This is the single source of truth for
# the Spark/Java/Hadoop binary paths -- they are parsed from its [binaries]
# section below. See sparkctl.toml.
set sparkctl_settings "$sparkctl_root/sparkctl.toml"

# ---------------------------------------------------------------------------
# Helpers and binary resolution (no edits needed below this line).
# ---------------------------------------------------------------------------

# Parse the [binaries] section of a sparkctl TOML settings file into a dict of
# key -> value. Only simple `key = "value"` lines are supported, which is all
# sparkctl writes. Comment lines (#) and other sections are ignored.
proc _sparkctl_read_binaries { path } {
    set result [dict create]
    if { ![file exists $path] } {
        return $result
    }
    set fh [open $path r]
    set section ""
    while { [gets $fh line] >= 0 } {
        set line [string trim $line]
        if { $line eq "" || [string index $line 0] eq "#" } {
            continue
        }
        if { [string index $line 0] eq "\[" } {
            set section [string trim $line "\[\]"]
            continue
        }
        if { $section ne "binaries" } {
            continue
        }
        set eq [string first "=" $line]
        if { $eq < 0 } {
            continue
        }
        set key [string trim [string range $line 0 [expr {$eq - 1}]]]
        set val [string trim [string range $line [expr {$eq + 1}] end]]
        if { [string index $val 0] eq "\"" } {
            # Quoted value: take the text between the first pair of quotes.
            set close [string first "\"" $val 1]
            if { $close > 0 } {
                set val [string range $val 1 [expr {$close - 1}]]
            }
        } else {
            # Bare value: drop any trailing inline comment, then stray quotes.
            set hash [string first "#" $val]
            if { $hash >= 0 } {
                set val [string trim [string range $val 0 [expr {$hash - 1}]]]
            }
            set val [string trim $val "\""]
        }
        dict set result $key $val
    }
    close $fh
    return $result
}

module-whatis "Orchestrate standalone Apache Spark clusters on HPC with sparkctl."

proc ModulesHelp { } {
    puts stderr "sparkctl configures and orchestrates standalone Apache Spark"
    puts stderr "clusters, e.g. inside a Slurm allocation. After loading:"
    puts stderr ""
    puts stderr "    sparkctl configure --start"
    puts stderr "    spark-submit --master spark://\$(hostname):7077 my-job.py"
    puts stderr "    sparkctl stop"
}

# Fail clearly if the deployment is incomplete rather than half-loading.
if { ![file isdirectory $sparkctl_venv] } {
    puts stderr "sparkctl: venv not found at $sparkctl_venv"
    break
}
if { ![file exists $sparkctl_settings] } {
    puts stderr "sparkctl: settings file not found at $sparkctl_settings"
    break
}

# Derive the binary locations from the settings file (single source of truth).
set binaries [_sparkctl_read_binaries $sparkctl_settings]
if { ![dict exists $binaries spark_path] || ![dict exists $binaries java_path] } {
    puts stderr "sparkctl: spark_path and java_path must be set in $sparkctl_settings"
    break
}
set spark_home [dict get $binaries spark_path]
set java_home  [dict get $binaries java_path]
set hadoop_home ""
if { [dict exists $binaries hadoop_path] } {
    set hadoop_home [dict get $binaries hadoop_path]
}

# Python virtual environment (gives `sparkctl` and `python`).
prepend-path PATH "$sparkctl_venv/bin"
setenv VIRTUAL_ENV "$sparkctl_venv"

# Tell sparkctl where to find the shared binary paths.
setenv SPARKCTL_SETTINGS_FILE "$sparkctl_settings"

# Java (for sparkctl's managed processes and the user's interactive commands).
setenv JAVA_HOME "$java_home"
prepend-path PATH "$java_home/bin"

# Spark binaries (so `spark-submit` and `pyspark` work in the user's shell).
setenv SPARK_HOME "$spark_home"
prepend-path PATH "$spark_home/bin"

# Optional Hadoop.
if { [string length $hadoop_home] > 0 } {
    setenv HADOOP_HOME "$hadoop_home"
    prepend-path PATH "$hadoop_home/bin"
}
