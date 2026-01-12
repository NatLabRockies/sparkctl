(mcp-server)=
# MCP Server for Log Analysis

sparkctl includes an MCP (Model Context Protocol) server that provides AI-assisted diagnosis
of Spark job failures. The server analyzes logs from master, worker, executor, thrift-server,
and connect-server components to detect error patterns and suggest recovery actions.

## Installation

The MCP server requires the optional `mcp` dependency:

```console
$ pip install 'sparkctl[mcp]'
```

## Running the Server

Start the MCP server:

```console
$ sparkctl-mcp-server
```

The server communicates over stdio using the MCP protocol. It is designed to be used with
AI assistants like Claude that support MCP.

## Available Tools

The MCP server provides four tools:

### get_spark_logs

Retrieve and aggregate Spark logs from the cluster.

**Parameters:**
- `spark_scratch` (required): Path to the spark_scratch directory
- `log_type`: One of "master", "worker", "executor", "connect", "thrift", or "all" (default: "all")
- `app_id`: Filter executor logs by application ID
- `executor_id`: Filter by specific executor ID
- `tail_lines`: Number of lines from end of each log (default: 500)

**Example use case:** "Show me the last 100 lines of executor logs for app-20240115120000-0000"

### analyze_spark_failure

Analyze logs for error patterns and provide diagnosis. This is the primary diagnostic tool.

**Parameters:**
- `spark_scratch` (required): Path to the spark_scratch directory
- `app_id`: Specific application to analyze
- `include_stack_traces`: Include full stack traces (default: true)
- `max_errors`: Maximum errors to return (default: 50)

**Detected error patterns:**
- Out of memory (OOM)
- Shuffle failures (FetchFailedException)
- Stage and task failures
- Resource exhaustion
- Connection/network issues
- Disk space issues
- Serialization errors
- Timeout errors

**Example use case:** "Analyze why my Spark job failed"

### get_recovery_suggestions

Get prioritized recovery suggestions based on detected errors.

**Parameters:**
- `error_types` (required): List of error types from analyze_spark_failure
- `current_config`: Current Spark configuration (optional)

**Example use case:** "How do I fix the OOM errors you found?"

### list_spark_applications

List Spark applications found in spark_scratch.

**Parameters:**
- `spark_scratch` (required): Path to the spark_scratch directory

**Example use case:** "What applications have run in this cluster?"

## Integration with torc

The sparkctl MCP server is designed to work alongside [torc](https://github.com/NREL/torc)'s
`analyze_workflow_logs` tool for full-stack diagnostics:

| Layer | Tool | Diagnostics |
|-------|------|-------------|
| Application | sparkctl MCP | Spark-specific: OOM, shuffle, stage failures, serialization |
| Infrastructure | torc MCP | System-level: Slurm errors, node failures, filesystem issues |

When sparkctl detects system-level issues (Slurm cancellation, filesystem errors, node health
problems), it will recommend using torc's analyze_workflow_logs tool for further investigation.

## Example Workflow

1. A Spark job fails on your HPC cluster
2. Ask your AI assistant: "Analyze my failed Spark job in ./spark_scratch"
3. The assistant uses `analyze_spark_failure` to detect error patterns
4. It identifies OOM errors in executors and shuffle failures
5. The assistant uses `get_recovery_suggestions` to get fixes
6. You apply the suggested configuration changes and rerun

## Direct Python Usage

The MCP tools can also be used directly in Python without the MCP server:

```python
from sparkctl.mcp_server import (
    analyze_spark_failure,
    get_recovery_suggestions,
    get_spark_logs,
    list_spark_applications,
)

# Analyze failures
analysis = analyze_spark_failure("./spark_scratch")
print(f"Root cause: {analysis.likely_root_cause}")
print(f"Errors: {analysis.error_summary}")

# Get recovery suggestions
suggestions = get_recovery_suggestions(list(analysis.error_summary.keys()))
for s in suggestions.suggestions:
    print(f"[{s.priority}] {s.title}")
    if s.sparkctl_command:
        print(f"    Run: {s.sparkctl_command}")
```

## Claude Code Configuration

To use the sparkctl MCP server with Claude Code, add it to your MCP configuration. The server
requires no arguments and communicates over stdio.

```json
{
  "mcpServers": {
    "sparkctl": {
      "command": "sparkctl-mcp-server",
      "args": []
    }
  }
}
```
