(mcp-server-api)=

# MCP Server API

The MCP server module provides tools for AI-assisted diagnosis of Spark job failures.

## Tools

These functions can be used directly in Python or through the MCP server.

```{eval-rst}
.. autofunction:: sparkctl.mcp_server.get_spark_logs
```

```{eval-rst}
.. autofunction:: sparkctl.mcp_server.analyze_spark_failure
```

```{eval-rst}
.. autofunction:: sparkctl.mcp_server.get_recovery_suggestions
```

```{eval-rst}
.. autofunction:: sparkctl.mcp_server.list_spark_applications
```

## Response Models

```{eval-rst}
.. autopydantic_model:: sparkctl.mcp_server.models.SparkLogsResponse
   :members:
```

```{eval-rst}
.. autopydantic_model:: sparkctl.mcp_server.models.SparkFailureAnalysis
   :members:
```

```{eval-rst}
.. autopydantic_model:: sparkctl.mcp_server.models.RecoverySuggestions
   :members:
```

```{eval-rst}
.. autopydantic_model:: sparkctl.mcp_server.models.SparkApplicationList
   :members:
```

## Utilities

```{eval-rst}
.. autoclass:: sparkctl.mcp_server.SparkLogParser
   :members:
```

```{eval-rst}
.. autoclass:: sparkctl.mcp_server.SparkLogLocator
   :members:
```

```{eval-rst}
.. autoclass:: sparkctl.mcp_server.ErrorPatternRegistry
   :members:
```

```{eval-rst}
.. autoclass:: sparkctl.mcp_server.RecoveryEngine
   :members:
```
