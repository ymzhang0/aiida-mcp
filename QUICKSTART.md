# AiiDA MCP quickstart

Start one local generic runtime for a human-selected Python environment and AiiDA profile:

```bash
AIIDA_MCP_PYTHON=/path/to/project/.venv/bin/python \
AIIDA_MCP_PROFILE=silicon \
AIIDA_MCP_WORKSPACE=/path/to/project \
uv run aiida-mcp serve
```

The local API is `http://127.0.0.1:8043/api`; the read-only MCP endpoint is
`http://127.0.0.1:8043/mcp`. It has no Manager UI, project registry or
`project_ref` parameter. Use `aiida-project manager-serve` for a scoped project
view and `aiida-manager` only for the desktop shell.
