# AiiDA MCP quickstart

Start one server for a human-selected Python environment and AiiDA profile:

```bash
AIIDA_MCP_PYTHON=/path/to/project/.venv/bin/python \
AIIDA_MCP_PROFILE=silicon \
AIIDA_MCP_WORKSPACE=/path/to/project \
uv run aiida-mcp serve
```

Connect an agent to `http://127.0.0.1:8043/mcp/`. For example, in Claude Code:

```bash
claude mcp add --transport http aiida http://127.0.0.1:8043/mcp/
```

Or let the agent launch it over stdio:

```json
{"mcpServers": {"aiida": {"command": "aiida-mcp", "args": ["serve"],
  "env": {"AIIDA_MCP_TRANSPORT": "stdio",
          "AIIDA_MCP_PYTHON": "/path/to/project/.venv/bin/python",
          "AIIDA_MCP_PROFILE": "silicon"}}}}
```

Submitting requires a running daemon (`verdi daemon start`). Add `--read-only`
to expose inspection tools only.
