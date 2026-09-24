# AiiDA MCP

`aiida-mcp` is a generic, read-only AiiDA runtime interface for AI/MCP clients.
It has no project registry, `project_ref`, campaign, Group-scope policy, or
Manager web page.

Start one instance for one human-selected runtime:

```bash
AIIDA_MCP_PYTHON=/path/to/project/.venv/bin/python \
AIIDA_MCP_PROFILE=silicon \
AIIDA_MCP_WORKSPACE=/path/to/project \
uv run aiida-mcp serve
```

The selected Python and profile are fixed when the server starts; tools cannot
supply either value. The server listens at `127.0.0.1:8043` by default.

- `/mcp` provides generic read-only MCP tools.
- `/api` provides the matching local generic transport for `aiida-project`.

The private `aiida_mcp.worker` subprocess executes in the selected Python
environment and is the only code that imports AiiDA or queries its ORM.

Project scope belongs to `aiida-project`, which reads `project.yaml`, resolves
its declared Group policy, and calls this generic runtime. The Manager UI is
owned by `aiida-manager`.

`process.workgraph` is the process-structure API. It returns the actual call
graph; there is deliberately no `process.tree` alias.

The generic read-only surface also includes direct Node/Link and bounded
provenance traversal. `node.compare` is implemented inside the worker: it
recursively compares attributes, ports, links, arrays, and repository content
without importing `aiida-analyser`.

`archive.inspect` reads a native AiiDA archive without importing it. Its path
must be relative to the configured workspace, so it cannot become an arbitrary
local-file reader.
