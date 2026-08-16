# AiiDA MCP

`aiida-mcp` is the shared control plane for the AiiDA ChatGPT plugin and the
AiiDA Manager.  It owns project bindings and worker lifecycles; it never adds
an HTTP listener to `aiida-worker`, which remains a stdio JSON-RPC process.

## Run locally

```bash
uv run --project . aiida-mcp
```

The daemon binds to `127.0.0.1:8043` by default:

- `http://127.0.0.1:8043/` is the local AiiDA Manager console.`n- `http://127.0.0.1:8043/api/projects` is its project API.
- `http://127.0.0.1:8043/mcp` is the ChatGPT Plugin's streamable MCP endpoint.

Set `AIIDA_MCP_TOKEN` before exposing `/mcp` through a tunnel.  The token is a
development guard only; production deployment must replace it with OAuth and
project-level authorization.

Create an AiiDA Project from the Manager first.  Its immutable `project_ref`
is the value placed in the ChatGPT Project instructions and passed to every
MCP tool.  A project binds an AiiDA profile, interpreter and AiiDA Group UUID;
the Group label is only used until the worker's group-query protocol receives
native UUID filtering.
