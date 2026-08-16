# Vibe research quickstart

This first implementation runs one `aiida-mcp` daemon for both surfaces.

```bash
cd /home/yzhang/project/aiida-llm
PYTHONPATH=aiida-mcp/src aris/.venv/bin/python -m aiida_mcp.launch
```

The AiiDA Manager console is served by the same daemon at `http://127.0.0.1:8043/`; its API is under `/api`.
The ChatGPT Plugin endpoint is `http://127.0.0.1:8043/mcp`; expose only this
route using a secure tunnel and configure `AIIDA_MCP_TOKEN` before doing so.

Create the scientific project through `POST /api/projects` first.  The result
contains the `project_ref` that the plugin passes for every AiiDA tool call.
