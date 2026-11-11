# AiiDA MCP

`aiida-mcp` lets an AI agent work with one AiiDA profile: inspect processes,
discover and submit workflows, and kill, pause or resume running work. Extra
tools from other packages (such as `aiida-analyser`) plug in through an
entry point.

## Use it from Claude Code

The repository is also a Claude Code plugin marketplace. The `aiida` plugin
bundles this server and a skill that teaches the agent how to check, draft,
confirm, and submit work:

```
/plugin marketplace add ymzhang0/aiida-mcp
/plugin install aiida@aiida-mcp
```

See [plugin/README.md](plugin/README.md). Other agents can use the server
directly over MCP, as described below.

## Run the server

Start one instance for one human-selected runtime:

```bash
AIIDA_MCP_PYTHON=/path/to/project/.venv/bin/python \
AIIDA_MCP_PROFILE=silicon \
AIIDA_MCP_WORKSPACE=/path/to/project \
uv run aiida-mcp serve            # add --read-only to disable all writes
```

The Python environment and profile are fixed when the server starts; tools
cannot change either. The server listens on `127.0.0.1:8043` by default
(`AIIDA_MCP_HOST`, `AIIDA_MCP_PORT`). Use `AIIDA_MCP_TRANSPORT=stdio` for
agents that launch MCP servers as subprocesses.

- `/mcp` is the MCP endpoint (streamable HTTP).
- Set `AIIDA_MCP_TOKEN` to require `Authorization: Bearer <token>` on both
  `/mcp` and `/api`. Without a token the server only answers requests whose
  `Host` is `127.0.0.1`/`localhost`, so a tunnel (ngrok, cloudflared) or a
  non-loopback `AIIDA_MCP_HOST` needs the token; the server refuses to start
  on a non-loopback address without one.
- `/api` is the matching read-only HTTP transport used by `aiida-project`.

## Tools

| Area | Tools |
| --- | --- |
| Check work | `list_processes` (filter by state, label, age, Group), `get_process`, `get_process_logs`, `get_workgraph` |
| Run work | `list_workflows`, `get_workflow_spec`, `find_input_candidates`, `list_codes`, `validate_submission`, **`submit`** |
| Structures | `search_structures` (MC3D, MC2D, Materials Project over OPTIMADE), **`import_structure`**, **`create_structure`** |
| Clusters and codes | `list_ssh_hosts`, `list_computers`, `check_code`, **`create_computer`**, **`create_code`** |
| Control work | **`kill_processes`**, **`pause_processes`**, **`resume_processes`** |
| Explore data | `list_nodes`, `get_node`, `get_node_links`, `compare_nodes`, `list_groups`, `get_group`, `get_ancestors`, `get_descendants`, `list_provenance_roots`, `get_provenance_subgraph`, `inspect_archive`, `get_status` |
| Plugins | named after the plugin's functions, e.g. `diagnose_process` |

Tools in **bold** change the profile. They carry MCP `readOnlyHint: false`, and
kill also carries `destructiveHint: true`, so agent clients ask before running
them. `--read-only` or `AIIDA_MCP_READ_ONLY=1` removes them, together with any
plugin tool not marked read-only.

`create_computer` and `create_code` only create. They refuse a
label that already exists, so an agent cannot reconfigure a computer that has
running jobs. Before storing anything they test the connection, or check that
the executable exists after the given `prepend_text` (module loads). Both also
accept the YAML that `verdi computer setup --config` and
`verdi code create --config` use.

`submit` accepts workflows (`aiida.workflows`) and calculations
(`aiida.calculations`). Inputs are plain JSON: `2` becomes `Int(2)`,
`{"a": 1}` becomes `Dict`, a code is `"label@computer"`, and a stored node is
referenced as `{"pk": N}` or `{"uuid": "..."}`. Workflows with
`get_builder_from_protocol` can instead take `protocol`, `protocol_arguments`
and `overrides`. `validate_submission` takes the same arguments and only
validates.

Input values are never guessed between "a number" and "the PK of a node":

| JSON value | Meaning |
| --- | --- |
| `{"pk": 42}` or `{"uuid": "..."}` | the stored node |
| `{"value": 42}` | a new node holding 42 |
| `42` on an `Int`/`Float`/`Bool`/`Str` port | the value 42 |
| `42` or a UUID string on a port that cannot hold it (`StructureData`, ...) | that stored node |
| `42` on a port where both readings fit (`Data`) | rejected; use an explicit form |

Tool errors carry the worker's reason and structured detail, not only the
JSON-RPC message ("Invalid params").

## Architecture

The server never imports AiiDA. A private worker subprocess
(`aiida_mcp.worker`) runs in the selected Python and talks JSON-RPC over
stdio. It is the only code that touches the ORM. That is why one server can
drive any project environment, including its AiiDA plugins.

## Plugins

A package adds tools by declaring an entry point in the **project's** Python
(the one given by `AIIDA_MCP_PYTHON`, not the server's environment):

```toml
[project.entry-points."aiida_mcp.tools"]
analyser = "aiida_analyser.mcp:tools"
```

The target is a list of plain functions, or a callable that returns one.
Each function becomes a tool with the function's name, so name functions verb first:

```python
def diagnose_process(identifier: str, include_outputs: bool = False) -> dict:
    """Diagnose a process: failing branches and why they failed."""
    ...

diagnose_process.read_only = True   # omit for tools that modify the profile
tools = [diagnose_process]
```

- The docstring becomes the description the agent reads, so write it for the
  agent.
- Type hints become the JSON input schema; unknown arguments are rejected.
- The profile is already loaded when the function runs. It must return
  JSON-serialisable data, and a non-dict result is wrapped as `{"result": ...}`.
- A name already used by a built-in tool or another plugin is skipped and
  logged, so choose specific names.
- Plugins never import `aiida_mcp`. The worker loads them; the server reads
  their manifest once at start-up. Restart the server after installing a
  plugin. A plugin that fails to import is logged and skipped. Use
  `--no-plugins` to disable plugin loading entirely.
