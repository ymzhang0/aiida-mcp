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

- `/mcp` is the MCP endpoint (streamable HTTP). Set `AIIDA_MCP_TOKEN` to
  require `Authorization: Bearer <token>`.
- `/api` is the matching read-only HTTP transport used by `aiida-project`.

## Tools

| Area | Tools |
| --- | --- |
| Check work | `aiida_process_list` (filter by state, label, age, Group), `aiida_process_get`, `aiida_process_logs`, `aiida_process_workgraph` |
| Run work | `aiida_workflow_list`, `aiida_workflow_spec`, `aiida_workflow_input_candidates`, `aiida_code_list`, `aiida_submission_draft`, **`aiida_submit`** |
| Clusters and codes | `aiida_ssh_hosts`, `aiida_computer_list`, `aiida_code_check`, **`aiida_computer_create`**, **`aiida_code_create`** |
| Control work | **`aiida_process_kill`**, **`aiida_process_pause`**, **`aiida_process_play`** |
| Explore data | `aiida_node_search`, `aiida_node_get`, `aiida_node_links`, `aiida_node_compare`, `aiida_group_list`, `aiida_group_get`, `aiida_provenance_*`, `aiida_archive_inspect`, `aiida_runtime_status` |
| Plugins | `<plugin>_<function>`, e.g. `analyser_process_report` |

Tools in **bold** change the profile. They carry MCP `readOnlyHint: false`, and
kill also carries `destructiveHint: true`, so agent clients ask before running
them. `--read-only` or `AIIDA_MCP_READ_ONLY=1` removes them, together with any
plugin tool not marked read-only.

`aiida_computer_create` and `aiida_code_create` only create. They refuse a
label that already exists, so an agent cannot reconfigure a computer that has
running jobs. Before storing anything they test the connection, or check that
the executable exists after the given `prepend_text` (module loads). Both also
accept the YAML that `verdi computer setup --config` and
`verdi code create --config` use.

`aiida_submit` accepts workflows (`aiida.workflows`) and calculations
(`aiida.calculations`). Inputs are plain JSON: `2` becomes `Int(2)`,
`{"a": 1}` becomes `Dict`, a code is `"label@computer"`, and a stored node is
referenced as `{"pk": N}` or `{"uuid": "..."}`. Workflows with
`get_builder_from_protocol` can instead take `protocol`, `protocol_arguments`
and `overrides`. `aiida_submission_draft` takes the same arguments and only
validates.

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
Each function becomes the tool `<entry point name>_<function name>`:

```python
def process_report(identifier: str, include_outputs: bool = False) -> dict:
    """Diagnose a process: failing branches and why they failed."""
    ...

process_report.read_only = True   # omit for tools that modify the profile
tools = [process_report]
```

- The docstring becomes the description the agent reads, so write it for the
  agent.
- Type hints become the JSON input schema; unknown arguments are rejected.
- The profile is already loaded when the function runs. It must return
  JSON-serialisable data, and a non-dict result is wrapped as `{"result": ...}`.
- Plugins never import `aiida_mcp`. The worker loads them; the server reads
  their manifest once at start-up. Restart the server after installing a
  plugin. A plugin that fails to import is logged and skipped. Use
  `--no-plugins` to disable plugin loading entirely.
