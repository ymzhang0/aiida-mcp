"""Generic MCP tools for one explicitly configured AiiDA runtime."""
from __future__ import annotations
import logging
from typing import Any
from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.tools.base import ToolResult
from mcp.types import ToolAnnotations
from .service import AiiDAService

log = logging.getLogger(__name__)

_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}
_WRITE = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False}
_DESTRUCTIVE = {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": False}
_REMOTE_READ = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
_REMOTE_WRITE = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True}

_INSTRUCTIONS = """AiiDA tools for the single Python environment and profile selected when this server started.
Checking work: list_processes (filter by process_state such as waiting/running/finished/excepted/killed)
-> get_process / get_process_logs / get_descendants for one process.
Running work: list_workflows -> get_workflow_spec -> list_codes / find_input_candidates
(create_structure if no stored structure fits) -> validate_submission (validates, stores nothing) -> submit
-> wait_for_process (do not poll get_process in a loop). If it failed: get_process / diagnose_process explain why.
Setting up a cluster: list_ssh_hosts / list_computers -> create_computer (tests the connection
first, stores nothing on failure) -> check_code -> create_code. Existing computers and codes are never modified.
Input values: plain JSON for base types (2 -> Int, {"a": 1} -> Dict); reference stored nodes as {"pk": N} or
{"uuid": "..."}; codes as "label@computer". Other tools come from installed plugins."""


class PluginTool(Tool):
    """Proxy one worker-side plugin function; its schema comes from the worker manifest."""

    service: Any = None

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        result = await self.service.plugin_call(self.name, arguments)
        return ToolResult(structured_content=result)


def build_mcp_server(service: AiiDAService, *, allow_write: bool = False, plugin_tools: list[dict[str, Any]] | None = None) -> FastMCP:
    server = FastMCP("aiida", instructions=_INSTRUCTIONS if allow_write else _INSTRUCTIONS + "\nThis server is read-only: submission and process control are disabled.")
    names: set[str] = set()

    def tool(**kwargs: Any):
        names.add(kwargs["name"])
        return server.tool(**kwargs)

    # Runtime, nodes, groups, provenance
    @tool(name="get_status", description="Read the configured AiiDA runtime status: profile, versions, daemon, installed workflow plugins.", annotations=_READ_ONLY)
    async def runtime_status() -> dict[str, Any]: return await service.status()
    @tool(name="list_nodes", description="List recent nodes in the configured runtime.", annotations=_READ_ONLY)
    async def node_search(limit: int = 50, node_type: str | None = None) -> dict[str, Any]: return await service.nodes(limit=limit, node_type=node_type)
    @tool(name="get_node", description="Read a node by PK.", annotations=_READ_ONLY)
    async def node_get(pk: int) -> dict[str, Any]: return await service.node_get(pk)
    @tool(name="get_node_links", description="Read direct AiiDA links for a node UUID or PK.", annotations=_READ_ONLY)
    async def node_links(identifier: str) -> dict[str, Any]: return await service.node_links(identifier)
    @tool(name="compare_nodes", description="Compare two AiiDA nodes recursively by attributes, ports, links, arrays, and repository content.", annotations=_READ_ONLY)
    async def node_compare(left: str, right: str, depth: int = 2, max_differences: int = 100) -> dict[str, Any]: return await service.node_compare(left, right, depth=depth, max_differences=max_differences)
    @tool(name="get_group", description="Read a Group by UUID or label.", annotations=_READ_ONLY)
    async def group_get(identifier: str) -> dict[str, Any]: return await service.group_get(identifier)
    @tool(name="list_groups", description="List Groups in the configured runtime.", annotations=_READ_ONLY)
    async def group_list() -> dict[str, Any]: return await service.group_list()
    @tool(name="get_ancestors", description="Read bounded upstream provenance for a node UUID.", annotations=_READ_ONLY)
    async def provenance_ancestors(root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_walk(root_uuid, direction="ancestors", depth=depth)
    @tool(name="get_descendants", description="Read bounded downstream provenance for a node UUID.", annotations=_READ_ONLY)
    async def provenance_descendants(root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_walk(root_uuid, direction="descendants", depth=depth)
    @tool(name="list_provenance_roots", description="List top-level process roots for a supplied Group UUID.", annotations=_READ_ONLY)
    async def provenance_roots(group_uuid: str, limit: int = 100) -> dict[str, Any]: return await service.provenance_roots(group_uuid, limit=limit)
    @tool(name="get_provenance_subgraph", description="Read a bounded real AiiDA provenance graph.", annotations=_READ_ONLY)
    async def provenance_subgraph(group_uuid: str, root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_subgraph(group_uuid, root_uuid, depth=depth)
    @tool(name="inspect_archive", description="Read a workspace-relative AiiDA archive summary without importing it.", annotations=_READ_ONLY)
    async def archive_inspect(path: str) -> dict[str, Any]: return await service.archive_inspect(path)

    # Processes
    @tool(name="list_processes", description="List processes newest first. Filter by process_state (created, waiting, running, finished, excepted, killed), label/process-label substring or PK, age in days, or Group UUID. root_only=False includes sub-processes.", annotations=_READ_ONLY)
    async def process_list(limit: int = 20, process_state: str | None = None, label: str | None = None, root_only: bool = True, days: int | None = None, group_uuid: str | None = None) -> dict[str, Any]:
        return await service.process_list(limit=limit, process_state=process_state, label=label, root_only=root_only, days=days, group_uuid=group_uuid)
    @tool(name="get_process", description="Read a process by PK or UUID: state, exit status, inputs, outputs, and called sub-processes.", annotations=_READ_ONLY)
    async def process_get(identifier: str) -> dict[str, Any]: return await service.process_get(identifier)
    @tool(name="get_workgraph", description="Read the call graph of an aiida-workgraph WorkGraph process.", annotations=_READ_ONLY)
    async def process_workgraph(identifier: str) -> dict[str, Any]: return await service.process_workgraph(identifier)
    @tool(name="wait_for_process", description="Block until a process terminates or timeout_seconds passes (max 1800), then return its state, exit status/message, and output names. Use this instead of calling get_process repeatedly. For HPC jobs that may queue for hours, use a modest timeout and tell the user the PK.", annotations=_READ_ONLY)
    async def wait_for_process(identifier: str, timeout_seconds: int = 300) -> dict[str, Any]: return await service.wait_for_process(identifier, timeout_seconds=timeout_seconds)
    @tool(name="get_process_logs", description="Read process reports and scheduler stderr excerpt by PK or UUID.", annotations=_READ_ONLY)
    async def process_logs(identifier: str) -> dict[str, Any]: return await service.process_logs(identifier)

    # Workflows, codes, submission drafting
    @tool(name="list_workflows", description="List installed workflow entry points with their description, protocols, and required inputs.", annotations=_READ_ONLY)
    async def workflow_list() -> dict[str, Any]: return await service.workflow_catalog()
    @tool(name="get_workflow_spec", description="Read the full input port specification of a workflow or calculation entry point (e.g. 'quantumespresso.pw.base' or 'core.arithmetic.add').", annotations=_READ_ONLY)
    async def workflow_spec(entry_point: str) -> dict[str, Any]: return await service.workflow_spec(entry_point)
    @tool(name="find_input_candidates", description="List stored nodes compatible with one input port, e.g. port_path='pw.structure'.", annotations=_READ_ONLY)
    async def workflow_input_candidates(entry_point: str, port_path: str, limit: int = 50) -> dict[str, Any]: return await service.workflow_input_candidates(entry_point, port_path, limit=limit)
    @tool(name="list_codes", description="List configured codes, optionally only those on one computer. Use full_label ('label@computer') as a code input.", annotations=_READ_ONLY)
    async def code_list(computer_label: str | None = None) -> dict[str, Any]: return await service.code_list(computer_label)
    @tool(name="validate_submission", description="Build and validate a process builder without storing or submitting anything. Give explicit `inputs`, or omit them and give `protocol` plus `protocol_arguments` (and optional `overrides`) for get_builder_from_protocol.", annotations=_READ_ONLY)
    async def submission_draft(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.submission_draft(_draft(entry_point, inputs, protocol, protocol_arguments, overrides))

    # Computers and codes
    @tool(name="list_ssh_hosts", description="List host aliases from the user's ~/.ssh/config (hostname, user, port, ProxyJump, identity file path; never key contents). Use them to fill create_computer.", annotations=_READ_ONLY)
    async def ssh_hosts() -> dict[str, Any]: return await service.ssh_hosts()
    @tool(name="list_computers", description="List AiiDA computers: hostname, transport, scheduler, work dir, MPI command, and whether the current user has configured access.", annotations=_READ_ONLY)
    async def computer_list() -> dict[str, Any]:
        computers = await service.computer_list()
        return {**computers, "capabilities": await service.infrastructure_capabilities()}
    @tool(name="check_code", description="Check over the computer's transport whether an executable exists, after the computer's prepend_text and the given prepend_text (e.g. 'module load QuantumESPRESSO'). `executable` may be a name on PATH ('pw.x') or an absolute path; returns the resolved path.", annotations=_REMOTE_READ)
    async def code_check(computer_label: str, executable: str, prepend_text: str | None = None) -> dict[str, Any]: return await service.code_check_executable(computer_label, executable, prepend_text)

    if allow_write:
        @tool(name="create_structure", description="Store a crystal structure (StructureData) and return its PK for use as {\"pk\": N}. Give exactly one of: `bulk` = ASE bulk() parameters, e.g. {\"name\": \"Si\", \"crystalstructure\": \"diamond\", \"a\": 5.43} (primitive cell; add \"cubic\": true for the conventional cell); `file` = workspace-relative CIF/POSCAR/xyz path; or `cell` (3x3, Angstrom) + `sites` [{\"symbol\": \"Si\", \"position\": [x, y, z]}] in Cartesian Angstrom. Optional `supercell` [n1, n2, n3], `label`, `description`.", annotations=_WRITE)
        async def create_structure(bulk: dict[str, Any] | None = None, file: str | None = None, cell: list[list[float]] | None = None, sites: list[dict[str, Any]] | None = None, pbc: list[bool] | None = None, supercell: list[int] | None = None, label: str | None = None, description: str | None = None) -> dict[str, Any]:
            params = {"bulk": bulk, "file": file, "cell": cell, "sites": sites, "pbc": pbc, "supercell": supercell, "label": label, "description": description}
            return await service.create_structure({key: value for key, value in params.items() if value is not None})
        @tool(name="create_computer", description="Create and configure a new AiiDA computer. Tests the connection first and stores nothing if it fails. Never modifies an existing computer. Give `fields` (computer_label, hostname, transport_type such as core.ssh_async/core.ssh/core.local, scheduler_type such as core.slurm, work_dir, mpirun_command, mpiprocs_per_machine, prepend_text, username, proxy_jump, key_filename, ...) and/or verdi-style `setup_yaml` and `configure_yaml`; `fields` win.", annotations=_REMOTE_WRITE)
        async def computer_create(fields: dict[str, Any] | None = None, setup_yaml: str | None = None, configure_yaml: str | None = None) -> dict[str, Any]:
            return await service.computer_create({"fields": dict(fields or {}), "setup_yaml": setup_yaml, "configure_yaml": configure_yaml})
        @tool(name="create_code", description="Register an existing executable as a new AiiDA InstalledCode. Checks the executable on the computer first and stores nothing if it is missing. Give `fields` (label, computer_label, remote_abspath as absolute path or PATH name, default_calc_job_plugin such as quantumespresso.pw, prepend_text such as module loads, with_mpi) and/or verdi-style `yaml`; `fields` win.", annotations=_REMOTE_WRITE)
        async def code_create(fields: dict[str, Any] | None = None, yaml: str | None = None) -> dict[str, Any]:
            return await service.code_create({"fields": dict(fields or {}), "yaml": yaml})

        @tool(name="submit", description="Submit a workflow or calculation to the AiiDA daemon. Takes the same arguments as validate_submission; run that first. Returns the new process PK.", annotations=_WRITE)
        async def submit(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
            return await service.submission_submit(_draft(entry_point, inputs, protocol, protocol_arguments, overrides))
        @tool(name="kill_processes", description="Kill running or waiting processes (PKs or UUIDs). Irreversible.", annotations=_DESTRUCTIVE)
        async def process_kill(identifiers: list[str], force: bool = False) -> dict[str, Any]: return await service.process_control("kill", identifiers, force=force)
        @tool(name="pause_processes", description="Pause running or waiting processes (PKs or UUIDs).", annotations=_WRITE)
        async def process_pause(identifiers: list[str]) -> dict[str, Any]: return await service.process_control("pause", identifiers)
        @tool(name="resume_processes", description="Resume paused processes (PKs or UUIDs).", annotations=_WRITE)
        async def process_play(identifiers: list[str]) -> dict[str, Any]: return await service.process_control("play", identifiers)

    for item in plugin_tools or []:
        if not item.get("read_only") and not allow_write:
            continue
        if item["name"] in names:
            log.warning("Skipping plugin tool %r from %r: the name is already used by a built-in tool", item["name"], item.get("plugin"))
            continue
        server.add_tool(PluginTool(
            name=str(item["name"]),
            description=str(item.get("description") or item["name"]),
            parameters=dict(item.get("input_schema") or {"type": "object", "properties": {}}),
            annotations=ToolAnnotations(**(_READ_ONLY if item.get("read_only") else _DESTRUCTIVE)),
            tags={"plugin", str(item.get("plugin") or "")},
            service=service,
        ))
    return server


def _draft(entry_point: str, inputs: dict[str, Any] | None, protocol: str | None, protocol_arguments: dict[str, Any] | None, overrides: dict[str, Any] | None) -> dict[str, Any]:
    draft: dict[str, Any] = {"entry_point": entry_point, "protocol": protocol, "intent_data": dict(protocol_arguments or {}), "overrides": dict(overrides or {})}
    if inputs is not None:
        draft["inputs"] = dict(inputs)
    return draft
