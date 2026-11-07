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
(search_structures + import_structure for known materials, else create_structure) -> validate_submission (validates, stores nothing) -> submit
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
    @tool(name="get_process", description="Read a process by PK or UUID: state, exit status/message, inputs, outputs with their values (e.g. a PwCalculation's energy in output_parameters), called sub-processes, and reports. Compact by default; detail='full' returns every field (large).", annotations=_READ_ONLY)
    async def process_get(identifier: str, detail: str = "compact") -> dict[str, Any]:
        result = await service.process_get(identifier)
        return result if detail == "full" else compact_process(result)
    @tool(name="get_workgraph", description="Read the call graph of an aiida-workgraph WorkGraph process.", annotations=_READ_ONLY)
    async def process_workgraph(identifier: str) -> dict[str, Any]: return await service.process_workgraph(identifier)
    async def _wait_with_result(identifier: str, timeout_seconds: int) -> dict[str, Any]:
        state = await service.wait_for_process(identifier, timeout_seconds=timeout_seconds)
        if state.get("terminated"):
            result = compact_process(await service.process_get(identifier))
            return {"timed_out": False, **{key: value for key, value in result.items() if key != "note"}}
        return state

    @tool(name="wait_for_process", description="Block until a process terminates or timeout_seconds passes (max 1800). When it has terminated, returns the same compact view as get_process (state, exit status, output values such as the energy), so no get_process call is needed. On timeout returns the current state. For HPC jobs that may queue for hours, use a modest timeout and tell the user the PK.", annotations=_READ_ONLY)
    async def wait_for_process(identifier: str, timeout_seconds: int = 300) -> dict[str, Any]: return await _wait_with_result(identifier, timeout_seconds)
    @tool(name="get_process_logs", description="Read process reports and scheduler stderr excerpt by PK or UUID.", annotations=_READ_ONLY)
    async def process_logs(identifier: str) -> dict[str, Any]: return await service.process_logs(identifier)

    # Workflows, codes, submission drafting
    @tool(name="list_workflows", description="List installed workflow entry points with their description, protocols, and required inputs.", annotations=_READ_ONLY)
    async def workflow_list() -> dict[str, Any]: return await service.workflow_catalog()
    @tool(name="get_workflow_spec", description="Inputs of a workflow or calculation entry point (e.g. 'quantumespresso.pw.base'), one line per port, plus its protocols and get_builder_from_protocol arguments. Not needed when you submit with a protocol and already know the arguments. Pass port_path to expand a namespace.", annotations=_READ_ONLY)
    async def workflow_spec(entry_point: str, port_path: str | None = None) -> dict[str, Any]: return compact_spec(await service.workflow_spec(entry_point), port_path)
    @tool(name="find_input_candidates", description="List stored nodes compatible with one input port, e.g. port_path='pw.structure'.", annotations=_READ_ONLY)
    async def workflow_input_candidates(entry_point: str, port_path: str, limit: int = 50) -> dict[str, Any]: return await service.workflow_input_candidates(entry_point, port_path, limit=limit)
    @tool(name="list_codes", description="List configured codes, optionally only those on one computer. Use full_label ('label@computer') as a code input.", annotations=_READ_ONLY)
    async def code_list(computer_label: str | None = None) -> dict[str, Any]: return await service.code_list(computer_label)
    @tool(name="validate_submission", description="Build and validate a process builder without storing or submitting anything. Give explicit `inputs`, or omit them and give `protocol` plus `protocol_arguments` (and optional `overrides`) for get_builder_from_protocol. Protocol arguments accept {\"pk\": N} node references, dicts of codes ({\"pw\": \"pw@cluster\"}), and enum names as strings ('SCDM', 'METAL'). After the builder is built, `calcjob_options` (e.g. {\"resources\": {...}, \"queue_name\": \"...\", \"max_wallclock_seconds\": 3600}) is applied to every calculation in it, and `builder_updates` sets dotted paths, e.g. {\"epw_base.parallelization\": {\"npool\": 16}, \"w90_bands.projwfc.projwfc.settings\": {\"CMDLINE\": [\"-nd\", \"1\"]}} (Dicts are merged).", annotations=_READ_ONLY)
    async def submission_draft(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None, calcjob_options: dict[str, Any] | None = None, builder_updates: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.submission_draft(_draft(entry_point, inputs, protocol, protocol_arguments, overrides, calcjob_options=calcjob_options, builder_updates=builder_updates))

    # Structure databases (OPTIMADE)
    @tool(name="search_structures", description="Search a structure database over OPTIMADE and return one compact row per match (id, formula, nsites, lattice lengths, and per-database extras: MC3D total/per-atom energy and original source, MC2D band gap, MP energy above hull). provider: 'mc3d' (Materials Cloud 3D, relaxed with AiiDA, PBE), 'mc2d' (Materials Cloud 2D layers, non-periodic along z), or 'mp' (Materials Project). Give `formula` (any order, e.g. 'SiO2') and/or `elements` (+ elements_only to exclude extra elements), optional max_sites.", annotations=_REMOTE_READ)
    async def search_structures(provider: str, formula: str | None = None, elements: list[str] | None = None, elements_only: bool = False, max_sites: int | None = None, limit: int = 10) -> dict[str, Any]:
        return await service.search_structures(provider, formula=formula, elements=elements, elements_only=elements_only, max_sites=max_sites, limit=limit)

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
        @tool(name="import_structure", description="Store one structure from search_structures as StructureData, recording its database, id, and URL in extras. Importing the same entry again returns the existing node (deduplicated). MC2D layers are stored with pbc (True, True, False). Returns the PK to use as {\"pk\": N}.", annotations=_REMOTE_WRITE)
        async def import_structure(provider: str, entry_id: str, label: str | None = None) -> dict[str, Any]: return await service.import_structure(provider, entry_id, label=label)
        @tool(name="create_computer", description="Create and configure a new AiiDA computer. Tests the connection first and stores nothing if it fails. Never modifies an existing computer. Give `fields` (computer_label, hostname, transport_type such as core.ssh_async/core.ssh/core.local, scheduler_type such as core.slurm, work_dir, mpirun_command, mpiprocs_per_machine, prepend_text, username, proxy_jump, key_filename, ...) and/or verdi-style `setup_yaml` and `configure_yaml`; `fields` win.", annotations=_REMOTE_WRITE)
        async def computer_create(fields: dict[str, Any] | None = None, setup_yaml: str | None = None, configure_yaml: str | None = None) -> dict[str, Any]:
            return await service.computer_create({"fields": dict(fields or {}), "setup_yaml": setup_yaml, "configure_yaml": configure_yaml})
        @tool(name="create_code", description="Register an existing executable as a new AiiDA InstalledCode. Checks the executable on the computer first and stores nothing if it is missing. Give `fields` (label, computer_label, remote_abspath as absolute path or PATH name, default_calc_job_plugin such as quantumespresso.pw, prepend_text such as module loads, with_mpi) and/or verdi-style `yaml`; `fields` win.", annotations=_REMOTE_WRITE)
        async def code_create(fields: dict[str, Any] | None = None, yaml: str | None = None) -> dict[str, Any]:
            return await service.code_create({"fields": dict(fields or {}), "yaml": yaml})

        @tool(name="submit", description="Validate and submit a workflow or calculation to the AiiDA daemon; invalid inputs are reported back and nothing is submitted, so a separate validate_submission call is only needed to preview. Same arguments as validate_submission, plus optional `label`, `description`, and `group` (a label; created if missing). With wait_seconds > 0 it also waits up to that long and returns the result (as wait_for_process does), so a short job needs one call.", annotations=_WRITE)
        async def submit(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None, calcjob_options: dict[str, Any] | None = None, builder_updates: dict[str, Any] | None = None, label: str | None = None, description: str | None = None, group: str | None = None, wait_seconds: int = 0) -> dict[str, Any]:
            submitted = await service.submission_submit(_draft(entry_point, inputs, protocol, protocol_arguments, overrides, label=label, description=description, group=group, calcjob_options=calcjob_options, builder_updates=builder_updates))
            if wait_seconds > 0 and submitted.get("pk"):
                return {"submitted": submitted, **(await _wait_with_result(str(submitted["pk"]), wait_seconds))}
            return submitted
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


def _shrink(value: Any, depth: int = 0) -> Any:
    """Keep scalars and small structures; replace long lists, long strings and deep nesting by a short note."""
    if isinstance(value, dict):
        if depth >= 3:
            return f"<dict of {len(value)}>"
        return {str(key): _shrink(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_shrink(item, depth + 1) for item in value] if len(value) <= 6 else f"<list of {len(value)}>"
    if isinstance(value, str) and len(value) > 300:
        return value[:300] + f"... <{len(value)} chars>"
    return value


def _compact_entry(entry: Any) -> Any:
    """One linked node as the agent needs it: identity plus its value, never the same data twice."""
    if not isinstance(entry, dict):
        return entry
    out: dict[str, Any] = {"pk": entry.get("pk"), "type": entry.get("process_label") or entry.get("type")}
    if entry.get("state"):
        out.update({"state": entry.get("state"), "exit_status": entry.get("exit_status")})
    preview = entry.get("preview_info") if isinstance(entry.get("preview_info"), dict) else {}
    payload = entry.get("payload")
    if out["type"] == "StructureData":
        out.update({"formula": preview.get("formula") or payload, "nsites": preview.get("atom_count"), "volume_A3": preview.get("cell_volume")})
    elif payload is not None:
        out["value"] = _shrink(payload)
    label = entry.get("label")
    if label and label not in (out["type"], entry.get("type"), str(out.get("value"))):
        out["label"] = label
    return {key: value for key, value in out.items() if value is not None}


_OPTION_HINT = "resources, max_wallclock_seconds, queue_name, account, withmpi, prepend_text, custom_scheduler_commands"


def _port_line(port: dict[str, Any]) -> str:
    kinds = [kind for kind in port.get("valid_type") or [] if kind != "NoneType"]
    parts = ["/".join(kinds) or "any"]
    if port.get("required"):
        parts.append("required")
    if "default" in port and "<function" not in str(port["default"]):
        parts.append(f"default={port['default']!r}"[:60])
    if port.get("help"):
        parts.append(str(port["help"]).split(". ")[0][:100])
    return " | ".join(parts)


def _compact_ports(ports: dict[str, Any], depth: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, port in ports.items():
        if not isinstance(port, dict):
            continue
        if port.get("type") == "namespace":
            if name == "metadata":
                out[name] = f"namespace; options: {_OPTION_HINT}"
            elif depth <= 0:
                children = port.get("ports") or {}
                required = [child for child, spec in children.items() if isinstance(spec, dict) and spec.get("required")]
                out[name] = f"namespace of {len(children)} ports" + (f"; required: {', '.join(required)}" if required else "")
            else:
                out[name] = _compact_ports(port.get("ports") or {}, depth - 1)
        else:
            out[name] = _port_line(port)
    return out


def compact_spec(spec: dict[str, Any], port_path: str | None = None) -> dict[str, Any]:
    ports = spec.get("inputs") or {}
    if port_path:
        for part in port_path.split("."):
            node = ports.get(part)
            if not isinstance(node, dict):
                raise ValueError(f"No input port {port_path!r}")
            ports = node.get("ports") or {part: node}
    out = {"entry_point": spec.get("entry_point"), "inputs": _compact_ports(ports, depth=1)}
    for key in ("protocols", "protocol_arguments"):
        if spec.get(key):
            out[key] = spec[key]
    out["note"] = "Compact spec; pass port_path (e.g. 'pw') to expand a namespace."
    return out


def compact_process(detail: dict[str, Any]) -> dict[str, Any]:
    summary = detail.get("summary") or {}
    keep = ("pk", "uuid", "process_label", "state", "exit_status", "exit_message", "process_status", "paused", "ctime")
    outputs = detail.get("outputs") or {}
    called = {name: _compact_entry(entry) for name, entry in outputs.items() if isinstance(entry, dict) and entry.get("state")}
    logs = detail.get("logs") or {}
    return {
        "summary": {key: summary.get(key) for key in keep if summary.get(key) is not None},
        "inputs": {name: _compact_entry(entry) for name, entry in (detail.get("inputs") or {}).items()},
        "outputs": {name: _compact_entry(entry) for name, entry in outputs.items() if name not in called},
        "called": called,
        "reports": (logs.get("reports") or [])[-20:],
        "note": "Compact view: long lists are summarised. Pass detail='full' for every field, or get_node for one node.",
    }


def _draft(entry_point: str, inputs: dict[str, Any] | None, protocol: str | None, protocol_arguments: dict[str, Any] | None, overrides: dict[str, Any] | None, **organize: Any) -> dict[str, Any]:
    draft: dict[str, Any] = {"entry_point": entry_point, "protocol": protocol, "intent_data": dict(protocol_arguments or {}), "overrides": dict(overrides or {})}
    draft.update({key: value for key, value in organize.items() if value})
    if inputs is not None:
        draft["inputs"] = dict(inputs)
    return draft
