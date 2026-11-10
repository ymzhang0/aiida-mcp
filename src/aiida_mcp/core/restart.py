"""Resubmit a terminated process from its own inputs, reusing finished steps through AiiDA caching.

Caching is decided by the daemon from the profile configuration, so it cannot be switched on for one submission: this
module reports whether the finished steps will be reused, and makes reuse safe. ``RemoteData.is_cleaned`` is an extra,
so it does not enter the hash and a cached clone of a cleaned calculation still looks valid. That is harmless while the
cleaned folder is only passed to other steps that are themselves reused, but a step that reruns gets an empty
``parent_folder``. A finished calculation whose folder was cleaned and that no successful step consumed is therefore
marked invalid as a cache source (``node.base.caching.is_valid_cache = False``), together with every node of the same
hash whose folder was cleaned, so it reruns instead.
"""
from __future__ import annotations

from typing import Any, Mapping

from aiida import orm
from aiida.common.links import LinkType
from aiida.engine import submit
from aiida.manage.caching import get_use_cache

from aiida_mcp.core.submission import _apply_builder_edits, _list_missing_required_ports, _serialize_builder_inputs, _validate_builder_inputs
from aiida_mcp.worker.protocol.errors import WorkerDomainError

_MAX_STEPS = 40
_CACHING_HINT = (
    "Caching is off in this profile, so every step will be recomputed. To reuse finished steps, run "
    "`verdi config set caching.default_enabled True` (or `caching.enabled_for` with the calculation entry points) "
    "and `verdi daemon restart`; the daemon only reads the setting when it starts. Ask the user first: it applies to "
    "every later submission in the profile."
)


def _remote_folder(calc: orm.CalcJobNode) -> orm.RemoteData | None:
    return calc.outputs.remote_folder if "remote_folder" in calc.outputs else None


def _is_cleaned(remote: orm.RemoteData | None) -> bool:
    return bool(remote is not None and remote.is_cleaned)


def _consumers(remote: orm.RemoteData) -> list[int]:
    return [n.pk for n in remote.base.links.get_outgoing(node_class=orm.CalcJobNode, link_type=LinkType.INPUT_CALC).all_nodes()]


def _step(calc: orm.CalcJobNode, root: orm.ProcessNode) -> dict[str, Any]:
    """PK, label and the call path from the restarted process, e.g. 'w90_bands.scf.iteration_01'."""
    path, current = [], calc
    while current.pk != root.pk:
        links = current.base.links.get_incoming(link_type=(LinkType.CALL_CALC, LinkType.CALL_WORK)).all()
        if not links:
            break
        path.append(links[0].link_label)
        current = links[0].node
    return {"pk": calc.pk, "process_label": calc.process_label, "path": ".".join(reversed(path)) or None}


def _plan_reuse(node: orm.ProcessNode, *, apply: bool) -> dict[str, Any]:
    """Decide for every calculation the old process ran whether the restart reuses it, invalidating unsafe sources.

    Consumers always have larger PKs than the calculations whose folder they read, so walking from the newest
    calculation backwards decides every consumer before its producer.
    """
    calcs = [node] if isinstance(node, orm.CalcJobNode) else [n for n in node.called_descendants if isinstance(n, orm.CalcJobNode)]
    reused: dict[int, bool] = {}
    steps, invalidated = [], set()
    for calc in sorted(calcs, key=lambda c: c.pk, reverse=True):
        step = _step(calc, node)
        remote = _remote_folder(calc)
        if not calc.is_finished_ok:
            state = calc.process_state.value if calc.process_state else "unknown"
            step.update(reuse=False, reason=f"did not finish ok ({state}, exit {calc.exit_status})")
        elif not get_use_cache(identifier=calc.process_type):
            step.update(reuse=False, reason="caching disabled for this calculation type")
        elif not calc.base.caching.is_valid_cache:
            step.update(reuse=False, reason="marked invalid as a cache source")
        elif _is_cleaned(remote) and not any(reused.get(pk) for pk in _consumers(remote)):
            # A rerun step may read this folder: never serve it from a source whose folder is gone.
            sources = {n.pk: n for n in [calc, *calc.base.caching.get_all_same_nodes()] if isinstance(n, orm.CalcJobNode)}
            for source in sources.values():
                if source.base.caching.is_valid_cache and _is_cleaned(_remote_folder(source)):
                    invalidated.add(source.pk)
                    if apply:
                        source.base.caching.is_valid_cache = False
            intact = sorted(pk for pk in sources if pk not in invalidated and sources[pk].base.caching.is_valid_cache)
            if intact:
                step.update(reuse=True, reason=f"working directory was cleaned; will be taken from PK {intact[0]}, whose directory was not")
            else:
                step.update(reuse=False, reason="working directory was cleaned and a step that reruns may need it")
        else:
            step.update(reuse=True, reason="finished ok; will be taken from the cache")
        reused[calc.pk] = step["reuse"]
        steps.append(step)
    steps.reverse()
    counts = {"reuse": sum(reused.values()), "rerun": len(reused) - sum(reused.values())}
    return {"counts": counts, "steps": steps[:_MAX_STEPS], "steps_truncated": max(0, len(steps) - _MAX_STEPS), "invalidated_cache_sources": sorted(invalidated)}


def _cleaned_remote_inputs(inputs: Any, prefix: str = "") -> list[str]:
    found = []
    for key, value in (inputs.items() if isinstance(inputs, Mapping) else []):
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, orm.RemoteData) and value.is_cleaned:
            found.append(f"{path} (RemoteData PK {value.pk})")
        elif isinstance(value, Mapping):
            found.extend(_cleaned_remote_inputs(value, path))
    return found


def restart_process(params: Mapping[str, Any]) -> dict[str, Any]:
    identifier = str(params.get("identifier") or "").strip()
    if not identifier:
        raise WorkerDomainError("invalid_argument", "identifier is required")
    try:
        node = orm.load_node(identifier)
    except Exception as exc:
        raise WorkerDomainError("not_found", f"Process {identifier} not found: {exc}") from exc
    if not isinstance(node, (orm.WorkflowNode, orm.CalcJobNode)):
        raise WorkerDomainError("invalid_argument", f"Node {node.pk} is a {type(node).__name__}; only workflows and calculation jobs can be restarted")
    if not node.is_terminated:
        hint = "it is paused: resume_processes continues it without recomputing anything" if node.paused else "wait for it to finish, or kill it first"
        raise WorkerDomainError("invalid_argument", f"Process {node.pk} is still {node.process_state.value}; {hint}")

    dry_run = bool(params.get("dry_run"))
    errors: list[dict[str, Any]] = []
    builder = node.get_builder_restart()
    try:
        _apply_builder_edits(builder, dict(params.get("calcjob_options") or {}), dict(params.get("builder_updates") or {}))
    except Exception as exc:
        errors.append({"stage": "builder_updates", "message": str(exc)})
    builder_inputs = builder._inputs(prune=True)
    if not errors:
        errors += [{"stage": "missing_required_ports", "port": port, "message": f"Required input '{port}' is missing"} for port in _list_missing_required_ports(node.process_class.spec().inputs, builder_inputs)]
    if not errors and (error := _validate_builder_inputs(node.process_class, builder_inputs)) is not None:
        errors.append(error)

    warnings = [f"Input {item} was already cleaned; a step that reads it will fail. Restart the process that produced it instead." for item in _cleaned_remote_inputs(builder_inputs)]
    if errors:
        return {"success": False, "status": "invalid", "restarted_from": node.pk, "errors": errors, "warnings": warnings}

    plan = _plan_reuse(node, apply=not dry_run)
    if plan["invalidated_cache_sources"] and "clean_workdir" in builder._port_namespace and builder_inputs.get("clean_workdir"):
        warnings.append("The original run cleaned its working directories, so the steps whose folders are still needed rerun. "
                        "Pass builder_updates {\"clean_workdir\": false} so that a later restart of this run can reuse them.")
    if not any(get_use_cache(identifier=step_type) for step_type in {node.process_type, *(n.process_type for n in node.called_descendants if isinstance(n, orm.CalcJobNode))}):
        warnings.append(_CACHING_HINT)
    groups = [str(params["group"])] if params.get("group") else orm.QueryBuilder().append(orm.Node, filters={"id": node.pk}, tag="n").append(orm.Group, with_node="n", project="label").all(flat=True)
    result: dict[str, Any] = {
        "success": True, "status": "dry_run" if dry_run else "submitted", "restarted_from": node.pk,
        "entry_point": node.process_type, "reuse": plan, "groups": groups, "warnings": warnings,
    }
    if dry_run:
        result["builder_inputs"] = _serialize_builder_inputs(builder_inputs)
        if plan["invalidated_cache_sources"]:
            result["note"] = "Dry run: nothing was changed. Submitting will mark the listed cache sources invalid."
        return result

    builder.metadata.label = str(params.get("label") or node.label or "")
    builder.metadata.description = str(params.get("description") or f"Restart of {node.process_label} PK {node.pk}")
    new = submit(builder)
    new.base.extras.set("restarted_from", node.uuid)
    for label in groups:
        orm.Group.collection.get_or_create(label=label)[0].add_nodes(new)
    result.update(pk=new.pk, uuid=new.uuid, label=new.label or None)
    return result
