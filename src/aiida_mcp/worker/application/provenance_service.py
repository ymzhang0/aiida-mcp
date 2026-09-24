"""Bounded, read-only AiiDA provenance queries for one project Group."""

from __future__ import annotations

from collections import deque
from typing import Any

from aiida import orm
from aiida.common.exceptions import NotExistent

from aiida_mcp.worker.protocol.errors import WorkerDomainError

_MAX_NODES = 300
_DETAIL_DEPTH = 12
_DETAIL_NODE_LIMIT = 2_000


def _group(group_uuid: str) -> orm.Group:
    try:
        return orm.load_group(uuid=str(group_uuid).strip())
    except (NotExistent, TypeError, ValueError) as exc:
        raise WorkerDomainError("not_found", "Group not found", group_uuid=group_uuid) from exc


def _node_payload(node: orm.Node) -> dict[str, Any]:
    state = getattr(node, "process_state", None)
    return {
        "pk": int(node.pk),
        "uuid": str(node.uuid),
        "label": str(node.label or node.__class__.__name__),
        "node_type": str(node.node_type),
        "process_type": str(getattr(node, "process_type", "") or ""),
        "process_label": str(getattr(node, "process_label", "") or ""),
        "process_state": getattr(state, "value", str(state or "")) or None,
        "ctime": node.ctime.isoformat() if getattr(node, "ctime", None) else None,
        "mtime": node.mtime.isoformat() if getattr(node, "mtime", None) else None,
    }


def _roots(group_uuid: str, limit: int) -> list[orm.ProcessNode]:
    roots: list[orm.ProcessNode] = []
    for node in _group(group_uuid).nodes:
        if not isinstance(node, orm.ProcessNode):
            continue
        try:
            if node.caller is not None:
                continue
        except Exception:  # A missing entry point must not widen the query.
            continue
        roots.append(node)
    return sorted(roots, key=lambda node: node.ctime, reverse=True)[:limit]


def roots(group_uuid: str, limit: int = 100) -> dict[str, Any]:
    if not str(group_uuid or "").strip():
        raise WorkerDomainError("invalid_argument", "group_uuid is required")
    return {"items": [_node_payload(node) for node in _roots(group_uuid, max(1, min(int(limit), 200)))]}


def _links(node: orm.Node) -> list[tuple[orm.Node, dict[str, str]]]:
    links: list[tuple[orm.Node, dict[str, str]]] = []
    for direction, manager in (("incoming", node.base.links.get_incoming()), ("outgoing", node.base.links.get_outgoing())):
        for triple in manager.all():
            other = triple.node
            link_type = getattr(triple, "link_type", "")
            links.append((other, {
                "source_uuid": str(other.uuid) if direction == "incoming" else str(node.uuid),
                "target_uuid": str(node.uuid) if direction == "incoming" else str(other.uuid),
                "label": str(getattr(triple, "link_label", "") or ""),
                "type": str(getattr(link_type, "value", link_type)),
            }))
    return links


def subgraph(group_uuid: str, root_uuid: str, depth: int = 2, *, node_limit: int = _MAX_NODES) -> dict[str, Any]:
    roots_by_uuid = {str(node.uuid): node for node in _roots(group_uuid, 200)}
    root = roots_by_uuid.get(str(root_uuid).strip())
    if root is None:
        raise WorkerDomainError("permission_denied", "Root is not a top-level process in this project", root_uuid=root_uuid)
    remaining_depth = max(0, min(int(depth), 6))
    nodes = {str(root.uuid): root}
    edges: dict[tuple[str, str, str, str], dict[str, str]] = {}
    queue = deque([(root, 0)])
    truncated = False
    while queue:
        node, distance = queue.popleft()
        if distance >= remaining_depth:
            continue
        for other, edge in _links(node):
            edges[(edge["source_uuid"], edge["target_uuid"], edge["label"], edge["type"])] = edge
            other_uuid = str(other.uuid)
            if other_uuid in nodes:
                continue
            if len(nodes) >= node_limit:
                truncated = True
                continue
            nodes[other_uuid] = other
            queue.append((other, distance + 1))
    return {
        "root_uuid": str(root.uuid),
        "depth": remaining_depth,
        "nodes": [_node_payload(node) for node in nodes.values()],
        "links": list(edges.values()),
        "truncated": truncated,
    }


def node_detail(group_uuid: str, uuid: str) -> dict[str, Any]:
    target = str(uuid or "").strip()
    if not target:
        raise WorkerDomainError("invalid_argument", "uuid is required")
    for root in _roots(group_uuid, 200):
        graph = subgraph(group_uuid, str(root.uuid), _DETAIL_DEPTH, node_limit=_DETAIL_NODE_LIMIT)
        if any(item["uuid"] == target for item in graph["nodes"]):
            node = orm.load_node(target)
            return {"node": _node_payload(node), "links": [link for link in graph["links"] if target in (link["source_uuid"], link["target_uuid"])]}
    raise WorkerDomainError("permission_denied", "Node is outside this project's provenance closure", uuid=target)
