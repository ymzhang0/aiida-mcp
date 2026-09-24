"""Generic read-only Node/Link traversal without project policy."""
from __future__ import annotations
from collections import deque
from typing import Any
from aiida import orm
from aiida.common.exceptions import NotExistent
from aiida_mcp.worker.protocol.errors import WorkerDomainError

def _node(identifier: str):
    try: return orm.load_node(int(identifier) if identifier.isdigit() else identifier)
    except (NotExistent, TypeError, ValueError) as exc: raise WorkerDomainError("not_found", "Node not found", identifier=identifier) from exc

def _item(node: orm.Node) -> dict[str, Any]: return {"pk": int(node.pk), "uuid": str(node.uuid), "label": str(node.label or node.__class__.__name__), "node_type": str(node.node_type), "process_label": str(getattr(node, "process_label", "") or "")}
def links(identifier: str) -> dict[str, Any]:
    node = _node(identifier); items = []
    for direction, manager in (("incoming", node.base.links.get_incoming()), ("outgoing", node.base.links.get_outgoing())):
        for triple in manager.all():
            other = triple.node; kind = getattr(triple, "link_type", "")
            items.append({"direction": direction, "node": _item(other), "label": str(getattr(triple, "link_label", "") or ""), "type": str(getattr(kind, "value", kind))})
    return {"node": _item(node), "links": items}
def walk(root_uuid: str, direction: str, depth: int) -> dict[str, Any]:
    if direction not in {"ancestors", "descendants"}: raise WorkerDomainError("invalid_argument", "direction must be ancestors or descendants")
    root = _node(root_uuid); nodes = {str(root.uuid): root}; edges = []; queue = deque([(root, 0)])
    while queue:
        node, distance = queue.popleft()
        if distance >= max(0, min(int(depth), 6)): continue
        manager = node.base.links.get_incoming() if direction == "ancestors" else node.base.links.get_outgoing()
        for triple in manager.all():
            other = triple.node; other_uuid = str(other.uuid); kind = getattr(triple, "link_type", "")
            edges.append({"source_uuid": other_uuid if direction == "ancestors" else str(node.uuid), "target_uuid": str(node.uuid) if direction == "ancestors" else other_uuid, "label": str(getattr(triple, "link_label", "") or ""), "type": str(getattr(kind, "value", kind))})
            if other_uuid not in nodes: nodes[other_uuid] = other; queue.append((other, distance + 1))
    return {"root_uuid": str(root.uuid), "direction": direction, "depth": max(0, min(int(depth), 6)), "nodes": [_item(node) for node in nodes.values()], "links": edges}
