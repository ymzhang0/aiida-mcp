"""Generic local HTTP transport for the configured AiiDA runtime."""
from __future__ import annotations
import logging
from typing import Any
from fastapi import APIRouter, HTTPException
from .runtime import WorkerRuntimeError
from .service import AiiDAService


log = logging.getLogger(__name__)


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, WorkerRuntimeError): return HTTPException(exc.status_code, {"message": str(exc), **exc.detail})
    if isinstance(exc, ValueError): return HTTPException(422, str(exc))
    # Unexpected failures are logged here; their text may hold paths or internals the client should not see.
    log.error("Unhandled error in the AiiDA API", exc_info=exc)
    return HTTPException(500, "Internal server error")


def build_router(service: AiiDAService) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["aiida"])
    @router.get("/ready")
    async def ready() -> dict[str, str]: return {"status": "ready"}
    @router.get("/runtime/status")
    async def status() -> dict[str, Any]:
        try: return await service.status()
        except Exception as exc: raise _error(exc) from exc
    @router.get("/nodes")
    async def nodes(limit: int = 50, node_type: str | None = None) -> dict[str, Any]:
        try: return await service.nodes(limit=limit, node_type=node_type)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/nodes/{identifier}/links")
    async def node_links(identifier: str) -> dict[str, Any]:
        try: return await service.node_links(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/nodes/compare")
    async def node_compare(left: str, right: str, depth: int = 2, max_differences: int = 100) -> dict[str, Any]:
        try: return await service.node_compare(left, right, depth=depth, max_differences=max_differences)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/nodes/{pk}")
    async def node_get(pk: int) -> dict[str, Any]:
        try: return await service.node_get(pk)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/processes/{identifier}")
    async def process_get(identifier: str) -> dict[str, Any]:
        try: return await service.process_get(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/processes/{identifier}/workgraph")
    async def process_workgraph(identifier: str) -> dict[str, Any]:
        try: return await service.process_workgraph(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/processes/{identifier}/logs")
    async def process_logs(identifier: str) -> dict[str, Any]:
        try: return await service.process_logs(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/groups")
    async def groups() -> dict[str, Any]:
        try: return await service.group_list()
        except Exception as exc: raise _error(exc) from exc
    @router.get("/groups/inspect")
    async def group_inspect(identifier: str) -> dict[str, Any]:
        try: return await service.group_get(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/groups/{identifier}")
    async def group_get(identifier: str) -> dict[str, Any]:
        try: return await service.group_get(identifier)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/provenance/ancestors")
    async def provenance_ancestors(root_uuid: str, depth: int = 2) -> dict[str, Any]:
        try: return await service.provenance_walk(root_uuid, direction="ancestors", depth=depth)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/provenance/descendants")
    async def provenance_descendants(root_uuid: str, depth: int = 2) -> dict[str, Any]:
        try: return await service.provenance_walk(root_uuid, direction="descendants", depth=depth)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/provenance/roots")
    async def provenance_roots(group_uuid: str, limit: int = 100) -> dict[str, Any]:
        try: return await service.provenance_roots(group_uuid, limit=limit)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/provenance/subgraph")
    async def provenance_subgraph(group_uuid: str, root_uuid: str, depth: int = 2) -> dict[str, Any]:
        try: return await service.provenance_subgraph(group_uuid, root_uuid, depth=depth)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/provenance/nodes/{uuid}")
    async def provenance_node_detail(uuid: str, group_uuid: str) -> dict[str, Any]:
        try: return await service.provenance_node_detail(group_uuid, uuid)
        except Exception as exc: raise _error(exc) from exc
    @router.get("/archives/inspect")
    async def archive_inspect(path: str) -> dict[str, Any]:
        try: return await service.archive_inspect(path)
        except Exception as exc: raise _error(exc) from exc
    return router
