from __future__ import annotations

from typing import Any

from aiida_mcp.core.engine import active_profile_name

def _get_statistics_payload() -> dict[str, Any]:
    from aiida_mcp.core.engine import _collect_system_counts, _is_daemon_running

    counts = _collect_system_counts()
    return {
        "profile": active_profile_name(),
        "daemon_status": _is_daemon_running(),
        "counts": counts,
    }



def _get_database_summary_payload() -> dict[str, Any]:
    from aiida_mcp.core.engine import _collect_system_counts

    counts = _collect_system_counts()
    return {
        "status": "success",
        "profile": active_profile_name(),
        "node_count": counts["nodes"],
        "process_count": counts["processes"],
        "failed_count": counts["failed_processes"],
    }
