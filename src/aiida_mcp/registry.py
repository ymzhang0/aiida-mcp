"""Persistent, Manager-owned AiiDA project bindings.

The registry deliberately models scientific ownership outside ChatGPT.  A
ChatGPT Project may store a ``project_ref`` in its instructions, but the
reference is resolved and authorized by this service before any worker call.
"""

from __future__ import annotations

import json
import os
import secrets
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


def _now() -> str:
    return datetime.now(UTC).isoformat()


def default_registry_path() -> Path:
    configured = os.environ.get("AIIDA_MCP_REGISTRY_FILE")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".aiida" / "aiida-mcp-projects.json"


@dataclass(frozen=True)
class AiiDAProject:
    """The one durable binding between a research context and AiiDA data."""

    id: str
    project_ref: str
    name: str
    aiida_profile: str
    python_interpreter_path: str
    group_uuid: str
    group_label: str
    workspace_path: str | None
    database_fingerprint: str | None
    chatgpt_external_project_id: str | None
    created_at: str
    updated_at: str
    archived: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AiiDAProject":
        return cls(
            id=str(value["id"]),
            project_ref=str(value["project_ref"]),
            name=str(value["name"]),
            aiida_profile=str(value["aiida_profile"]),
            python_interpreter_path=str(value["python_interpreter_path"]),
            group_uuid=str(value["group_uuid"]),
            group_label=str(value.get("group_label") or ""),
            workspace_path=str(value["workspace_path"]) if value.get("workspace_path") else None,
            database_fingerprint=(
                str(value["database_fingerprint"])
                if value.get("database_fingerprint")
                else None
            ),
            chatgpt_external_project_id=(
                str(value["chatgpt_external_project_id"])
                if value.get("chatgpt_external_project_id")
                else None
            ),
            created_at=str(value["created_at"]),
            updated_at=str(value.get("updated_at") or value["created_at"]),
            archived=bool(value.get("archived", False)),
        )

    def public_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result.pop("python_interpreter_path", None)
        result.pop("workspace_path", None)
        result.pop("database_fingerprint", None)
        return result


class ProjectRegistry:
    """Small JSON registry with atomic writes and immutable project refs."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path).expanduser() if path else default_registry_path()

    def list_projects(self, *, include_archived: bool = False) -> list[AiiDAProject]:
        projects = self._read()
        if not include_archived:
            projects = [project for project in projects if not project.archived]
        return sorted(projects, key=lambda project: project.name.casefold())

    def get(self, project_ref: str) -> AiiDAProject | None:
        cleaned = str(project_ref or "").strip()
        return next(
            (item for item in self._read() if item.project_ref == cleaned or item.id == cleaned),
            None,
        )

    def create(
        self,
        *,
        name: str,
        aiida_profile: str,
        python_interpreter_path: str,
        group_uuid: str,
        group_label: str = "",
        workspace_path: str | None = None,
        database_fingerprint: str | None = None,
        chatgpt_external_project_id: str | None = None,
    ) -> AiiDAProject:
        required = {
            "name": name,
            "aiida_profile": aiida_profile,
            "python_interpreter_path": python_interpreter_path,
            "group_uuid": group_uuid,
        }
        missing = [field for field, value in required.items() if not str(value or "").strip()]
        if missing:
            raise ValueError(f"Missing project fields: {', '.join(missing)}")
        projects = self._read()
        normalized_group = str(group_uuid).strip()
        if any(item.group_uuid == normalized_group and item.aiida_profile == str(aiida_profile).strip() for item in projects):
            raise ValueError("This AiiDA Group is already bound for the selected profile")
        timestamp = _now()
        project = AiiDAProject(
            id=uuid4().hex,
            project_ref=f"sp_{secrets.token_urlsafe(18)}",
            name=str(name).strip(),
            aiida_profile=str(aiida_profile).strip(),
            python_interpreter_path=str(Path(python_interpreter_path).expanduser()),
            group_uuid=normalized_group,
            group_label=str(group_label or "").strip(),
            workspace_path=str(Path(workspace_path).expanduser()) if workspace_path else None,
            database_fingerprint=str(database_fingerprint).strip() if database_fingerprint else None,
            chatgpt_external_project_id=(
                str(chatgpt_external_project_id).strip() if chatgpt_external_project_id else None
            ),
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._write([*projects, project])
        return project

    def archive(self, project_ref: str) -> AiiDAProject:
        projects = self._read()
        cleaned = str(project_ref or "").strip()
        updated: list[AiiDAProject] = []
        target: AiiDAProject | None = None
        for project in projects:
            if project.project_ref != cleaned and project.id != cleaned:
                updated.append(project)
                continue
            target = AiiDAProject(**{**asdict(project), "archived": True, "updated_at": _now()})
            updated.append(target)
        if target is None:
            raise KeyError(cleaned)
        self._write(updated)
        return target

    def _read(self) -> list[AiiDAProject]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot read AiiDA project registry: {self.path}") from exc
        raw_projects = payload.get("projects", []) if isinstance(payload, dict) else []
        return [AiiDAProject.from_dict(value) for value in raw_projects if isinstance(value, dict)]

    def _write(self, projects: list[AiiDAProject]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({"schema_version": 1, "projects": [asdict(project) for project in projects]}, indent=2)
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.path)
