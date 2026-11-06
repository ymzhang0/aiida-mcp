from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SSHHostDetails(BaseModel):
    alias: str
    hostname: str | None = None
    username: str | None = None
    port: int | None = None
    proxy_jump: str | None = None
    proxy_command: str | None = None
    identity_file: str | None = None


class InfrastructureCapabilitiesResponse(BaseModel):
    aiida_core_version: str
    available_transports: list[str] = Field(default_factory=list)
    recommended_transport: str = "core.ssh"
    supports_async_ssh: bool = False
    transport_auth_fields: dict[str, list[str]] = Field(default_factory=dict)


class CodeSetupRequest(BaseModel):
    computer_label: str
    label: str
    description: str | None = None
    default_calc_job_plugin: str
    remote_abspath: str
    prepend_text: str | None = None
    append_text: str | None = None
    with_mpi: bool | None = None
    use_double_quotes: bool = False


class InfrastructureSetupRequest(BaseModel):
    # Computer fields
    computer_label: str = Field(..., min_length=1, max_length=255)
    hostname: str = Field(..., min_length=1)
    computer_description: str | None = None
    transport_type: str = "core.ssh"
    scheduler_type: str = "core.direct"
    shebang: str = "#!/bin/bash"
    work_dir: str = "/tmp/aiida"
    mpiprocs_per_machine: int = 1
    mpirun_command: str = "mpirun -np {tot_num_mpiprocs}"
    default_memory_per_machine: int | None = None
    use_double_quotes: bool = False
    prepend_text: str | None = None
    append_text: str | None = None

    # Auth fields
    user: str | None = None
    username: str | None = None
    port: int | None = None
    look_for_keys: bool | None = None
    key_filename: str | None = None
    timeout: int | None = None
    connection_timeout: int | None = None
    allow_agent: bool | None = None
    compress: bool | None = None
    gss_auth: bool | None = None
    gss_kex: bool | None = None
    gss_deleg_creds: bool | None = None
    gss_host: str | None = None
    load_system_host_keys: bool | None = None
    key_policy: str | None = None
    proxy_command: str | None = None
    proxy_jump: str | None = None
    safe_interval: float | None = None
    use_login_shell: bool = True
    host: str | None = None
    max_io_allowed: int | None = None
    authentication_script: str | None = None
    backend: str | None = None

    # Reconfigure an existing computer with the same label instead of refusing.
    allow_update: bool = False

    # Code fields
    code_label: str | None = None
    code_description: str | None = None
    default_calc_job_plugin: str | None = None
    remote_abspath: str | None = None
    code_prepend_text: str | None = None
    code_append_text: str | None = None


class InfrastructureExportResponse(BaseModel):
    kind: str
    label: str
    filename: str
    format: str = "yaml"
    content: str
