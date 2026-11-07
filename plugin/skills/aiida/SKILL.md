---
name: aiida
description: Work with the user's AiiDA profile through the aiida MCP tools - check on calculations and workflows, find out why one failed, find a structure in MC3D/MC2D/Materials Project, submit workflows, kill/pause/resume processes, or set up a cluster (computer) and register codes. Use when the user mentions AiiDA, verdi, a process PK, a WorkChain or CalcJob, a calculation or job they ran, or asks to run or submit a simulation (Quantum ESPRESSO, EPW, Wannier90, ...), or to configure a cluster or code for AiiDA.
---

# Working with AiiDA

The tools are bound to one AiiDA profile. Work in as few calls as possible:
skip lookups for anything the user already gave you (code label, structure,
parameters), and call independent tools in the same turn.

## Check on work
- `list_processes` (filter by `process_state`, `label`, `days`; `root_only: false` includes children), then `get_process` (compact; `detail="full"` only if something is missing).
- A failure: `diagnose_process` if available, otherwise `get_process_logs` on the failing child. `finished` with exit 0 means success, non-zero means a handled failure, `excepted` means a crash. Always report the exit status and the reason (`exit_message`, `process_status`).

## Run work
1. Structure: `search_structures` (`mc3d` = AiiDA-relaxed PBE bulk, sorted by energy per atom; `mc2d` = 2D layers; `mp`), then `import_structure`; or `create_structure` (`bulk`, a project `file`, or `cell` + `sites`); or an existing PK.
2. Inputs: for a workflow with protocols, use `protocol` + `protocol_arguments` (e.g. `{"code": "pw@cluster", "structure": {"pk": N}, "options": {"resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1}, "queue_name": "...", "max_wallclock_seconds": 600, "withmpi": false}}`), plus `overrides` such as `{"pseudo_family": "SSSP/1.3/PBE/efficiency", "pw": {"parameters": {"SYSTEM": {"ecutrho": 360}}}}`. Multi-step workflows (e.g. aiida-epw) take node references, dicts of codes, and enum names as strings in `protocol_arguments`; use `calcjob_options` to give every calculation the same resources and `builder_updates` for dotted-path changes to sub-steps. Call `get_workflow_spec` only when you do not know the inputs. Base values are plain JSON, stored nodes are `{"pk": N}`, and codes are `label@computer`.
3. **Confirm the entry point, code, key inputs, and resources with the user** unless they already confirmed them.
4. `submit` with a `label` and a `group` that keeps related runs together. `submit` validates the inputs itself. For a job expected to finish within minutes, pass `wait_seconds` (e.g. 900): one call returns the result, with the energy in `outputs.output_parameters.value`. For longer jobs, report the PK and use `wait_for_process` later, which also returns the result.
5. If it failed, diagnose it and explain the cause in physical terms. Propose one concrete fix and confirm it before resubmitting.

## Control work
Before `kill_processes`, confirm the PKs with the user; killing a workflow kills its children. `pause_processes` and `resume_processes` can be undone.

## Clusters and codes (create-only)
`list_computers`, then `list_ssh_hosts` (for `core.ssh_async` set `host` to the alias; never read keys or passwords). Ask the user for the scheduler, `work_dir`, MPI launcher, cores per node, and modules. **Confirm**, then `create_computer`: it tests the connection and stores nothing on failure (`Permission denied` usually means an ssh-agent is needed). `check_code` with the module loads in `prepend_text`, then **confirm** and `create_code`.

## Limits
If `get_status` shows `daemon_status: false`, or a `daemon_warning`, ask for `verdi daemon start` or `verdi daemon restart` before submitting. Missing write tools mean the server is in read-only mode.
