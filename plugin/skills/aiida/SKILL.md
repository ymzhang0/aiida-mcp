---
name: aiida
description: Work with the user's AiiDA profile through the aiida MCP tools - check on calculations and workflows, find out why one failed, find a workflow and submit it, or kill/pause/resume processes. Use when the user mentions AiiDA, verdi, a process PK, a WorkChain or CalcJob, a calculation or job they ran, or asks to run or submit a simulation (Quantum ESPRESSO, EPW, Wannier90, ...).
---

# Working with AiiDA

The `aiida` MCP server is bound to one Python environment and one AiiDA profile,
chosen when the plugin was configured. Call `aiida_runtime_status` first if you
are unsure which profile is active or whether the daemon is running.

## Checking on work

1. `aiida_process_list`: newest first. Filter with `process_state`
   (`created`, `waiting`, `running`, `finished`, `excepted`, `killed`), `label`
   (a substring of the process label, or a PK), or `days`. The default lists only
   top-level processes; pass `root_only: false` to include sub-processes.
2. `aiida_process_get <pk>`: state, exit status, inputs, outputs, called
   sub-processes.
3. For a failure, use `analyser_process_report <pk>` if available (with
   `include_outputs: true` for stdout/stderr excerpts). It walks the sub-process
   tree to the failing calculation and explains why it failed. Otherwise use
   `aiida_process_logs` on the failing sub-process.

`finished` with `exit_status` 0 means success. `finished` with a non-zero exit
status is a failure handled by the workflow. `excepted` means the process
crashed. Always report the exit status, not only the state.

## Running work

Always follow this order:

1. `aiida_workflow_list` to find the entry point (e.g. `quantumespresso.pw.base`).
2. `aiida_workflow_spec` to read its inputs. Calculations such as
   `core.arithmetic.add` work too.
3. Resolve the inputs:
   - Codes: `aiida_code_list`, then pass the `full_label` (`pw@cluster`).
   - Existing structures and other nodes: `aiida_workflow_input_candidates` with
     the `port_path` (e.g. `pw.structure`), then pass `{"pk": N}`.
   - Base values are plain JSON: `2` becomes Int, `"x"` becomes Str, and
     `{"ecutwfc": 40}` becomes Dict.
   - If the workflow lists `protocols`, prefer `protocol` + `protocol_arguments`
     (e.g. `{"code": "pw@cluster", "structure": {"pk": 12}}`) over hand-written
     `inputs`.
   - Calculations need `metadata.options.resources`, e.g.
     `{"num_machines": 1, "num_mpiprocs_per_machine": 1}`. HPC schedulers usually
     also need `max_wallclock_seconds`, and often `account` or `queue_name`.
4. `aiida_submission_draft` with exactly the arguments you intend to submit. It
   validates without storing anything. Fix every reported error before
   continuing.
5. **Show the user the entry point, the code/computer, the key inputs, and the
   resources, and get their confirmation.** Submitting uses real compute time.
6. `aiida_submit` with the same arguments, then report the returned PK.
7. To follow progress, call `aiida_process_get` again later. Do not poll in a
   tight loop. HPC jobs can wait in the queue for hours.

## Controlling work

`aiida_process_kill` is irreversible. Confirm the PKs with the user first, and
say whether they are top-level workflows (killing them kills their children).
`aiida_process_pause` / `aiida_process_play` are safe to undo.

## Limits

- Submission needs the AiiDA daemon. If `aiida_runtime_status` reports
  `daemon_status: false`, ask the user to run `verdi daemon start`.
- These tools do not set up computers or codes. Ask the user to do that with
  `verdi computer setup` / `verdi code create`.
- If submission tools are missing, the server runs in read-only mode.
