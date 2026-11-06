---
name: aiida
description: Work with the user's AiiDA profile through the aiida MCP tools - check on calculations and workflows, find out why one failed, find a workflow and submit it, kill/pause/resume processes, or set up a cluster (computer) and register codes. Use when the user mentions AiiDA, verdi, a process PK, a WorkChain or CalcJob, a calculation or job they ran, or asks to run or submit a simulation (Quantum ESPRESSO, EPW, Wannier90, ...), or to configure a cluster or code for AiiDA.
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

## Setting up a cluster and codes

Registering a code means telling AiiDA where an existing executable is. These
tools do not compile or install software.

1. `aiida_computer_list` shows what already exists. Reuse an existing computer
   when it fits. Existing computers and codes cannot be changed here.
2. `aiida_ssh_hosts` lists the user's SSH aliases. For `core.ssh_async`, set
   `host` to the alias so the user's ~/.ssh/config (ProxyJump, key) applies.
   For `core.ssh`, copy `username`, `proxy_jump`, and `key_filename`. Never ask
   for or read private keys or passwords.
3. Ask the user for whatever you cannot know: the scheduler (usually
   `core.slurm`), a scratch `work_dir` (`{username}` is substituted), the MPI
   launcher (`srun` or `mpirun -np {tot_num_mpiprocs}`), cores per node, and
   any `prepend_text` (modules, `export` lines). If the cluster has a
   verdi-style YAML (for example from aiida-code-registry), pass it as
   `setup_yaml` / `configure_yaml`.
4. **Confirm the settings with the user**, then call `aiida_computer_create`.
   It tests the connection first and stores nothing if the test fails. A
   `Permission denied` error usually means the key needs an ssh-agent or a
   passphrase-less key. Tell the user; do not retry blindly.
5. `aiida_code_check` with the executable name (e.g. `pw.x`) and the module
   loads in `prepend_text` (e.g. `module load QuantumESPRESSO/7.2`). Iterate
   with the user until it is found.
6. **Confirm**, then call `aiida_code_create` with `default_calc_job_plugin`
   (e.g. `quantumespresso.pw`), `with_mpi`, and the same `prepend_text`. It
   re-checks the executable and stores the resolved absolute path.
7. Before the first real submission, suggest a tiny test calculation.

## Limits

- Submission needs the AiiDA daemon. If `aiida_runtime_status` reports
  `daemon_status: false`, ask the user to run `verdi daemon start`.
- If submission tools are missing, the server runs in read-only mode.
