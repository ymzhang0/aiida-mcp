# AiiDA plugin for Claude Code

This plugin lets an agent inspect, submit, and manage your AiiDA processes. It
bundles the `aiida-mcp` server and a skill that teaches the agent the
check → draft → confirm → submit workflow.

## Install

1. Install `aiida-mcp` into the Python environment that has AiiDA and your
   plugins (for example aiida-quantumespresso):

   ```bash
   /path/to/project/.venv/bin/pip install "aiida-mcp @ git+https://github.com/ymzhang0/aiida-mcp.git"
   ```

2. In Claude Code:

   ```
   /plugin marketplace add ymzhang0/aiida-mcp
   /plugin install aiida@aiida-mcp
   ```

   When prompted, enter the path to that Python interpreter and your AiiDA
   profile name. To change them later, run
   `claude plugin configure aiida@aiida-mcp`.

3. Start the daemon if you want to submit work: `verdi daemon start`.

## Options

| Option | Meaning |
| --- | --- |
| `python` | Interpreter with AiiDA and `aiida-mcp` installed |
| `profile` | AiiDA profile name (`verdi profile list`) |
| `read_only` | `true` hides submit/kill/pause/play |

Extra tools, such as process failure diagnosis, appear automatically when a
package that provides `aiida_mcp.tools` (for example `aiida-analyser`) is
installed in the same environment.
