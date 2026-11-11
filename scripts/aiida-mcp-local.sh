#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MCP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PORT="${AIIDA_MCP_PORT:-8043}"
URL="http://127.0.0.1:${PORT}"
# Per-user state directory: a fixed path in /tmp can be pre-created or read by other users.
STATE_DIR="${AIIDA_MCP_STATE_DIR:-${XDG_STATE_HOME:-${HOME}/.local/state}/aiida-mcp}"
mkdir -p "${STATE_DIR}"; chmod 700 "${STATE_DIR}"
PID_FILE="${STATE_DIR}/aiida-mcp-${PORT}.pid"
LOG_FILE="${STATE_DIR}/aiida-mcp-${PORT}.log"
PYTHON_BIN="${AIIDA_MCP_SERVER_PYTHON:-${MCP_DIR}/.venv/bin/python}"

is_ready() { curl --silent --fail --max-time 2 "${URL}/api/ready" >/dev/null 2>&1; }
start() {
  if [[ -z "${AIIDA_MCP_PYTHON:-}" || -z "${AIIDA_MCP_PROFILE:-}" ]]; then
    echo "Set AIIDA_MCP_PYTHON and AIIDA_MCP_PROFILE before starting aiida-mcp." >&2; exit 2
  fi
  if is_ready; then echo "AiiDA MCP is ready at ${URL}"; return; fi
  if curl --silent --max-time 2 "${URL}/" >/dev/null 2>&1; then
    echo "Port ${PORT} is occupied by an older or unrelated local service. Stop it, then launch AiiDA MCP again." >&2
    exit 1
  fi
  if [[ ! -x "${PYTHON_BIN}" ]]; then echo "Missing ${PYTHON_BIN}. Run: (cd ${MCP_DIR} && uv sync)" >&2; exit 1; fi
  PYTHONPATH="${MCP_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    nohup "${PYTHON_BIN}" -m aiida_mcp serve >"${LOG_FILE}" 2>&1 &
  echo "$!" >"${PID_FILE}"
  for _ in {1..30}; do
    if is_ready; then echo "AiiDA MCP is ready at ${URL}"; return; fi
    sleep 1
  done
  echo "AiiDA MCP did not start. See ${LOG_FILE}" >&2; exit 1
}
stop() {
  if [[ -f "${PID_FILE}" ]]; then
    pid="$(<"${PID_FILE}")"
    # The PID may have been reused since the file was written: only kill our own server.
    if [[ "${pid}" =~ ^[0-9]+$ ]] && ps -p "${pid}" -o args= 2>/dev/null | grep -q "aiida_mcp serve"; then
      kill "${pid}"
    elif [[ -n "${pid}" ]]; then
      echo "PID ${pid} is not a running aiida-mcp server; removing the stale PID file." >&2
    fi
    rm -f "${PID_FILE}"
  fi
}
case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) is_ready && echo "online ${URL}" || echo "offline ${URL}" ;;
  *) echo "Usage: $0 {start|stop|status}" >&2; exit 2 ;;
esac