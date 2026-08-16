#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MCP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PORT="${AIIDA_MCP_PORT:-8043}"
URL="http://127.0.0.1:${PORT}"
PID_FILE="${TMPDIR:-/tmp}/aiida-mcp.pid"
LOG_FILE="${TMPDIR:-/tmp}/aiida-mcp.log"
PYTHON_BIN="${AIIDA_MCP_SERVER_PYTHON:-${MCP_DIR}/.venv/bin/python}"

is_ready() { curl --silent --fail --max-time 2 "${URL}/api/ready" >/dev/null 2>&1; }
start() {
  if is_ready; then echo "AiiDA Manager is ready at ${URL}"; return; fi
  if curl --silent --max-time 2 "${URL}/" >/dev/null 2>&1; then
    echo "Port ${PORT} is occupied by an older or unrelated local service. Stop it, then launch AiiDA Manager again." >&2
    exit 1
  fi
  if [[ ! -x "${PYTHON_BIN}" ]]; then echo "Missing ${PYTHON_BIN}. Run: (cd ${MCP_DIR} && uv sync)" >&2; exit 1; fi
  PYTHONPATH="${MCP_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    nohup "${PYTHON_BIN}" -m aiida_mcp.server >"${LOG_FILE}" 2>&1 &
  echo "$!" >"${PID_FILE}"
  for _ in {1..30}; do
    if is_ready; then echo "AiiDA Manager is ready at ${URL}"; return; fi
    sleep 1
  done
  echo "AiiDA Manager did not start. See ${LOG_FILE}" >&2; exit 1
}
stop() {
  if [[ -f "${PID_FILE}" ]]; then
    pid="$(<"${PID_FILE}")"
    if kill -0 "${pid}" 2>/dev/null; then kill "${pid}"; fi
    rm -f "${PID_FILE}"
  fi
}
case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) is_ready && echo "online ${URL}" || echo "offline ${URL}" ;;
  *) echo "Usage: $0 {start|stop|status}" >&2; exit 2 ;;
esac