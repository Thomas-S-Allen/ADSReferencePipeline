#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR_REL="Reference/logs/benchmarks/attached_reference_benchmark_runs"
CONTAINER_APP_ROOT="${CONTAINER_APP_ROOT:-/app}"
INPUT_PATH="${INPUT_PATH:-${CONTAINER_APP_ROOT}/adsrefpipe/tests/unittests/stubdata}"
EXTENSIONS="*.raw,*.xml,*.txt,*.html,*.tex,*.refs,*.pairs"
DAYS_BACK=""
MODE="mock"
ASYNC_MODE="enqueue-only"
MAX_FILES=""
TIMEOUT="900"
READINESS_TIMEOUT="180"
RUNNER_PATH="${RUNNER_PATH:-${CONTAINER_APP_ROOT}/scripts/run-in-container-benchmark.bash}"
TARGET_CONTAINER="reference_pipeline"
SYSTEM_SAMPLE_INTERVAL="1.0"
GROUP_BY="source_type"
DISABLE_SYSTEM_LOAD="false"
PROGRESS="true"

usage() {
  cat <<'USAGE'
Usage: ./run-attached-reference-benchmark.bash [options]

Options:
  --container NAME               Target reference container name
  --input-path PATH              Input file or directory inside the target container
  --extensions CSV               Comma-separated file patterns
  --days-back N                  Only include files modified within the last N days
  --max-files N                  Optional file cap
  --mode real|mock               Benchmark mode
  --async-mode enqueue-only|end-to-end
  --timeout N                    Benchmark timeout in seconds
  --readiness-timeout N          Container readiness timeout in seconds
  --output-dir RELPATH           Host output directory relative to wrapper root
  --runner-path PATH             In-container runner path
  --system-sample-interval FLOAT Sampling interval in seconds
  --group-by source_type|parser|none
  --disable-system-load          Disable in-container system sampling
  --no-progress                  Disable live compact progress lines
  --help                         Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container|--target)
      TARGET_CONTAINER="$2"
      shift 2
      ;;
    --input-path)
      INPUT_PATH="$2"
      shift 2
      ;;
    --extensions)
      EXTENSIONS="$2"
      shift 2
      ;;
    --days-back)
      DAYS_BACK="$2"
      shift 2
      ;;
    --max-files)
      MAX_FILES="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --async-mode)
      ASYNC_MODE="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --readiness-timeout)
      READINESS_TIMEOUT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR_REL="$2"
      shift 2
      ;;
    --runner-path)
      RUNNER_PATH="$2"
      shift 2
      ;;
    --system-sample-interval)
      SYSTEM_SAMPLE_INTERVAL="$2"
      shift 2
      ;;
    --group-by)
      GROUP_BY="$2"
      shift 2
      ;;
    --disable-system-load)
      DISABLE_SYSTEM_LOAD="true"
      shift
      ;;
    --no-progress)
      PROGRESS="false"
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

RUN_STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
OUTPUT_BASE_DIR="${SCRIPT_DIR}/${OUTPUT_DIR_REL}"
OUTPUT_DIR="${OUTPUT_BASE_DIR}/run_${RUN_STAMP}"
RUN_LOG_DIR="${OUTPUT_DIR}/run_logs"
MANIFEST_PATH="${OUTPUT_DIR}/attached_benchmark_manifest.json"
SUMMARY_PATH="${OUTPUT_DIR}/attached_benchmark_manifest.md"
WRAPPER_LOG="${OUTPUT_DIR}/attached_benchmark.log"
HOST_CONTEXT_PATH="${RUN_LOG_DIR}/host_context.json"
CONTAINER_STDOUT_PATH="${RUN_LOG_DIR}/container_runner.stdout.log"
PROGRESS_LOG_PATH="${RUN_LOG_DIR}/attached_${RUN_STAMP}.stdout.log"
PROGRESS_STOP_PATH="${RUN_LOG_DIR}/progress_display.done"
RESULT_PATH_HOST="${RUN_LOG_DIR}/attached_${RUN_STAMP}.result.json"
mkdir -p "${OUTPUT_DIR}" "${RUN_LOG_DIR}"
: > "${WRAPPER_LOG}"
rm -f "${PROGRESS_STOP_PATH}"

log() {
  local message="$1"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[${timestamp}] ${message}" | tee -a "${WRAPPER_LOG}"
}

check_docker_access() {
  docker info >/dev/null 2>&1
}

check_target_container() {
  docker inspect --format '{{.State.Running}}' "${TARGET_CONTAINER}" 2>/dev/null | grep -q true
}

wait_for_container_readiness() {
  local deadline=$(( $(date +%s) + READINESS_TIMEOUT ))
  while [[ $(date +%s) -lt ${deadline} ]]; do
    if docker exec "${TARGET_CONTAINER}" python3 -c "import os; import sys; target=os.environ.get('INPUT_PATH', '${INPUT_PATH}'); sys.exit(0 if os.path.exists(target) else 1)" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

capture_host_context() {
  local output_path="$1"
  python3 - "${output_path}" <<'PY'
import json
import re
import subprocess
import sys
from datetime import datetime, timezone

output_path = sys.argv[1]

def run_command(args):
    try:
        completed = subprocess.run(args, capture_output=True, text=True, check=False)
        return {
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:
        return {
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
        }

def parse_load(stdout):
    match = re.search(r"load averages?: ([0-9.]+)[, ]+([0-9.]+)[, ]+([0-9.]+)", stdout)
    if not match:
        return None
    return {
        "load_1m": float(match.group(1)),
        "load_5m": float(match.group(2)),
        "load_15m": float(match.group(3)),
    }

def parse_memory(stdout):
    page_size = 4096
    page_match = re.search(r"page size of (\d+) bytes", stdout)
    if page_match:
        page_size = int(page_match.group(1))
    values = {}
    for key in ["Pages free", "Pages inactive", "Pages speculative"]:
        match = re.search(rf"{re.escape(key)}:\s+(\d+)\.", stdout)
        if match:
            values[key] = int(match.group(1))
    if not values:
        return None
    available_pages = sum(values.values())
    return {
        "page_size_bytes": page_size,
        "pages_free": values.get("Pages free"),
        "pages_inactive": values.get("Pages inactive"),
        "pages_speculative": values.get("Pages speculative"),
        "available_bytes_estimate": available_pages * page_size,
    }

payload = {
    "captured_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}

uptime_result = run_command(["uptime"])
payload["cpu_load"] = {
    "raw": uptime_result["stdout"],
    "parse": parse_load(uptime_result["stdout"]),
}

vm_stat_result = run_command(["vm_stat"])
payload["free_memory"] = {
    "raw": vm_stat_result["stdout"],
    "parse": parse_memory(vm_stat_result["stdout"]),
}

docker_df_result = run_command(["docker", "system", "df"])
payload["docker_disk_usage"] = docker_df_result

with open(output_path, "w") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

resolve_app_mount() {
  docker inspect --format '{{range .Mounts}}{{if eq .Destination "'"${CONTAINER_APP_ROOT}"'"}}{{println .Source}}{{end}}{{end}}' "${TARGET_CONTAINER}" 2>/dev/null | awk 'NF {print; exit}'
}

APP_HOST_DIR="$(resolve_app_mount || true)"
CONTAINER_OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${CONTAINER_APP_ROOT}/logs/benchmarks/attached_reference_benchmark_runs}"
CONTAINER_LABEL="attached_${RUN_STAMP}"
LOGS_HOST_DIR="${SCRIPT_DIR}/Reference/logs"

if ! check_docker_access; then
  log "Docker daemon is not reachable from this shell"
  exit 1
fi

if ! check_target_container; then
  log "Target container is not running: ${TARGET_CONTAINER}"
  exit 1
fi

if ! wait_for_container_readiness; then
  log "Target container did not become benchmark-ready within ${READINESS_TIMEOUT}s"
  exit 1
fi

capture_host_context "${HOST_CONTEXT_PATH}"

CONTAINER_COMMAND="${RUNNER_PATH} --input-path '${INPUT_PATH}' --extensions '${EXTENSIONS}' --mode '${MODE}' --async-mode '${ASYNC_MODE}' --timeout '${TIMEOUT}' --output-dir '${CONTAINER_OUTPUT_DIR}' --label '${CONTAINER_LABEL}' --run-stamp '${RUN_STAMP}' --system-sample-interval '${SYSTEM_SAMPLE_INTERVAL}' --group-by '${GROUP_BY}'"
if [[ -n "${DAYS_BACK}" ]]; then
  CONTAINER_COMMAND+=" --days-back '${DAYS_BACK}'"
fi
if [[ -n "${MAX_FILES}" ]]; then
  CONTAINER_COMMAND+=" --max-files '${MAX_FILES}'"
fi
if [[ "${DISABLE_SYSTEM_LOAD}" == "true" ]]; then
  CONTAINER_COMMAND+=" --disable-system-load"
fi
if [[ "${PROGRESS}" == "false" ]]; then
  CONTAINER_COMMAND+=" --no-progress"
fi

PROGRESS_PID=""
if [[ "${PROGRESS}" == "true" && -n "${APP_HOST_DIR}" ]]; then
  python3 - "${PROGRESS_LOG_PATH}" "${PROGRESS_STOP_PATH}" "${APP_HOST_DIR}" <<'PY' &
import os
import sys
import time

log_path = sys.argv[1]
stop_path = sys.argv[2]
app_host_dir = sys.argv[3]
offset = 0
seen_log_lines = set()

if app_host_dir:
    sys.path.insert(0, app_host_dir)

from adsrefpipe import perf_metrics


def process_available_lines():
    global offset
    if not os.path.exists(log_path):
        return False
    with open(log_path, "r", errors="replace") as handle:
        handle.seek(offset)
        lines = handle.readlines()
        offset = handle.tell()
    for line in lines:
        progress = perf_metrics.format_benchmark_progress_line_from_log_line(line)
        if progress and line not in seen_log_lines:
            seen_log_lines.add(line)
            print(progress, flush=True)
    return bool(lines)


while True:
    saw_lines = process_available_lines()
    if os.path.exists(stop_path):
        process_available_lines()
        break
    if not saw_lines:
        time.sleep(0.5)
PY
  PROGRESS_PID=$!
fi

log "Running in-container benchmark via ${TARGET_CONTAINER}"
if docker exec "${TARGET_CONTAINER}" bash -lc "${CONTAINER_COMMAND}" > "${CONTAINER_STDOUT_PATH}" 2>> "${WRAPPER_LOG}"; then
  CONTAINER_EXIT_CODE=0
else
  CONTAINER_EXIT_CODE=$?
fi

if [[ -n "${PROGRESS_PID}" ]]; then
  : > "${PROGRESS_STOP_PATH}"
  wait "${PROGRESS_PID}" || true
  rm -f "${PROGRESS_STOP_PATH}"
fi

if ! python3 - "${RESULT_PATH_HOST}" "${CONTAINER_STDOUT_PATH}" "${MANIFEST_PATH}" "${SUMMARY_PATH}" "${HOST_CONTEXT_PATH}" "${TARGET_CONTAINER}" "${APP_HOST_DIR}" "${LOGS_HOST_DIR}" "${CONTAINER_EXIT_CODE}" "${CONTAINER_APP_ROOT}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

result_path = Path(sys.argv[1])
stdout_path = Path(sys.argv[2])
manifest_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])
host_context_path = Path(sys.argv[5])
target_container = sys.argv[6]
app_host_dir = sys.argv[7]
logs_host_dir = sys.argv[8]
container_exit_code = int(sys.argv[9])
container_app_root = sys.argv[10]

result = None
if result_path.exists():
    try:
        candidate = json.loads(result_path.read_text())
        if isinstance(candidate, dict):
            result = candidate
    except json.JSONDecodeError:
        result = None

host_context = None
if host_context_path.exists():
    host_context = json.loads(host_context_path.read_text())

payload = {
    "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "target_container": target_container,
    "container_exit_code": container_exit_code,
    "app_host_dir": app_host_dir or None,
    "host_context": host_context,
    "result": result,
}

if result and app_host_dir:
    for key in ["artifact_json", "artifact_markdown", "artifact_source_type_csv", "stdout_log", "run_dir"]:
        value = result.get(key)
        container_logs_prefix = f"{container_app_root}/logs/"
        container_app_prefix = f"{container_app_root}/"
        if isinstance(value, str) and value.startswith(container_logs_prefix):
            result[f"{key}_host"] = str(Path(logs_host_dir) / value[len(container_logs_prefix):])
        elif isinstance(value, str) and value.startswith(container_app_prefix):
            result[f"{key}_host"] = str(Path(app_host_dir) / value[len(container_app_prefix):])

manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

host = host_context or {}
cpu_1m = (((host.get("cpu_load") or {}).get("parse") or {}).get("load_1m"))
free_bytes = (((host.get("free_memory") or {}).get("parse") or {}).get("available_bytes_estimate"))
free_gb = round(free_bytes / (1024 ** 3), 2) if isinstance(free_bytes, (int, float)) else ""
lines = [
    "# Attached Reference Benchmark",
    "",
    f"- Target container: `{target_container}`",
    f"- Container exit code: `{container_exit_code}`",
    f"- Artifact JSON: `{(result or {}).get('artifact_json', '')}`",
    f"- Artifact JSON host path: `{(result or {}).get('artifact_json_host', '')}`",
    "",
    "| Status | Throughput | Load-Adj | Files | Records | Per-Record Wall ms | CPU 1m | Free GB |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    "| {status} | {throughput} | {load_adjusted} | {files} | {records} | {wall} | {cpu_1m} | {free_gb} |".format(
        status=(result or {}).get("status", ""),
        throughput=(result or {}).get("throughput", ""),
        load_adjusted=(result or {}).get("load_adjusted_throughput", ""),
        files=(result or {}).get("files_processed", ""),
        records=(result or {}).get("records_processed", ""),
        wall=(result or {}).get("per_record_wall_mean_ms", ""),
        cpu_1m=cpu_1m if cpu_1m is not None else "",
        free_gb=free_gb,
    ),
]
summary_path.write_text("\n".join(lines) + "\n")
PY
then
  exit 1
fi

log "Attached benchmark manifest: ${MANIFEST_PATH}"
log "Attached benchmark summary: ${SUMMARY_PATH}"
