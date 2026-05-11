#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_APP_ROOT="${APP_DIR:-/app}"
APP_ROOT="${CONTAINER_APP_ROOT:-${DEFAULT_APP_ROOT}}"
export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

INPUT_PATH="${BENCHMARK_INPUT_PATH:-${APP_ROOT}/adsrefpipe/tests/unittests/stubdata}"
EXTENSIONS="*.raw,*.xml,*.txt,*.html,*.tex,*.refs,*.pairs"
DAYS_BACK=""
MODE="mock"
MAX_FILES=""
TIMEOUT="900"
OUTPUT_DIR="${BENCHMARK_OUTPUT_DIR:-${APP_ROOT}/logs/benchmarks/container_benchmark_runs}"
EVENTS_PATH="${BENCHMARK_EVENTS_PATH:-${APP_ROOT}/logs/benchmarks/perf_events.jsonl}"
LABEL=""
RUN_STAMP=""
SYSTEM_SAMPLE_INTERVAL="1.0"
DISABLE_SYSTEM_LOAD="false"
GROUP_BY="source_type"
WARMUP="true"
PROGRESS="true"

usage() {
  cat <<EOF
Usage: ${CONTAINER_APP_ROOT:-/app}/scripts/run-in-container-benchmark.bash [options]

Options:
  --input-path PATH                  File or directory inside the container
  --extensions CSV                   Comma-separated file patterns
  --days-back N                      Only include files modified within the last N days
  --max-files N                      Optional file cap
  --mode real|mock                   Benchmark mode
  --timeout N                        Benchmark timeout in seconds
  --output-dir PATH                  Output directory inside the container
  --events-path PATH                 Perf events path inside the container
  --label TEXT                       Stable artifact label prefix
  --run-stamp TEXT                   Reuse an externally supplied run timestamp
  --system-sample-interval FLOAT     Host/system sample interval inside container
  --disable-system-load              Disable system load sampling
  --group-by source_type|parser|none Preferred grouping mode
  --no-warmup                        Disable warmup pass
  --no-progress                      Disable live compact progress lines
  --help                             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --events-path)
      EVENTS_PATH="$2"
      shift 2
      ;;
    --label)
      LABEL="$2"
      shift 2
      ;;
    --run-stamp)
      RUN_STAMP="$2"
      shift 2
      ;;
    --system-sample-interval)
      SYSTEM_SAMPLE_INTERVAL="$2"
      shift 2
      ;;
    --disable-system-load)
      DISABLE_SYSTEM_LOAD="true"
      shift
      ;;
    --group-by)
      GROUP_BY="$2"
      shift 2
      ;;
    --no-warmup)
      WARMUP="false"
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

RUN_STAMP="${RUN_STAMP:-$(date -u +"%Y%m%dT%H%M%SZ")}"
RUN_LABEL="${LABEL:-benchmark_${RUN_STAMP}}"
RUN_DIR="${OUTPUT_DIR}/run_${RUN_STAMP}"
RUN_LOG_DIR="${RUN_DIR}/run_logs"
STDOUT_PATH="${RUN_LOG_DIR}/${RUN_LABEL}.stdout.log"
RESULT_PATH="${RUN_LOG_DIR}/${RUN_LABEL}.result.json"
PROGRESS_STOP_PATH="${RUN_LOG_DIR}/${RUN_LABEL}.progress.done"
mkdir -p "${RUN_DIR}" "${RUN_LOG_DIR}"
rm -f "${PROGRESS_STOP_PATH}"

if [[ ! -e "${INPUT_PATH}" ]]; then
  cat > "${RESULT_PATH}" <<EOF
{"status":"failed","error":"input path not found: ${INPUT_PATH}"}
EOF
  echo "Container benchmark result: ${RESULT_PATH}"
  exit 1
fi

BENCHMARK_CMD=(
  python3 -m adsrefpipe.benchmark run
  --input-path "${INPUT_PATH}"
  --extensions "${EXTENSIONS}"
  --mode "${MODE}"
  --timeout "${TIMEOUT}"
  --output-dir "${RUN_DIR}"
  --events-path "${EVENTS_PATH}"
  --system-sample-interval "${SYSTEM_SAMPLE_INTERVAL}"
  --group-by "${GROUP_BY}"
)

if [[ -n "${DAYS_BACK}" ]]; then
  BENCHMARK_CMD+=(--days-back "${DAYS_BACK}")
fi
if [[ -n "${MAX_FILES}" ]]; then
  BENCHMARK_CMD+=(--max-files "${MAX_FILES}")
fi
if [[ "${DISABLE_SYSTEM_LOAD}" == "true" ]]; then
  BENCHMARK_CMD+=(--disable-system-load)
fi
if [[ "${WARMUP}" == "false" ]]; then
  BENCHMARK_CMD+=(--no-warmup)
fi

(
  cd "${APP_DIR}"
  "${BENCHMARK_CMD[@]}"
) > "${STDOUT_PATH}" 2>&1 &
BENCHMARK_PID=$!

PROGRESS_PID=""
if [[ "${PROGRESS}" == "true" ]]; then
  python3 - "${STDOUT_PATH}" "${PROGRESS_STOP_PATH}" <<'PY' &
import os
import sys
import time

from adsrefpipe import perf_metrics

log_path = sys.argv[1]
stop_path = sys.argv[2]
offset = 0
seen_log_lines = set()


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

if wait "${BENCHMARK_PID}"; then
  BENCHMARK_EXIT_CODE=0
else
  BENCHMARK_EXIT_CODE=$?
fi
if [[ -n "${PROGRESS_PID}" ]]; then
  : > "${PROGRESS_STOP_PATH}"
  wait "${PROGRESS_PID}" || true
  rm -f "${PROGRESS_STOP_PATH}"
fi

BENCHMARK_EXIT_CODE="${BENCHMARK_EXIT_CODE:-0}"

if ! python3 - "${STDOUT_PATH}" "${RESULT_PATH}" "${RUN_DIR}" "${RUN_LABEL}" "${BENCHMARK_EXIT_CODE}" <<'PY'
import json
import shutil
import sys
from pathlib import Path

from adsrefpipe import perf_metrics

stdout_path = Path(sys.argv[1])
result_path = Path(sys.argv[2])
run_dir = Path(sys.argv[3])
run_label = sys.argv[4]
benchmark_exit_code = int(sys.argv[5])

payload = {
    "status": "failed" if benchmark_exit_code else "complete",
    "benchmark_exit_code": benchmark_exit_code,
    "run_dir": str(run_dir),
    "stdout_log": str(stdout_path),
}

stdout = stdout_path.read_text() if stdout_path.exists() else ""
decoder = json.JSONDecoder()
benchmark_result = None
for index, char in enumerate(stdout):
    if char != "{":
        continue
    try:
        candidate, _ = decoder.raw_decode(stdout[index:])
    except json.JSONDecodeError:
        continue
    if isinstance(candidate, dict):
        benchmark_result = candidate

if benchmark_result is None:
    payload["status"] = "invalid" if benchmark_exit_code == 0 else payload["status"]
    payload["error"] = "failed to find final benchmark JSON object in stdout"
else:
    payload["status"] = benchmark_result.get("status", payload["status"])
    raw_json_value = benchmark_result.get("json")
    raw_md_value = benchmark_result.get("markdown")
    raw_csv_value = benchmark_result.get("source_type_csv")
    raw_json = Path(raw_json_value) if raw_json_value else None
    raw_md = Path(raw_md_value) if raw_md_value else None
    raw_csv = Path(raw_csv_value) if raw_csv_value else None
    if raw_json is not None and raw_json.is_file():
        summary = json.loads(raw_json.read_text())
        stable_json = run_dir / f"{run_label}.json"
        stable_md = run_dir / f"{run_label}.md"
        stable_csv = run_dir / f"{run_label}.source_types.csv"
        stable_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        payload["artifact_json"] = str(stable_json)
        payload["throughput"] = ((summary.get("throughput") or {}).get("overall_records_per_minute"))
        payload["load_adjusted_throughput"] = ((summary.get("throughput") or {}).get("load_adjusted_records_per_minute"))
        payload["wall_duration_s"] = ((summary.get("duration_s") or {}).get("wall_clock"))
        payload["records_processed"] = ((summary.get("counts") or {}).get("records_processed"))
        payload["files_processed"] = ((summary.get("counts") or {}).get("files_processed"))
        payload["per_record_wall_mean_ms"] = ((((summary.get("per_record_metrics_ms") or {}).get("wall_time") or {}).get("mean")))
        payload["source_type_breakdown"] = summary.get("source_type_breakdown")
        payload["raw_subfamily_breakdown"] = summary.get("raw_subfamily_breakdown")
        if raw_md is not None and raw_md.is_file():
            shutil.copy2(raw_md, stable_md)
            payload["artifact_markdown"] = str(stable_md)
        else:
            perf_metrics.render_markdown(summary, str(stable_md))
            payload["artifact_markdown"] = str(stable_md)
            payload["artifact_markdown_generated_from_json"] = True
        if raw_csv is not None and raw_csv.is_file():
            shutil.copy2(raw_csv, stable_csv)
            payload["artifact_source_type_csv"] = str(stable_csv)

result_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print("Container benchmark result: %s" % result_path)
if payload.get("artifact_markdown"):
    print("Container benchmark summary: %s" % payload["artifact_markdown"])
if payload.get("artifact_source_type_csv"):
    print("Container benchmark source-type CSV: %s" % payload["artifact_source_type_csv"])
print("Container benchmark stdout log: %s" % stdout_path)
PY
then
  exit 1
fi
