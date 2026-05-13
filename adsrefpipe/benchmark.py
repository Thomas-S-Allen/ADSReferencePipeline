"""Benchmark CLI for ADSReferencePipeline throughput profiling."""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, TypedDict

try:
    from adsputils import get_date, load_config
except ImportError:  # pragma: no cover
    def get_date(*args, **kwargs):
        if args and args[0] == "1972":
            return datetime(1972, 1, 1)
        return datetime.now()

    def load_config(*args, **kwargs):
        return {}

import adsrefpipe.perf_metrics as perf_metrics
import adsrefpipe.utils as utils


DEFAULT_EXTENSIONS = "*.raw,*.xml,*.txt,*.html,*.tex,*.refs,*.pairs"
LOGGER = logging.getLogger(__name__)
RE_RECORD_ID = re.compile(r"^H(?P<history_id>\d+)I(?P<item_num>\d+)$")


class SourceFileClassification(TypedDict):
    source_filename: str
    parser_name: Optional[str]
    input_extension: Optional[str]
    source_type: Optional[str]


def _pipeline_run_module():
    import run as pipeline_run

    return pipeline_run


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_csv_list(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _sample_interval_arg(value: str) -> float:
    parsed = float(value)
    if parsed < 0.1:
        raise argparse.ArgumentTypeError("--system-sample-interval must be >= 0.1 seconds")
    return parsed


def _safe_git_commit() -> Optional[str]:
    try:
        import subprocess

        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode == 0:
            return proc.stdout.strip() or None
    except Exception:
        return None
    return None


def collect_candidate_files(input_path: str, extensions: Iterable[str]) -> List[str]:
    if os.path.isfile(input_path):
        return [input_path]

    patterns = [pattern.strip() for pattern in extensions if pattern.strip()]
    if not patterns:
        patterns = ["*"]

    matched = []
    for root, _, files in os.walk(input_path):
        for basename in files:
            if basename.endswith(".result") or basename.endswith(".pyc") or basename == ".DS_Store":
                continue
            full_path = os.path.join(root, basename)
            for pattern in patterns:
                if __import__("fnmatch").fnmatch(basename, pattern):
                    matched.append(full_path)
                    break
    return sorted(set(matched))


def _flatten_grouped_files(grouped_files: Iterable[Iterable[str]]) -> List[str]:
    flattened: List[str] = []
    seen = set()
    for group in grouped_files:
        for filename in group:
            if filename not in seen:
                flattened.append(filename)
                seen.add(filename)
    return flattened


def _fallback_grouped_source_filenames(
    source_file_path: str,
    file_extension: str,
    date_cutoff: time.struct_time,
) -> List[List[str]]:
    from collections import defaultdict

    groups = defaultdict(list)
    root_label = "__ROOT__"

    for root, _, files in os.walk(source_file_path):
        for basename in files:
            if fnmatch.fnmatch(basename, file_extension):
                filename = os.path.join(root, basename)
                if time.localtime(os.path.getmtime(filename)) >= date_cutoff:
                    rel_dir = os.path.relpath(root, source_file_path)
                    key = root_label if rel_dir in (".", "") else rel_dir.split(os.sep, 1)[0]
                    groups[key].append(filename)

    if not groups:
        return []

    result: List[List[str]] = []
    if root_label in groups:
        result.append(sorted(groups[root_label]))
    for key in sorted(current_key for current_key in groups.keys() if current_key != root_label):
        result.append(sorted(groups[key]))
    return result


def _resolve_run_source_filename_getter():
    try:
        import run as pipeline_run

        return pipeline_run.get_source_filenames
    except Exception:
        return _fallback_grouped_source_filenames


def collect_candidate_files_with_days_back(
    input_path: str,
    extensions: Iterable[str],
    days_back: Optional[int],
) -> List[str]:
    if os.path.isfile(input_path):
        return [input_path]

    patterns = [pattern.strip() for pattern in extensions if pattern.strip()]
    if not patterns:
        patterns = ["*"]

    if days_back is None:
        return collect_candidate_files(input_path, patterns)

    grouped_getter = _resolve_run_source_filename_getter()
    date_cutoff = (get_date() - timedelta(days=int(days_back))).timetuple()
    flattened: List[str] = []
    seen = set()

    for pattern in patterns:
        grouped_files = grouped_getter(input_path, pattern, date_cutoff)
        for filename in _flatten_grouped_files(grouped_files):
            if filename not in seen:
                flattened.append(filename)
                seen.add(filename)

    return flattened


def classify_source_file(
    filename: str,
    parser_info: Optional[Dict[str, object]] = None,
) -> SourceFileClassification:
    parser_info = parser_info or {}
    input_extension = parser_info.get("extension_pattern") or perf_metrics.source_type_from_filename(filename)
    source_type = input_extension or perf_metrics.source_type_from_filename(filename)
    return {
        "source_filename": filename,
        "parser_name": parser_info.get("name"),
        "input_extension": input_extension,
        "source_type": source_type,
    }


def _mock_resolved_reference(reference: dict, service_url: str) -> list:
    reference_id = str(reference.get("id") or "mock-record")
    refstring = reference.get("refstr") or reference.get("refplaintext") or reference.get("refraw") or ""
    return [{
        "id": reference_id,
        "refstring": refstring,
        "bibcode": "2000mock........A",
        "scix_id": "mock:%s" % reference_id,
        "score": 1.0,
        "external_identifier": ["mock:%s" % reference_id],
        "publication_year": reference.get("publication_year"),
        "refereed_status": reference.get("refereed_status"),
        "service_url": service_url,
    }]


@contextmanager
def mock_resolver(enabled: bool):
    original = utils.post_request_resolved_reference
    if enabled:
        utils.post_request_resolved_reference = _mock_resolved_reference
    try:
        yield
    finally:
        if enabled:
            utils.post_request_resolved_reference = original


@contextmanager
def benchmark_environment(
    run_id: str,
    context_id: str,
    events_path: str,
    mode: str,
    config: Optional[dict] = None,
):
    previous = {}
    updates = {
        "PERF_METRICS_ENABLED": "true",
        "PERF_METRICS_RUN_ID": str(run_id),
        "PERF_METRICS_CONTEXT_ID": str(context_id),
        "PERF_METRICS_PATH": str(events_path),
        "PERF_BENCHMARK_MODE": str(mode),
        "PERF_BENCHMARK_CONTINUE_ON_ERROR": "true",
    }
    context_dir = perf_metrics.metrics_context_dir(config=config)
    if context_dir:
        updates["PERF_METRICS_CONTEXT_DIR"] = context_dir

    for key, value in updates.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value

    perf_metrics.register_run_metrics_context(
        run_id=run_id,
        enabled=True,
        path=events_path,
        context_id=context_id,
        config=config,
        context_dir=context_dir,
    )
    try:
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _write_run_artifacts(summary: Dict[str, object], output_dir: str) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    run_id = ((summary.get("run_metadata") or {}).get("run_id") or "unknown")
    stem = "ads_reference_benchmark_%s_run%s" % (_utc_timestamp(), run_id)
    json_path = os.path.join(output_dir, "%s.json" % stem)
    md_path = os.path.join(output_dir, "%s.md" % stem)
    csv_path = os.path.join(output_dir, "%s.source_types.csv" % stem)
    perf_metrics.write_json(json_path, summary)
    perf_metrics.render_markdown(summary, md_path)
    perf_metrics.write_source_type_csv(summary, csv_path)
    return {"json": json_path, "markdown": md_path, "source_type_csv": csv_path}


def _run_warmup(files: List[str], mode: str) -> None:
    if not files:
        return
    try:
        with mock_resolver(mode == "mock"):
            _pipeline_run_module().process_files(files[:1])
    except Exception:
        # Warmup is best-effort and should not prevent the measured run from
        # executing, especially in real mode where a resolver can rate-limit.
        return


def _record_ids_for_stage(events: List[Dict[str, Any]], stage: str) -> set:
    return {
        str(event.get("record_id"))
        for event in events
        if event.get("stage") == stage and event.get("record_id")
    }


def _pending_record_ids_from_db(record_ids: set) -> set:
    if not record_ids:
        return set()

    parsed_ids = []
    for record_id in sorted(record_ids):
        match = RE_RECORD_ID.match(str(record_id))
        if not match:
            parsed_ids.append((record_id, None, None))
            continue
        parsed_ids.append((record_id, int(match.group("history_id")), int(match.group("item_num"))))

    valid_pairs = [(history_id, item_num) for _, history_id, item_num in parsed_ids if history_id is not None and item_num is not None]
    if not valid_pairs:
        return set(record_ids)

    from adsrefpipe.models import ResolvedReference

    run_module = _pipeline_run_module()
    pending = set(record_ids)
    with run_module.app.session_scope() as session:
        rows = session.query(
            ResolvedReference.history_id,
            ResolvedReference.item_num,
            ResolvedReference.bibcode,
            ResolvedReference.scix_id,
            ResolvedReference.score,
        ).all()

    state_by_key = {
        (int(row.history_id), int(row.item_num)): row
        for row in rows
        if (int(row.history_id), int(row.item_num)) in valid_pairs
    }
    for record_id, history_id, item_num in parsed_ids:
        if history_id is None or item_num is None:
            continue
        row = state_by_key.get((history_id, item_num))
        if not row:
            continue
        if row.bibcode != "0000" or row.scix_id != "0000" or float(row.score) != -1.0:
            pending.discard(record_id)
    return pending


def _wait_for_async_completion(
    run_id: str,
    context_id: str,
    events_path: str,
    timeout_s: int,
) -> Dict[str, Any]:
    deadline = time.time() + max(1, int(timeout_s))
    submitted_ids = set()
    completed_ids = set()
    while time.time() < deadline:
        events = perf_metrics.load_events(events_path, run_id=run_id, context_id=context_id)
        submitted_ids = _record_ids_for_stage(events, "record_submit")
        completed_ids = _record_ids_for_stage(events, "record_wall")
        if submitted_ids and completed_ids.issuperset(submitted_ids):
            return {
                "submitted_record_ids": submitted_ids,
                "completed_record_ids": completed_ids,
                "completion_source": "events",
                "timed_out": False,
            }
        time.sleep(0.5)

    pending_ids = submitted_ids - completed_ids if submitted_ids else set()
    pending_db_ids = _pending_record_ids_from_db(pending_ids)
    if submitted_ids and not pending_db_ids:
        return {
            "submitted_record_ids": submitted_ids,
            "completed_record_ids": submitted_ids,
            "completion_source": "db_fallback",
            "timed_out": False,
        }
    return {
        "submitted_record_ids": submitted_ids,
        "completed_record_ids": completed_ids,
        "completion_source": None,
        "timed_out": True,
    }


def _run_case(
    input_path: str,
    extensions: List[str],
    days_back: Optional[int],
    max_files: Optional[int],
    mode: str,
    events_path: str,
    system_sample_interval_s: float,
    system_load_enabled: bool,
    warmup: bool,
    group_by: str,
    async_mode: str,
    timeout_s: int,
) -> Dict[str, Any]:
    config = load_config(proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), "../")))
    all_files = collect_candidate_files_with_days_back(input_path, extensions, days_back)
    selected_files = all_files[:max_files] if max_files else all_files

    if not selected_files:
        raise RuntimeError("No benchmark candidate files found under %s" % input_path)

    if warmup:
        _run_warmup(selected_files, mode=mode)

    run_id = uuid.uuid4().hex
    context_id = uuid.uuid4().hex
    system_samples = []
    sampler_stop = threading.Event()

    def _sample_loop() -> None:
        while not sampler_stop.wait(system_sample_interval_s):
            system_samples.append(perf_metrics.collect_system_sample())

    sampler_thread = None
    completion_state = None
    start_wall = time.time()
    with benchmark_environment(run_id=run_id, context_id=context_id, events_path=events_path, mode=mode, config=config):
        try:
            if system_load_enabled:
                system_samples.append(perf_metrics.collect_system_sample())
                sampler_thread = threading.Thread(target=_sample_loop, daemon=True)
                sampler_thread.start()

            with mock_resolver(mode == "mock"):
                _pipeline_run_module().process_files(selected_files)
            if async_mode == "end-to-end":
                completion_state = _wait_for_async_completion(
                    run_id=run_id,
                    context_id=context_id,
                    events_path=events_path,
                    timeout_s=timeout_s,
                )
        finally:
            if system_load_enabled:
                sampler_stop.set()
                if sampler_thread is not None:
                    sampler_thread.join(timeout=max(0.1, system_sample_interval_s))
                    if sampler_thread.is_alive():
                        LOGGER.warning(
                            "System load sampler thread did not terminate before aggregation",
                            extra={
                                "sample_interval_s": system_sample_interval_s,
                                "run_id": run_id,
                                "context_id": context_id,
                            },
                        )
                system_samples.append(perf_metrics.collect_system_sample())
    end_wall = time.time()

    events = perf_metrics.load_events(events_path, run_id=run_id, context_id=context_id)
    summary = perf_metrics.aggregate_ads_events(
        events,
        started_at=start_wall,
        ended_at=end_wall,
        expected_files=len(selected_files),
    )
    summary["run_metadata"] = {
        "run_id": run_id,
        "context_id": context_id,
        "input_path": input_path,
        "extensions": extensions,
        "days_back": days_back,
        "max_files": max_files,
        "mode": mode,
        "group_by": group_by,
        "async_mode": async_mode,
        "git_commit": _safe_git_commit(),
        "timestamp_utc": _utc_timestamp(),
        "system_sample_interval_s": system_sample_interval_s,
        "system_load_enabled": system_load_enabled,
        "warmup": bool(warmup),
        "timeout_s": int(timeout_s),
    }
    if async_mode == "end-to-end":
        completion_state = completion_state or {
            "submitted_record_ids": set(),
            "completed_record_ids": set(),
            "completion_source": None,
            "timed_out": False,
        }
        summary["async_completion"] = {
            "submitted_record_count": len(completion_state["submitted_record_ids"]),
            "completed_record_count": len(completion_state["completed_record_ids"]),
            "completion_source": completion_state["completion_source"],
            "timed_out": bool(completion_state["timed_out"]),
        }
        if completion_state["completion_source"] == "db_fallback":
            summary["counts"]["records_processed"] = len(completion_state["submitted_record_ids"])
        if completion_state["timed_out"]:
            summary["status"] = "incomplete"
    summary["selected_files"] = selected_files
    summary["counts"]["files_selected"] = len(selected_files)
    summary["system_load"] = perf_metrics.aggregate_system_samples(
        system_samples if system_load_enabled else [],
        enabled=system_load_enabled,
        sample_interval_s=system_sample_interval_s,
    )
    perf_metrics.apply_system_load_adjustment(summary)
    return summary


def cmd_run(args) -> int:
    config = load_config(proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), "../")))
    output_dir = args.output_dir or config.get("PERF_METRICS_OUTPUT_DIR", os.path.join("logs", "benchmarks"))
    os.makedirs(output_dir, exist_ok=True)
    events_path = args.events_path or os.path.join(output_dir, "perf_events.jsonl")
    extensions = _parse_csv_list(args.extensions) or _parse_csv_list(DEFAULT_EXTENSIONS)

    summary = _run_case(
        input_path=args.input_path,
        extensions=extensions,
        days_back=args.days_back,
        max_files=args.max_files,
        mode=args.mode,
        events_path=events_path,
        system_sample_interval_s=args.system_sample_interval,
        system_load_enabled=not bool(args.disable_system_load),
        warmup=bool(args.warmup),
        group_by=args.group_by,
        async_mode=args.async_mode,
        timeout_s=args.timeout,
    )

    artifacts = _write_run_artifacts(summary, output_dir=output_dir)
    print(json.dumps({
        "status": summary.get("status"),
        "throughput": ((summary.get("throughput") or {}).get("overall_records_per_minute")),
        "load_adjusted_throughput": ((summary.get("throughput") or {}).get("load_adjusted_records_per_minute")),
        "json": artifacts["json"],
        "markdown": artifacts["markdown"],
        "source_type_csv": artifacts["source_type_csv"],
    }, indent=2, sort_keys=True))
    return 0 if summary.get("status") == "complete" else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ADS reference throughput benchmark CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run one benchmark configuration")
    run_parser.add_argument(
        "--input-path",
        default=os.path.join(os.path.dirname(__file__), "tests", "unittests", "stubdata"),
        help="File or directory to benchmark",
    )
    run_parser.add_argument("--extensions", default=DEFAULT_EXTENSIONS)
    run_parser.add_argument("--days-back", type=int, default=None)
    run_parser.add_argument("--max-files", type=int, default=None)
    run_parser.add_argument("--mode", choices=["real", "mock"], default="mock")
    run_parser.add_argument("--async-mode", choices=["enqueue-only", "end-to-end"], default="enqueue-only")
    run_parser.add_argument("--output-dir", default=None)
    run_parser.add_argument("--events-path", default=None)
    run_parser.add_argument("--timeout", type=int, default=900)
    run_parser.add_argument("--system-sample-interval", type=_sample_interval_arg, default=1.0)
    run_parser.add_argument("--disable-system-load", action="store_true", default=False)
    run_parser.add_argument("--group-by", choices=["source_type", "parser", "none"], default="source_type")
    run_parser.add_argument("--no-warmup", dest="warmup", action="store_false")
    run_parser.set_defaults(warmup=True)
    run_parser.set_defaults(func=cmd_run)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
