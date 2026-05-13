"""Performance metrics helpers for ADSReferencePipeline benchmarks.

This module stays stdlib-only and uses file-backed JSONL events so benchmark
instrumentation remains lightweight and safe to leave disabled by default.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Dict, Iterable, List, Optional, TypedDict

LOGGER = logging.getLogger(__name__)
_EVENT_WRITE_LOCK = threading.Lock()


class AdsBenchmarkSummary(TypedDict):
    counts: Dict[str, Any]
    throughput: Dict[str, Any]
    latency_ms: Dict[str, Any]
    task_timing_ms: Dict[str, Any]
    app_timing_ms: Dict[str, Any]
    duration_s: Dict[str, Any]
    per_record_metrics_ms: Dict[str, Any]
    source_type_breakdown: Dict[str, Any]
    parser_breakdown: Dict[str, Any]
    raw_subfamily_breakdown: Dict[str, Any]
    errors: Dict[str, Any]
    status: str
    selected_files: List[str]
    file_wall_ms: Dict[str, Any]


_PROGRESS_MESSAGE_RE = re.compile(
    r"^Source file (?P<source_filename>.+?) for bibcode .+? with "
    r"(?P<record_count>\d+) references, processed successfully\.$"
)


def format_benchmark_progress_line_from_log_line(line: str) -> Optional[str]:
    """Return a compact benchmark progress line for one structured log line."""
    try:
        payload = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None

    message = payload.get("message")
    if not isinstance(message, str):
        return None
    match = _PROGRESS_MESSAGE_RE.match(message)
    if not match:
        return None

    timestamp = payload.get("timestamp") or payload.get("asctime")
    if not isinstance(timestamp, str):
        return None
    if "T" in timestamp:
        time_part = timestamp.split("T", 1)[1]
    else:
        time_part = timestamp
    time_part = time_part.split(".", 1)[0].replace("Z", "")

    return "%s %s with %d references" % (
        time_part,
        os.path.basename(match.group("source_filename")),
        int(match.group("record_count")),
    )


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on", "active"}


def _metrics_debug(message: str, **context: Any) -> None:
    if context:
        suffix = ", ".join("%s=%r" % (key, value) for key, value in sorted(context.items()))
        LOGGER.debug("%s (%s)", message, suffix)
        return
    LOGGER.debug("%s", message)


def _deep_get(data: Optional[Dict[str, Any]], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def metrics_enabled(config: Optional[dict] = None) -> bool:
    env_value = os.getenv("PERF_METRICS_ENABLED")
    if env_value is not None:
        return _as_bool(env_value)
    if config is None:
        return False
    return _as_bool(config.get("PERF_METRICS_ENABLED", False))


def metrics_path(config: Optional[dict] = None) -> Optional[str]:
    env_path = os.getenv("PERF_METRICS_PATH")
    if env_path:
        return env_path
    if config is None:
        return None
    config_path = config.get("PERF_METRICS_PATH")
    if config_path:
        return config_path
    output_dir = config.get("PERF_METRICS_OUTPUT_DIR")
    if output_dir:
        return os.path.join(output_dir, "perf_events.jsonl")
    return None


def metrics_context_dir(config: Optional[dict] = None) -> Optional[str]:
    env_dir = os.getenv("PERF_METRICS_CONTEXT_DIR")
    if env_dir:
        return env_dir
    if config is not None:
        config_dir = config.get("PERF_METRICS_CONTEXT_DIR")
        if config_dir:
            return config_dir
    base_path = metrics_path(config=config)
    if base_path:
        return os.path.join(os.path.dirname(base_path), "perf_run_context")
    return os.path.join(tempfile.gettempdir(), "adsrefpipe_perf_run_context")


def _record_context_dir(config: Optional[dict] = None, context_dir: Optional[str] = None) -> Optional[str]:
    base_dir = context_dir or metrics_context_dir(config=config)
    if not base_dir:
        return None
    return os.path.join(base_dir, "record_context")


def _record_context_path(
    record_id: Any,
    config: Optional[dict] = None,
    context_dir: Optional[str] = None,
) -> Optional[str]:
    if record_id is None:
        return None
    directory = _record_context_dir(config=config, context_dir=context_dir)
    if not directory:
        return None
    return os.path.join(directory, "record_%s.json" % str(record_id))


def current_run_id() -> Optional[str]:
    run_id = os.getenv("PERF_METRICS_RUN_ID")
    return str(run_id) if run_id else None


def current_context_id() -> Optional[str]:
    context_id = os.getenv("PERF_METRICS_CONTEXT_ID")
    return str(context_id) if context_id else None


def current_run_mode() -> Optional[str]:
    value = os.getenv("PERF_BENCHMARK_MODE") or os.getenv("PERF_RUN_MODE")
    return str(value) if value else None


def source_type_from_filename(filename: Optional[str]) -> Optional[str]:
    if not filename:
        return None
    basename = os.path.basename(str(filename))
    if "." not in basename:
        return None

    parts = basename.split(".")
    last = parts[-1].lower()
    prev = parts[-2].lower() if len(parts) >= 2 else ""

    if last == "xml":
        if len(parts) >= 3 and len(prev) > 1:
            return ".%s.xml" % prev
        return ".xml"

    if last == "raw":
        return ".raw"

    if last == "txt":
        if prev == "ocr":
            return ".ocr.txt"
        return ".txt"

    if last in {"html", "tex", "refs", "pairs"}:
        return ".%s" % last

    return ".%s" % last


def journal_from_filename(filename: Optional[str]) -> Optional[str]:
    if not filename:
        return None
    parts = str(filename).replace("\\", "/").split("/")
    if len(parts) < 3:
        return None
    return parts[-3] or None


def raw_subfamily_from_metadata(
    filename: Optional[str] = None,
    parser_name: Optional[str] = None,
    input_extension: Optional[str] = None,
    source_type: Optional[str] = None,
) -> Optional[str]:
    effective_source_type = source_type or input_extension or source_type_from_filename(filename)
    if effective_source_type != ".raw":
        return None

    parser = str(parser_name or "").strip()
    journal = str(journal_from_filename(filename) or "").strip()
    basename = os.path.basename(str(filename or "")).lower()

    if parser == "arXiv" or journal == "arXiv":
        return "raw_arxiv"
    if parser == "ThreeBibsTxt" or basename.endswith(".ref.raw"):
        return "raw_ref_raw"
    if parser == "JLVEnHTML" or journal == "JLVEn":
        return "raw_jlven_html"
    if parser == "PASJhtml" or journal == "PASJ":
        return "raw_pasj_html"
    if parser == "PASPhtml" or journal == "PASP":
        return "raw_pasp_html"
    if parser == "AAS" or basename.endswith(".aas.raw"):
        return "raw_aas"
    if parser == "ICARUS" or basename.endswith(".icarus.raw"):
        return "raw_icarus"
    if parser == "PThPhTXT" or journal == "PThPh":
        return "raw_pthph"
    if journal == "PThPS":
        return "raw_pthps"
    if parser == "ADStxt" or journal == "ADS":
        return "raw_adstxt"
    return "raw_other"


def build_event_extra(
    source_filename: Optional[str] = None,
    parser_name: Optional[str] = None,
    source_bibcode: Optional[str] = None,
    input_extension: Optional[str] = None,
    source_type: Optional[str] = None,
    record_count: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(extra or {})
    payload.setdefault("source_filename", source_filename)
    payload.setdefault("parser_name", parser_name)
    payload.setdefault("source_bibcode", source_bibcode)
    payload.setdefault("input_extension", input_extension or source_type_from_filename(source_filename))
    payload.setdefault("source_type", source_type or payload.get("input_extension") or source_type_from_filename(source_filename))
    payload.setdefault(
        "raw_subfamily",
        raw_subfamily_from_metadata(
            filename=source_filename,
            parser_name=parser_name,
            input_extension=payload.get("input_extension"),
            source_type=payload.get("source_type"),
        ),
    )
    payload.setdefault("run_mode", current_run_mode())
    if record_count is not None:
        payload["record_count"] = int(record_count)
    return payload


def _run_context_path(
    run_id: Any,
    config: Optional[dict] = None,
    context_dir: Optional[str] = None,
    context_id: Optional[str] = None,
) -> Optional[str]:
    if run_id is None:
        return None
    directory = context_dir or metrics_context_dir(config=config)
    if not directory:
        return None
    if context_id:
        return os.path.join(directory, "run_%s_%s.json" % (run_id, context_id))
    return os.path.join(directory, "run_%s.json" % run_id)


def register_run_metrics_context(
    run_id: Any,
    enabled: bool,
    path: Optional[str],
    context_id: Optional[str] = None,
    config: Optional[dict] = None,
    context_dir: Optional[str] = None,
) -> None:
    try:
        targets = []
        target = _run_context_path(run_id, config=config, context_dir=context_dir, context_id=context_id)
        if target:
            targets.append(target)
        generic_target = _run_context_path(run_id, config=config, context_dir=context_dir)
        if generic_target and generic_target not in targets:
            targets.append(generic_target)
        if not targets:
            return
        payload = {
            "enabled": bool(enabled),
            "path": path,
            "context_id": context_id,
            "updated_at": time.time(),
        }
        for current_target in targets:
            directory = os.path.dirname(current_target)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(current_target, "w") as handle:
                json.dump(payload, handle, sort_keys=True)
    except Exception as exc:
        _metrics_debug(
            "Failed to register metrics context",
            run_id=run_id,
            context_id=context_id,
            path=path,
            context_dir=context_dir,
            error=str(exc),
        )
        return


def register_record_metrics_context(
    record_id: Any,
    run_id: Any,
    enabled: bool,
    path: Optional[str],
    context_id: Optional[str] = None,
    config: Optional[dict] = None,
    context_dir: Optional[str] = None,
) -> None:
    try:
        target = _record_context_path(record_id, config=config, context_dir=context_dir)
        if not target:
            return
        directory = os.path.dirname(target)
        if directory:
            os.makedirs(directory, exist_ok=True)
        payload = {
            "enabled": bool(enabled),
            "path": path,
            "run_id": str(run_id) if run_id is not None else None,
            "context_id": context_id,
            "updated_at": time.time(),
        }
        with open(target, "w") as handle:
            json.dump(payload, handle, sort_keys=True)
    except Exception as exc:
        _metrics_debug(
            "Failed to register record metrics context",
            record_id=record_id,
            run_id=run_id,
            context_id=context_id,
            path=path,
            context_dir=context_dir,
            error=str(exc),
        )
        return


def resolve_run_metrics_context(
    run_id: Any,
    config: Optional[dict] = None,
    context_id: Optional[str] = None,
) -> Dict[str, Any]:
    target = _run_context_path(run_id, config=config, context_id=context_id)
    if (not target or not os.path.exists(target)) and context_id is not None:
        target = _run_context_path(run_id, config=config)
    if not target or not os.path.exists(target):
        return {"enabled": None, "path": None, "context_id": None}
    try:
        with open(target, "r") as handle:
            payload = json.load(handle)
        return {
            "enabled": payload.get("enabled"),
            "path": payload.get("path"),
            "context_id": payload.get("context_id"),
        }
    except Exception as exc:
        _metrics_debug(
            "Failed to resolve metrics context",
            run_id=run_id,
            context_id=context_id,
            target=target,
            error=str(exc),
        )
        return {"enabled": None, "path": None, "context_id": None}


def resolve_record_metrics_context(
    record_id: Any,
    config: Optional[dict] = None,
    context_dir: Optional[str] = None,
) -> Dict[str, Any]:
    target = _record_context_path(record_id, config=config, context_dir=context_dir)
    if not target or not os.path.exists(target):
        return {"enabled": None, "path": None, "run_id": None, "context_id": None}
    try:
        with open(target, "r") as handle:
            payload = json.load(handle)
        return {
            "enabled": payload.get("enabled"),
            "path": payload.get("path"),
            "run_id": payload.get("run_id"),
            "context_id": payload.get("context_id"),
        }
    except Exception as exc:
        _metrics_debug(
            "Failed to resolve record metrics context",
            record_id=record_id,
            target=target,
            error=str(exc),
        )
        return {"enabled": None, "path": None, "run_id": None, "context_id": None}


def _append_jsonl_record(target_path: str, payload: Dict[str, Any]) -> None:
    serialized_line = json.dumps(payload, sort_keys=True) + "\n"
    with _EVENT_WRITE_LOCK:
        with open(target_path, "a") as handle:
            try:
                import fcntl  # POSIX-only best effort.
            except ImportError:
                fcntl = None
            if fcntl is not None:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                except OSError as exc:
                    _metrics_debug(
                        "Failed to acquire POSIX metrics file lock; continuing with in-process lock only",
                        target_path=target_path,
                        error=str(exc),
                    )
            try:
                handle.write(serialized_line)
            finally:
                if fcntl is not None:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass


def emit_event(
    stage: str,
    run_id: Optional[Any] = None,
    context_id: Optional[str] = None,
    record_id: Optional[str] = None,
    duration_ms: Optional[float] = None,
    status: str = "ok",
    extra: Optional[dict] = None,
    config: Optional[dict] = None,
    path: Optional[str] = None,
) -> None:
    try:
        resolved_run_id = run_id if run_id is not None else current_run_id()
        resolved_context_id = context_id or current_context_id()
        run_context = (
            resolve_run_metrics_context(resolved_run_id, config=config, context_id=resolved_context_id)
            if resolved_run_id is not None
            else {"enabled": None, "path": None, "context_id": None}
        )
        enabled = metrics_enabled(config=config)
        if run_context.get("enabled") is not None:
            enabled = bool(run_context.get("enabled"))
        if not enabled:
            return

        target_path = path or run_context.get("path") or metrics_path(config=config)
        if not target_path:
            return

        payload = {
            "ts": time.time(),
            "stage": stage,
            "run_id": str(resolved_run_id) if resolved_run_id is not None else None,
            "context_id": resolved_context_id or run_context.get("context_id"),
            "record_id": record_id,
            "duration_ms": float(duration_ms) if duration_ms is not None else None,
            "status": status,
            "extra": extra or {},
        }

        directory = os.path.dirname(target_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        _append_jsonl_record(target_path, payload)
    except Exception as exc:
        _metrics_debug(
            "Failed to emit metrics event",
            stage=stage,
            run_id=run_id,
            context_id=context_id,
            path=path,
            error=str(exc),
        )
        return


@contextmanager
def timed_stage(
    stage: str,
    run_id: Optional[Any] = None,
    context_id: Optional[str] = None,
    record_id: Optional[str] = None,
    status: str = "ok",
    extra: Optional[dict] = None,
    config: Optional[dict] = None,
    path: Optional[str] = None,
):
    start = time.perf_counter()
    outcome = status
    try:
        yield
    except Exception:
        outcome = "error"
        raise
    finally:
        emit_event(
            stage=stage,
            run_id=run_id,
            context_id=context_id,
            record_id=record_id,
            duration_ms=(time.perf_counter() - start) * 1000.0,
            status=outcome,
            extra=extra,
            config=config,
            path=path,
        )


@contextmanager
def timed_profile(
    category: str,
    name: str,
    run_id: Optional[Any] = None,
    context_id: Optional[str] = None,
    record_id: Optional[str] = None,
    status: str = "ok",
    extra: Optional[dict] = None,
    config: Optional[dict] = None,
    path: Optional[str] = None,
):
    payload_extra = {"name": name}
    if extra:
        payload_extra.update(extra)
    with timed_stage(
        stage=category,
        run_id=run_id,
        context_id=context_id,
        record_id=record_id,
        status=status,
        extra=payload_extra,
        config=config,
        path=path,
    ):
        yield


def profiled_function(
    category: str,
    name: Optional[str] = None,
    run_id_getter=None,
    context_id_getter=None,
    record_id_getter=None,
    extra_getter=None,
    config_getter=None,
):
    def decorator(func):
        profile_name = name or func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            run_id = run_id_getter(*args, **kwargs) if run_id_getter else None
            context_id = context_id_getter(*args, **kwargs) if context_id_getter else None
            record_id = record_id_getter(*args, **kwargs) if record_id_getter else None
            extra = extra_getter(*args, **kwargs) if extra_getter else None
            config = config_getter(*args, **kwargs) if config_getter else None
            with timed_profile(
                category=category,
                name=profile_name,
                run_id=run_id,
                context_id=context_id,
                record_id=record_id,
                extra=extra,
                config=config,
            ):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def load_events(path: str, run_id: Optional[Any] = None, context_id: Optional[str] = None) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []

    run_id_str = str(run_id) if run_id is not None else None
    context_id_str = str(context_id) if context_id is not None else None
    output = []
    try:
        with open(path, "r") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception as exc:
                    _metrics_debug(
                        "Failed to parse metrics event line",
                        path=path,
                        line_number=line_number,
                        error=str(exc),
                    )
                    continue
                if run_id_str is not None and payload.get("run_id") != run_id_str:
                    continue
                if context_id_str is not None and payload.get("context_id") != context_id_str:
                    continue
                output.append(payload)
    except Exception as exc:
        _metrics_debug(
            "Failed to load metrics events",
            path=path,
            run_id=run_id_str,
            context_id=context_id_str,
            error=str(exc),
        )
        return []
    return output


def percentile(values: Iterable[float], pct: float) -> Optional[float]:
    data = sorted(float(v) for v in values)
    if not data:
        return None
    if pct <= 0:
        return data[0]
    if pct >= 100:
        return data[-1]
    idx = (len(data) - 1) * (pct / 100.0)
    lower = int(idx)
    upper = min(lower + 1, len(data) - 1)
    weight = idx - lower
    return data[lower] * (1.0 - weight) + data[upper] * weight


def _numeric_stats(values: List[float], include_p99: bool = True) -> Dict[str, Any]:
    if not values:
        output = {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "p50": None,
            "p95": None,
        }
        if include_p99:
            output["p99"] = None
        return output

    output = {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "mean": sum(values) / float(len(values)),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
    }
    if include_p99:
        output["p99"] = percentile(values, 99)
    return output


def _read_linux_meminfo(path: str = "/proc/meminfo") -> Optional[Dict[str, float]]:
    try:
        values = {}
        with open(path, "r") as handle:
            for line in handle:
                parts = line.split(":")
                if len(parts) != 2:
                    continue
                match = re.search(r"(\d+)", parts[1])
                if match:
                    values[parts[0].strip()] = int(match.group(1))
        total_kib = values.get("MemTotal")
        available_kib = values.get("MemAvailable")
        if total_kib is None or available_kib is None or total_kib <= 0:
            return None
        total_bytes = int(total_kib) * 1024
        available_bytes = int(available_kib) * 1024
        return {
            "memory_total_bytes": total_bytes,
            "memory_available_bytes": available_bytes,
            "memory_available_ratio": float(available_bytes) / float(total_bytes),
            "memory_probe": "linux_meminfo",
        }
    except Exception as exc:
        _metrics_debug("Failed to read Linux memory info", path=path, error=str(exc))
        return None


def _read_macos_memory() -> Optional[Dict[str, float]]:
    try:
        import subprocess

        total_proc = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            check=False,
        )
        if total_proc.returncode != 0:
            return None
        total_bytes = int((total_proc.stdout or "").strip())
        vm_proc = subprocess.run(
            ["vm_stat"],
            capture_output=True,
            text=True,
            check=False,
        )
        if vm_proc.returncode != 0:
            return None
        page_size = 4096
        page_size_match = re.search(r"page size of (\d+) bytes", vm_proc.stdout or "")
        if page_size_match:
            page_size = int(page_size_match.group(1))
        pages = {}
        for line in (vm_proc.stdout or "").splitlines():
            match = re.match(r"([^:]+):\s+(\d+)\.", line.strip())
            if match:
                pages[match.group(1)] = int(match.group(2))
        available_pages = pages.get("Pages free", 0) + pages.get("Pages inactive", 0) + pages.get("Pages speculative", 0)
        available_bytes = int(available_pages) * int(page_size)
        return {
            "memory_total_bytes": int(total_bytes),
            "memory_available_bytes": available_bytes,
            "memory_available_ratio": float(available_bytes) / float(total_bytes) if total_bytes else None,
            "memory_probe": "macos_vm_stat",
        }
    except Exception as exc:
        _metrics_debug("Failed to read macOS memory info", error=str(exc))
        return None


def _host_memory_snapshot() -> Dict[str, Optional[float]]:
    system = platform.system().lower()
    if system == "linux":
        snapshot = _read_linux_meminfo()
        if snapshot:
            return snapshot
        return {
            "memory_total_bytes": None,
            "memory_available_bytes": None,
            "memory_available_ratio": None,
            "memory_probe": "linux_meminfo_unavailable",
        }
    if system == "darwin":
        snapshot = _read_macos_memory()
        if snapshot:
            return snapshot
        return {
            "memory_total_bytes": None,
            "memory_available_bytes": None,
            "memory_available_ratio": None,
            "memory_probe": "macos_vm_stat_unavailable",
        }
    return {
        "memory_total_bytes": None,
        "memory_available_bytes": None,
        "memory_available_ratio": None,
        "memory_probe": "unsupported",
    }


def _host_load_snapshot() -> Dict[str, Optional[float]]:
    cpu_count = os.cpu_count()
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception as exc:
        _metrics_debug("Failed to read host load average", error=str(exc))
        load1 = load5 = load15 = None

    def _normalize(value: Optional[float]) -> Optional[float]:
        if value is None or not cpu_count:
            return None
        return float(value) / float(cpu_count)

    return {
        "platform": platform.system().lower(),
        "cpu_count": cpu_count,
        "loadavg_1m": float(load1) if load1 is not None else None,
        "loadavg_5m": float(load5) if load5 is not None else None,
        "loadavg_15m": float(load15) if load15 is not None else None,
        "normalized_load_1m": _normalize(load1),
        "normalized_load_5m": _normalize(load5),
        "normalized_load_15m": _normalize(load15),
    }


def collect_system_sample() -> Dict[str, Optional[float]]:
    sample = {"ts": time.time()}
    sample.update(_host_load_snapshot())
    sample.update(_host_memory_snapshot())
    total_bytes = sample.get("memory_total_bytes")
    available_bytes = sample.get("memory_available_bytes")
    if total_bytes is not None and available_bytes is not None:
        used_bytes = max(0.0, float(total_bytes) - float(available_bytes))
        sample["memory_used_bytes"] = used_bytes
        sample["memory_used_ratio"] = used_bytes / float(total_bytes) if float(total_bytes) > 0 else None
    else:
        sample["memory_used_bytes"] = None
        sample["memory_used_ratio"] = None
    return sample


def aggregate_system_samples(samples: List[Dict[str, Any]], enabled: bool = True, sample_interval_s: float = 1.0) -> Dict[str, Any]:
    platform_name = platform.system().lower()
    cpu_count = os.cpu_count()
    memory_probe = "unsupported"
    if samples:
        platform_name = str(samples[0].get("platform") or platform_name)
        cpu_count = samples[0].get("cpu_count")
        memory_probe = str(samples[0].get("memory_probe") or memory_probe)

    summary = {}
    for key in (
        "loadavg_1m",
        "loadavg_5m",
        "loadavg_15m",
        "normalized_load_1m",
        "normalized_load_5m",
        "normalized_load_15m",
        "memory_total_bytes",
        "memory_available_bytes",
        "memory_used_bytes",
        "memory_available_ratio",
        "memory_used_ratio",
    ):
        values = [float(sample[key]) for sample in samples if sample.get(key) is not None]
        summary[key] = _numeric_stats(values, include_p99=False)

    return {
        "collection": {
            "enabled": bool(enabled),
            "sample_interval_s": float(sample_interval_s),
            "sample_count": len(samples),
            "platform": platform_name,
            "cpu_count": cpu_count,
            "memory_probe": memory_probe,
        },
        "samples": samples,
        "summary": summary,
    }


def apply_system_load_adjustment(summary: Dict[str, Any]) -> Dict[str, Any]:
    throughput = summary.setdefault("throughput", {})
    raw = throughput.get("overall_records_per_minute")
    mean_load = _deep_get(summary, "system_load", "summary", "normalized_load_1m", "mean")
    factor = max(1.0, float(mean_load)) if mean_load is not None else 1.0
    throughput["host_load_adjustment_factor"] = factor
    throughput["load_adjusted_records_per_minute"] = (float(raw) * factor) if raw is not None else None
    return summary


def aggregate_ads_events(
    events: List[Dict[str, Any]],
    started_at: Optional[float] = None,
    ended_at: Optional[float] = None,
    expected_files: Optional[int] = None,
) -> AdsBenchmarkSummary:
    stage_timings = {}
    task_timings = {}
    app_timings = {}
    errors_by_stage = {}
    file_wall = []
    record_wall = []
    resolver_wall = []
    db_wall = []

    source_type_groups = {}
    parser_groups = {}
    raw_subfamily_groups = {}
    file_names = set()
    record_ids = set()
    processed_record_ids = set()
    submitted_record_ids = set()
    records_submitted = 0
    failure_count = 0

    event_timestamps = [event.get("ts") for event in events if event.get("ts") is not None]

    def _group_entry(groups: Dict[str, Dict[str, Any]], key: Optional[str]) -> Dict[str, Any]:
        group_key = str(key or "unknown")
        groups.setdefault(group_key, {
            "file_names": set(),
            "record_ids": set(),
            "wall": [],
            "parse": [],
            "resolver": [],
            "db": [],
        })
        return groups[group_key]

    for event in events:
        stage = str(event.get("stage") or "unknown")
        status = str(event.get("status") or "ok")
        duration = event.get("duration_ms")
        extra = event.get("extra", {}) or {}
        record_count = int(extra.get("record_count", 1) or 1)
        source_filename = extra.get("source_filename")
        source_type = extra.get("source_type") or extra.get("input_extension") or source_type_from_filename(source_filename)
        parser_name = extra.get("parser_name")
        raw_subfamily = extra.get("raw_subfamily") or raw_subfamily_from_metadata(
            filename=source_filename,
            parser_name=parser_name,
            input_extension=extra.get("input_extension"),
            source_type=source_type,
        )
        record_id = event.get("record_id")

        if source_filename:
            file_names.add(source_filename)
        if record_id:
            record_ids.add(record_id)
            if stage == "record_submit":
                submitted_record_ids.add(record_id)
            if stage == "record_wall":
                processed_record_ids.add(record_id)

        if status != "ok":
            failure_count += 1
            errors_by_stage[stage] = errors_by_stage.get(stage, 0) + 1

        if stage == "ingest_enqueue":
            records_submitted += int(extra.get("record_count", 1) or 1)
        elif stage == "record_submit" and record_id:
            records_submitted = max(records_submitted, len(submitted_record_ids))

        if duration is None:
            continue

        duration_value = float(duration)
        normalized_value = duration_value / float(record_count) if record_count > 0 else duration_value
        stage_timings.setdefault(stage, []).append(normalized_value)

        if stage == "task_timing":
            task_timings.setdefault(str(extra.get("name") or "unknown"), []).append(duration_value)
            continue
        if stage == "app_timing":
            app_timings.setdefault(str(extra.get("name") or "unknown"), []).append(duration_value)
            continue

        if stage == "file_wall":
            file_wall.append(normalized_value)
        elif stage == "record_wall":
            record_wall.append(duration_value)
        elif stage == "resolver_http":
            resolver_wall.append(duration_value)
        elif stage in {"pre_resolved_db", "post_resolved_db"}:
            db_wall.append(normalized_value)

        type_group = _group_entry(source_type_groups, source_type)
        parser_group = _group_entry(parser_groups, parser_name)
        raw_group = _group_entry(raw_subfamily_groups, raw_subfamily) if raw_subfamily else None
        for group in tuple(group for group in (type_group, parser_group, raw_group) if group is not None):
            if source_filename:
                group["file_names"].add(source_filename)
            if record_id:
                group["record_ids"].add(record_id)
            if stage == "record_wall":
                group["wall"].append(duration_value)
            elif stage == "parse_dispatch":
                group["parse"].append(normalized_value)
            elif stage == "resolver_http":
                group["resolver"].append(duration_value)
            elif stage in {"pre_resolved_db", "post_resolved_db"}:
                group["db"].append(normalized_value)

    if started_at is None:
        started_at = min(event_timestamps) if event_timestamps else None
    if ended_at is None:
        ended_at = max(event_timestamps) if event_timestamps else None
    wall_duration_s = None if started_at is None or ended_at is None else max(0.0, float(ended_at) - float(started_at))

    throughput = None
    if wall_duration_s and wall_duration_s > 0:
        throughput_basis = len(processed_record_ids) or len(submitted_record_ids) or records_submitted
        throughput = (float(throughput_basis) / float(wall_duration_s)) * 60.0

    def _serialize_group(groups: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        output = {}
        for key, payload in groups.items():
            record_total = len(payload["record_ids"])
            throughput_value = None
            if wall_duration_s and wall_duration_s > 0 and record_total > 0:
                throughput_value = (float(record_total) / float(wall_duration_s)) * 60.0
            output[key] = {
                "file_count": len(payload["file_names"]),
                "record_count": record_total,
                "wall_time_ms": _numeric_stats(payload["wall"], include_p99=True),
                "parse_stage_ms": _numeric_stats(payload["parse"], include_p99=True),
                "resolver_stage_ms": _numeric_stats(payload["resolver"], include_p99=True),
                "db_stage_ms": _numeric_stats(payload["db"], include_p99=True),
                "throughput_records_per_minute": throughput_value,
            }
        return output

    status = "complete"
    if expected_files is not None and len(file_names) < int(expected_files):
        status = "incomplete"
    if expected_files == 0:
        status = "incomplete"

    return {
        "counts": {
            "files_selected": int(expected_files or 0),
            "files_processed": len(file_names),
            "records_submitted": records_submitted,
            "records_processed": len(processed_record_ids),
            "failures": failure_count,
        },
        "throughput": {
            "overall_records_per_minute": throughput,
        },
        "latency_ms": {
            stage: _numeric_stats(values, include_p99=True)
            for stage, values in stage_timings.items()
            if stage not in {"task_timing", "app_timing"}
        },
        "task_timing_ms": {name: _numeric_stats(values, include_p99=True) for name, values in task_timings.items()},
        "app_timing_ms": {name: _numeric_stats(values, include_p99=True) for name, values in app_timings.items()},
        "duration_s": {
            "wall_clock": wall_duration_s,
        },
        "per_record_metrics_ms": {
            "wall_time": _numeric_stats(record_wall, include_p99=True),
            "parse_stage": _numeric_stats(stage_timings.get("parse_dispatch", []), include_p99=True),
            "resolver_stage": _numeric_stats(resolver_wall, include_p99=True),
            "db_stage": _numeric_stats(db_wall, include_p99=True),
        },
        "source_type_breakdown": _serialize_group(source_type_groups),
        "parser_breakdown": _serialize_group(parser_groups),
        "raw_subfamily_breakdown": _serialize_group(raw_subfamily_groups),
        "errors": {
            "by_stage": errors_by_stage,
        },
        "status": status,
        "selected_files": sorted(file_names),
        "file_wall_ms": _numeric_stats(file_wall, include_p99=True),
    }


def write_json(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _fmt(value: Optional[float], places: int = 2) -> str:
    if value is None:
        return "n/a"
    return ("%0." + str(places) + "f") % float(value)


def _fmt_bytes(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    current = float(value)
    for unit in units:
        if abs(current) < 1024.0 or unit == units[-1]:
            return "%0.2f %s" % (current, unit)
        current /= 1024.0


def _blank_if_none(value: Any) -> Any:
    return "" if value is None else value


def render_markdown(summary: Dict[str, Any], output_path: str) -> None:
    counts = summary.get("counts", {}) or {}
    throughput = summary.get("throughput", {}) or {}
    latency = summary.get("latency_ms", {}) or {}
    per_record = summary.get("per_record_metrics_ms", {}) or {}
    source_type_breakdown = summary.get("source_type_breakdown", {}) or {}
    parser_breakdown = summary.get("parser_breakdown", {}) or {}
    raw_subfamily_breakdown = summary.get("raw_subfamily_breakdown", {}) or {}
    system_load = summary.get("system_load", {}) or {}
    run_metadata = summary.get("run_metadata", {}) or {}

    lines = [
        "# ADS Reference Benchmark Report",
        "",
        "## Run Config",
        "",
    ]
    for key in sorted(run_metadata.keys()):
        lines.append("- **%s**: `%s`" % (key, run_metadata[key]))

    lines.extend([
        "",
        "## Top-Line Results",
        "",
        "- **Status**: `%s`" % summary.get("status", "unknown"),
        "- **Files Processed**: `%s`" % counts.get("files_processed", 0),
        "- **Records Processed**: `%s`" % counts.get("records_processed", 0),
        "- **Throughput**: `%s records/min`" % _fmt(throughput.get("overall_records_per_minute")),
        "- **Load-Adjusted Throughput**: `%s records/min`" % _fmt(throughput.get("load_adjusted_records_per_minute")),
        "- **Wall Duration**: `%s s`" % _fmt(_deep_get(summary, "duration_s", "wall_clock")),
        "",
        "## Per-Record Metrics (ms)",
        "",
        "All values in this section are per processed record, in milliseconds.",
        "",
        "| Metric | Count | p50 / record | p95 / record | p99 / record | Mean / record |",
        "|---|---:|---:|---:|---:|---:|",
    ])

    for key in ("wall_time", "parse_stage", "resolver_stage", "db_stage"):
        stats = per_record.get(key, {}) or {}
        lines.append(
            "| {name} | {count} | {p50} | {p95} | {p99} | {mean} |".format(
                name=key,
                count=stats.get("count", 0),
                p50=_fmt(stats.get("p50")),
                p95=_fmt(stats.get("p95")),
                p99=_fmt(stats.get("p99")),
                mean=_fmt(stats.get("mean")),
            )
        )

    if latency:
        lines.extend([
            "",
            "## Stage Latency (ms)",
            "",
            "Unless noted otherwise, stage latency values are normalized to per-record milliseconds.",
            "",
            "| Stage | Count | p50 / record | p95 / record | Mean / record |",
            "|---|---:|---:|---:|---:|",
        ])
        for stage in sorted(latency.keys()):
            stats = latency[stage]
            lines.append(
                "| {stage} | {count} | {p50} | {p95} | {mean} |".format(
                    stage=stage,
                    count=stats.get("count", 0),
                    p50=_fmt(stats.get("p50")),
                    p95=_fmt(stats.get("p95")),
                    mean=_fmt(stats.get("mean")),
                )
            )

    if source_type_breakdown:
        ranked_source_types = sorted(
            source_type_breakdown.items(),
            key=lambda item: (((item[1].get("wall_time_ms") or {}).get("p95")) or 0.0),
            reverse=True,
        )
        lines.extend([
            "",
            "## Slowest Source Types",
            "",
            "Wall values below are per-record milliseconds aggregated across all records in the source-type group.",
            "",
            "| Source Type | Files | Records | Wall p95 / record | Wall Mean / record | Throughput (records/min) |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        for source_type, stats in ranked_source_types[:10]:
            lines.append(
                "| {source_type} | {files} | {records} | {wall_p95} | {wall_mean} | {throughput} |".format(
                    source_type=source_type,
                    files=stats.get("file_count", 0),
                    records=stats.get("record_count", 0),
                    wall_p95=_fmt(_deep_get(stats, "wall_time_ms", "p95")),
                    wall_mean=_fmt(_deep_get(stats, "wall_time_ms", "mean")),
                    throughput=_fmt(stats.get("throughput_records_per_minute")),
                )
            )

        lines.extend([
            "",
            "## Source Types",
            "",
            "`Files` counts source files in the group. `Records` counts extracted/processed references. All mean values below are per-record milliseconds.",
            "",
            "| Source Type | Files | Records | Wall Mean / record | Parse Mean / record | Resolver Mean / record | DB Mean / record | Throughput (records/min) |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ])
        for source_type, stats in ranked_source_types:
            lines.append(
                "| {source_type} | {files} | {records} | {wall} | {parse} | {resolver} | {db} | {throughput} |".format(
                    source_type=source_type,
                    files=stats.get("file_count", 0),
                    records=stats.get("record_count", 0),
                    wall=_fmt(_deep_get(stats, "wall_time_ms", "mean")),
                    parse=_fmt(_deep_get(stats, "parse_stage_ms", "mean")),
                    resolver=_fmt(_deep_get(stats, "resolver_stage_ms", "mean")),
                    db=_fmt(_deep_get(stats, "db_stage_ms", "mean")),
                    throughput=_fmt(stats.get("throughput_records_per_minute")),
                )
            )

    if parser_breakdown:
        lines.extend([
            "",
            "## Parsers",
            "",
            "Wall Mean below is per-record milliseconds for records handled by each parser group.",
            "",
            "| Parser | Files | Records | Wall Mean / record |",
            "|---|---:|---:|---:|",
        ])
        for parser_name in sorted(parser_breakdown.keys()):
            stats = parser_breakdown[parser_name]
            lines.append(
                "| {parser_name} | {files} | {records} | {wall} |".format(
                    parser_name=parser_name,
                    files=stats.get("file_count", 0),
                    records=stats.get("record_count", 0),
                    wall=_fmt(_deep_get(stats, "wall_time_ms", "mean")),
                )
            )

    if raw_subfamily_breakdown:
        lines.extend([
            "",
            "## Raw Subfamilies",
            "",
            "Wall values below are per-record milliseconds within each raw subfamily.",
            "",
            "| Raw Subfamily | Files | Records | Wall Mean / record | Wall p95 / record | Throughput (records/min) |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        for raw_subfamily, stats in sorted(
            raw_subfamily_breakdown.items(),
            key=lambda item: (((item[1].get("wall_time_ms") or {}).get("p95")) or 0.0),
            reverse=True,
        ):
            lines.append(
                "| {raw_subfamily} | {files} | {records} | {wall_mean} | {wall_p95} | {throughput} |".format(
                    raw_subfamily=raw_subfamily,
                    files=stats.get("file_count", 0),
                    records=stats.get("record_count", 0),
                    wall_mean=_fmt(_deep_get(stats, "wall_time_ms", "mean")),
                    wall_p95=_fmt(_deep_get(stats, "wall_time_ms", "p95")),
                    throughput=_fmt(stats.get("throughput_records_per_minute")),
                )
            )

    if system_load:
        collection = system_load.get("collection", {}) or {}
        load_summary = system_load.get("summary", {}) or {}
        mean_load_1m = _deep_get(load_summary, "loadavg_1m", "mean")
        mean_load_5m = _deep_get(load_summary, "loadavg_5m", "mean")
        mean_load_15m = _deep_get(load_summary, "loadavg_15m", "mean")
        max_load_1m = _deep_get(load_summary, "loadavg_1m", "max")
        mean_norm_1m = _deep_get(load_summary, "normalized_load_1m", "mean")
        max_norm_1m = _deep_get(load_summary, "normalized_load_1m", "max")
        mean_mem_total = _deep_get(load_summary, "memory_total_bytes", "mean")
        mean_mem_available = _deep_get(load_summary, "memory_available_bytes", "mean")
        min_mem_available = _deep_get(load_summary, "memory_available_bytes", "min")
        mean_mem_used = _deep_get(load_summary, "memory_used_bytes", "mean")
        max_mem_used = _deep_get(load_summary, "memory_used_bytes", "max")
        mean_mem_available_ratio = _deep_get(load_summary, "memory_available_ratio", "mean")
        min_mem_available_ratio = _deep_get(load_summary, "memory_available_ratio", "min")
        mean_mem_used_ratio = _deep_get(load_summary, "memory_used_ratio", "mean")
        max_mem_used_ratio = _deep_get(load_summary, "memory_used_ratio", "max")
        lines.extend([
            "",
            "## System Load",
            "",
            "- **Enabled**: `%s`" % collection.get("enabled"),
            "- **Sample Count**: `%s`" % collection.get("sample_count"),
            "- **Sample Interval**: `%s s`" % _fmt(collection.get("sample_interval_s")),
            "- **Platform**: `%s`" % collection.get("platform"),
            "- **CPU Count**: `%s`" % collection.get("cpu_count"),
            "- **Memory Probe**: `%s`" % collection.get("memory_probe"),
            "- **Mean Raw Load (1m / 5m / 15m)**: `%s / %s / %s`" % (_fmt(mean_load_1m), _fmt(mean_load_5m), _fmt(mean_load_15m)),
            "- **Peak Raw Load (1m)**: `%s`" % _fmt(max_load_1m),
            "- **Mean Normalized Load (1m)**: `%s`" % _fmt(mean_norm_1m),
            "- **Peak Normalized Load (1m)**: `%s`" % _fmt(max_norm_1m),
            "- **Mean Memory Total**: `%s`" % _fmt_bytes(mean_mem_total),
            "- **Mean Memory Available**: `%s`" % _fmt_bytes(mean_mem_available),
            "- **Minimum Memory Available**: `%s`" % _fmt_bytes(min_mem_available),
            "- **Mean Memory Used**: `%s`" % _fmt_bytes(mean_mem_used),
            "- **Peak Memory Used**: `%s`" % _fmt_bytes(max_mem_used),
            "- **Mean Memory Available Ratio**: `%s`" % _fmt(mean_mem_available_ratio),
            "- **Minimum Memory Available Ratio**: `%s`" % _fmt(min_mem_available_ratio),
            "- **Mean Memory Used Ratio**: `%s`" % _fmt(mean_mem_used_ratio),
            "- **Peak Memory Used Ratio**: `%s`" % _fmt(max_mem_used_ratio),
            "",
            "### System Load Samples",
            "",
            "| Metric | Mean | Min | Max | p50 | p95 |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        metric_rows = [
            ("Raw load 1m", "loadavg_1m", "number"),
            ("Raw load 5m", "loadavg_5m", "number"),
            ("Raw load 15m", "loadavg_15m", "number"),
            ("Normalized load 1m", "normalized_load_1m", "number"),
            ("Memory total", "memory_total_bytes", "bytes"),
            ("Memory available", "memory_available_bytes", "bytes"),
            ("Memory used", "memory_used_bytes", "bytes"),
            ("Memory available ratio", "memory_available_ratio", "number"),
            ("Memory used ratio", "memory_used_ratio", "number"),
        ]
        for label, key, value_type in metric_rows:
            stats = load_summary.get(key) or {}
            formatter = _fmt_bytes if value_type == "bytes" else _fmt
            lines.append(
                "| {label} | {mean} | {min_v} | {max_v} | {p50} | {p95} |".format(
                    label=label,
                    mean=formatter(stats.get("mean")),
                    min_v=formatter(stats.get("min")),
                    max_v=formatter(stats.get("max")),
                    p50=formatter(stats.get("p50")),
                    p95=formatter(stats.get("p95")),
                )
            )

    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(output_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")


def write_source_type_csv(summary: Dict[str, Any], output_path: str) -> None:
    import csv

    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    rows = []
    for source_type, stats in sorted(
        (summary.get("source_type_breakdown", {}) or {}).items(),
        key=lambda item: (((item[1].get("wall_time_ms") or {}).get("p95")) or 0.0),
        reverse=True,
    ):
        rows.append({
            "source_type": source_type,
            "file_count": stats.get("file_count", 0),
            "record_count": stats.get("record_count", 0),
            "wall_mean_ms": _blank_if_none(_deep_get(stats, "wall_time_ms", "mean")),
            "wall_p95_ms": _blank_if_none(_deep_get(stats, "wall_time_ms", "p95")),
            "parse_mean_ms": _blank_if_none(_deep_get(stats, "parse_stage_ms", "mean")),
            "resolver_mean_ms": _blank_if_none(_deep_get(stats, "resolver_stage_ms", "mean")),
            "db_mean_ms": _blank_if_none(_deep_get(stats, "db_stage_ms", "mean")),
            "throughput_records_per_minute": _blank_if_none(stats.get("throughput_records_per_minute")),
        })

    fieldnames = [
        "source_type",
        "file_count",
        "record_count",
        "wall_mean_ms",
        "wall_p95_ms",
        "parse_mean_ms",
        "resolver_mean_ms",
        "db_mean_ms",
        "throughput_records_per_minute",
    ]
    with open(output_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
