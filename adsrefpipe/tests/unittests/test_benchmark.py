import types
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import sys

fake_requests = types.SimpleNamespace(
    post=lambda *args, **kwargs: None,
    get=lambda *args, **kwargs: None,
    exceptions=types.SimpleNamespace(RequestException=Exception),
)
sys.modules.setdefault("requests", fake_requests)
sys.modules.setdefault(
    "adsputils",
    types.SimpleNamespace(
        load_config=lambda *args, **kwargs: {},
        get_date=lambda *args, **kwargs: (
            datetime(1972, 1, 1) if args and args[0] == "1972" else datetime(2026, 5, 8)
        ),
        setup_logging=lambda *args, **kwargs: types.SimpleNamespace(info=lambda *a, **k: None, error=lambda *a, **k: None, debug=lambda *a, **k: None),
    ),
)

from adsrefpipe import benchmark
from adsrefpipe import perf_metrics


class TestBenchmark(unittest.TestCase):

    def test_collect_candidate_files_filters_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "one.raw")
            xml_path = os.path.join(tmpdir, "two.jats.xml")
            ignored_path = os.path.join(tmpdir, "three.raw.result")
            with open(raw_path, "w") as handle:
                handle.write("raw")
            with open(xml_path, "w") as handle:
                handle.write("xml")
            with open(ignored_path, "w") as handle:
                handle.write("ignored")

            files = benchmark.collect_candidate_files(tmpdir, ["*.raw", "*.xml"])
            self.assertEqual(files, sorted([raw_path, xml_path]))

    def test_classify_source_file_prefers_parser_extension(self):
        payload = benchmark.classify_source_file(
            "/tmp/foo.iop.xml",
            {"name": "IOP", "extension_pattern": ".iop.xml"},
        )
        self.assertEqual(payload["parser_name"], "IOP")
        self.assertEqual(payload["source_type"], ".iop.xml")

    def test_collect_candidate_files_with_days_back_ignores_filter_for_single_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "one.raw")
            with open(raw_path, "w") as handle:
                handle.write("raw")

            files = benchmark.collect_candidate_files_with_days_back(raw_path, ["*.raw"], days_back=1)
            self.assertEqual(files, [raw_path])

    def test_collect_candidate_files_with_days_back_filters_by_mtime(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            newer_dir = os.path.join(tmpdir, "recent")
            older_dir = os.path.join(tmpdir, "older")
            os.makedirs(newer_dir)
            os.makedirs(older_dir)
            new_path = os.path.join(newer_dir, "new.raw")
            old_path = os.path.join(older_dir, "old.raw")
            with open(new_path, "w") as handle:
                handle.write("new")
            with open(old_path, "w") as handle:
                handle.write("old")

            fake_now = datetime(2026, 5, 8, 12, 0, 0)
            cutoff = fake_now - timedelta(days=1)
            os.utime(new_path, (fake_now.timestamp(), fake_now.timestamp()))
            old_timestamp = (cutoff - timedelta(seconds=1)).timestamp()
            os.utime(old_path, (old_timestamp, old_timestamp))

            with patch.object(benchmark, "get_date", return_value=fake_now):
                files = benchmark.collect_candidate_files_with_days_back(tmpdir, ["*.raw"], days_back=1)

            self.assertEqual(files, [new_path])

    def test_collect_candidate_files_with_days_back_matches_run_group_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir_b = os.path.join(tmpdir, "b")
            subdir_a = os.path.join(tmpdir, "a")
            os.makedirs(subdir_b)
            os.makedirs(subdir_a)
            file_root = os.path.join(tmpdir, "root.raw")
            file_a = os.path.join(subdir_a, "a.raw")
            file_b = os.path.join(subdir_b, "b.raw")
            for current_path in [file_root, file_a, file_b]:
                with open(current_path, "w") as handle:
                    handle.write("x")

            fake_now = datetime(2026, 5, 8, 12, 0, 0)
            for current_path in [file_root, file_a, file_b]:
                os.utime(current_path, (fake_now.timestamp(), fake_now.timestamp()))

            with patch.object(benchmark, "get_date", return_value=fake_now):
                files = benchmark.collect_candidate_files_with_days_back(tmpdir, ["*.raw"], days_back=1)

            self.assertEqual(files, [file_root, file_a, file_b])

    def test_mock_resolver_returns_deterministic_payload(self):
        resolved = benchmark._mock_resolved_reference({"id": "H1I1", "refstr": "A ref"}, "http://example/text")
        self.assertEqual(resolved[0]["id"], "H1I1")
        self.assertEqual(resolved[0]["bibcode"], "2000mock........A")

    def test_progress_line_formatter_delegates_to_perf_metrics(self):
        self.assertIsNone(perf_metrics.format_benchmark_progress_line_from_log_line("not-json"))
        self.assertIsNone(perf_metrics.format_benchmark_progress_line_from_log_line(json.dumps({
            "timestamp": "2026-04-10T18:29:31.196Z",
            "message": "Updated 1 resolved reference records successfully.",
        })))
        self.assertEqual(
            perf_metrics.format_benchmark_progress_line_from_log_line(json.dumps({
                "timestamp": "2026-04-10T18:29:31.196Z",
                "message": (
                    "Source file /app/adsrefpipe/tests/unittests/stubdata/test.jats.xml "
                    "for bibcode 0000HiA.....Z with 16 references, processed successfully."
                ),
            })),
            "18:29:31 test.jats.xml with 16 references",
        )

    def test_run_case_mock_mode_collects_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_file = os.path.join(tmpdir, "sample.raw")
            with open(sample_file, "w") as handle:
                handle.write("content")
            events_path = os.path.join(tmpdir, "events.jsonl")

            def fake_process_files(files):
                for index, filename in enumerate(files, start=1):
                    extra = {
                        "source_filename": filename,
                        "source_type": ".raw",
                        "input_extension": ".raw",
                        "parser_name": "arXiv",
                        "record_count": 1,
                    }
                    benchmark.perf_metrics.emit_event("ingest_enqueue", record_id=None, extra=extra)
                    benchmark.perf_metrics.emit_event("parse_dispatch", record_id=None, duration_ms=5.0, extra=extra)
                    benchmark.perf_metrics.emit_event("resolver_http", record_id="rec-%s" % index, duration_ms=4.0, extra=extra)
                    benchmark.perf_metrics.emit_event("post_resolved_db", record_id="rec-%s" % index, duration_ms=3.0, extra=extra)
                    benchmark.perf_metrics.emit_event("record_wall", record_id="rec-%s" % index, duration_ms=12.0, extra=extra)
                    benchmark.perf_metrics.emit_event("file_wall", record_id=None, duration_ms=12.0, extra=extra)

            class FakePipelineRun:
                @staticmethod
                def process_files(files):
                    return fake_process_files(files)

            with patch.object(benchmark, "_pipeline_run_module", return_value=FakePipelineRun):
                summary = benchmark._run_case(
                    input_path=tmpdir,
                    extensions=["*.raw"],
                    days_back=None,
                    max_files=1,
                    mode="mock",
                    events_path=events_path,
                    system_sample_interval_s=0.01,
                    system_load_enabled=False,
                    warmup=False,
                    group_by="source_type",
                )

            self.assertEqual(summary["status"], "complete")
            self.assertEqual(summary["counts"]["files_processed"], 1)
            self.assertEqual(summary["counts"]["records_processed"], 1)
            self.assertIn(".raw", summary["source_type_breakdown"])
            self.assertGreater(summary["throughput"]["overall_records_per_minute"], 0)

    def test_cmd_run_prints_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input")
            os.makedirs(input_path)
            with open(os.path.join(input_path, "sample.raw"), "w") as handle:
                handle.write("content")

            fake_summary = {
                "status": "complete",
                "counts": {"files_selected": 1, "files_processed": 1, "records_submitted": 1, "records_processed": 1, "failures": 0},
                "throughput": {"overall_records_per_minute": 60.0, "load_adjusted_records_per_minute": 60.0},
                "duration_s": {"wall_clock": 1.0},
                "per_record_metrics_ms": {},
                "source_type_breakdown": {".raw": {"file_count": 1, "record_count": 1, "wall_time_ms": {}, "parse_stage_ms": {}, "resolver_stage_ms": {}, "db_stage_ms": {}, "throughput_records_per_minute": 60.0}},
                "parser_breakdown": {},
                "system_load": {"collection": {"enabled": False}, "summary": {}},
                "run_metadata": {"run_id": "run-1"},
            }

            args = benchmark.build_parser().parse_args([
                "run",
                "--input-path", input_path,
                "--output-dir", tmpdir,
                "--events-path", os.path.join(tmpdir, "events.jsonl"),
                "--no-warmup",
                "--disable-system-load",
            ])

            with patch.object(benchmark, "_run_case", return_value=fake_summary):
                with patch("sys.stdout.write") as mock_write:
                    rc = benchmark.cmd_run(args)

            self.assertEqual(rc, 0)
            rendered = "".join(call.args[0] for call in mock_write.call_args_list)
            self.assertIn('"status": "complete"', rendered)
            self.assertTrue(any(name.endswith(".json") for name in os.listdir(tmpdir)))
            self.assertTrue(any(name.endswith(".source_types.csv") for name in os.listdir(tmpdir)))

    def test_build_parser_accepts_days_back(self):
        args = benchmark.build_parser().parse_args([
            "run",
            "--days-back", "1",
        ])
        self.assertEqual(args.days_back, 1)

    def test_cmd_run_with_fake_pipeline_generates_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input")
            os.makedirs(input_path)
            sample_path = os.path.join(input_path, "sample.raw")
            with open(sample_path, "w") as handle:
                handle.write("content")
            events_path = os.path.join(tmpdir, "events.jsonl")

            class FakePipelineRun:
                @staticmethod
                def process_files(files):
                    for index, filename in enumerate(files, start=1):
                        extra = {
                            "source_filename": filename,
                            "source_type": ".raw",
                            "input_extension": ".raw",
                            "parser_name": "arXiv",
                            "record_count": 1,
                        }
                        benchmark.perf_metrics.emit_event("ingest_enqueue", record_id=None, extra=extra)
                        benchmark.perf_metrics.emit_event("parse_dispatch", record_id=None, duration_ms=5.0, extra=extra)
                        benchmark.perf_metrics.emit_event("resolver_http", record_id="rec-%s" % index, duration_ms=4.0, extra=extra)
                        benchmark.perf_metrics.emit_event("post_resolved_db", record_id="rec-%s" % index, duration_ms=3.0, extra=extra)
                        benchmark.perf_metrics.emit_event("record_wall", record_id="rec-%s" % index, duration_ms=12.0, extra=extra)
                        benchmark.perf_metrics.emit_event("file_wall", record_id=None, duration_ms=12.0, extra=extra)

            args = benchmark.build_parser().parse_args([
                "run",
                "--input-path", input_path,
                "--output-dir", tmpdir,
                "--events-path", events_path,
                "--days-back", "1",
                "--no-warmup",
                "--disable-system-load",
            ])

            with patch.object(benchmark, "_pipeline_run_module", return_value=FakePipelineRun):
                with patch.object(benchmark, "_safe_git_commit", return_value="deadbeef"):
                    with patch("sys.stdout.write") as mock_write:
                        rc = benchmark.cmd_run(args)

            self.assertEqual(rc, 0)
            rendered = "".join(call.args[0] for call in mock_write.call_args_list)
            self.assertIn('"status": "complete"', rendered)
            created_files = os.listdir(tmpdir)
            json_artifacts = [name for name in created_files if name.endswith(".json")]
            markdown_artifacts = [name for name in created_files if name.endswith(".md")]
            csv_artifacts = [name for name in created_files if name.endswith(".source_types.csv")]
            self.assertTrue(json_artifacts)
            self.assertTrue(markdown_artifacts)
            self.assertTrue(csv_artifacts)
            benchmark_json = os.path.join(tmpdir, sorted(json_artifacts)[0])
            with open(benchmark_json, "r") as handle:
                summary = json.load(handle)
            self.assertEqual(summary["status"], "complete")
            self.assertIn(".raw", summary["source_type_breakdown"])
            self.assertEqual(summary["run_metadata"]["days_back"], 1)

    def test_build_parser_rejects_invalid_sample_interval(self):
        parser = benchmark.build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args([
                "run",
                "--system-sample-interval", "0",
            ])

    def test_run_case_applies_max_files_after_days_back_filtering(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first_path = os.path.join(tmpdir, "first.raw")
            second_dir = os.path.join(tmpdir, "subdir")
            os.makedirs(second_dir)
            second_path = os.path.join(second_dir, "second.raw")
            for current_path in [first_path, second_path]:
                with open(current_path, "w") as handle:
                    handle.write("content")

            fake_now = datetime(2026, 5, 8, 12, 0, 0)
            for current_path in [first_path, second_path]:
                os.utime(current_path, (fake_now.timestamp(), fake_now.timestamp()))

            seen_files = []

            class FakePipelineRun:
                @staticmethod
                def process_files(files):
                    seen_files.extend(files)
                    for index, filename in enumerate(files, start=1):
                        extra = {
                            "source_filename": filename,
                            "source_type": ".raw",
                            "input_extension": ".raw",
                            "parser_name": "arXiv",
                            "record_count": 1,
                        }
                        benchmark.perf_metrics.emit_event("ingest_enqueue", record_id=None, extra=extra)
                        benchmark.perf_metrics.emit_event("record_wall", record_id="rec-%s" % index, duration_ms=5.0, extra=extra)
                        benchmark.perf_metrics.emit_event("file_wall", record_id=None, duration_ms=5.0, extra=extra)

            with patch.object(benchmark, "_pipeline_run_module", return_value=FakePipelineRun):
                with patch.object(benchmark, "get_date", return_value=fake_now):
                    summary = benchmark._run_case(
                        input_path=tmpdir,
                        extensions=["*.raw"],
                        days_back=1,
                        max_files=1,
                        mode="mock",
                        events_path=os.path.join(tmpdir, "events.jsonl"),
                        system_sample_interval_s=0.01,
                        system_load_enabled=False,
                        warmup=False,
                        group_by="source_type",
                    )

            self.assertEqual(summary["counts"]["files_selected"], 1)
            self.assertEqual(len(seen_files), 1)

    def test_run_case_warns_when_sampler_thread_stays_alive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_file = os.path.join(tmpdir, "sample.raw")
            with open(sample_file, "w") as handle:
                handle.write("content")
            events_path = os.path.join(tmpdir, "events.jsonl")

            def fake_process_files(files):
                extra = {
                    "source_filename": files[0],
                    "source_type": ".raw",
                    "input_extension": ".raw",
                    "parser_name": "arXiv",
                    "record_count": 1,
                }
                benchmark.perf_metrics.emit_event("ingest_enqueue", record_id=None, extra=extra)
                benchmark.perf_metrics.emit_event("record_wall", record_id="rec-1", duration_ms=5.0, extra=extra)

            class FakePipelineRun:
                @staticmethod
                def process_files(files):
                    return fake_process_files(files)

            class FakeSamplerThread:
                def __init__(self, *args, **kwargs):
                    self._alive = True

                def start(self):
                    return None

                def join(self, timeout=None):
                    return None

                def is_alive(self):
                    return self._alive

            with patch.object(benchmark, "_pipeline_run_module", return_value=FakePipelineRun):
                with patch.object(benchmark.threading, "Thread", return_value=FakeSamplerThread()):
                    with patch.object(benchmark, "LOGGER") as mock_logger:
                        summary = benchmark._run_case(
                            input_path=tmpdir,
                            extensions=["*.raw"],
                            days_back=None,
                            max_files=1,
                            mode="mock",
                            events_path=events_path,
                            system_sample_interval_s=0.1,
                            system_load_enabled=True,
                            warmup=False,
                            group_by="source_type",
                        )

            self.assertEqual(summary["status"], "complete")
            mock_logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
