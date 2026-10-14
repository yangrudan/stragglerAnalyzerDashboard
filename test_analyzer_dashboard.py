import json
import re
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import analyzer_dashboard as dashboard


SAMPLE = """straggler-analyzer dingtalk_enabled=true cooldown_seconds=300 queue_capacity=256 delivery_retry_initial_seconds=5 delivery_retry_max_seconds=60
straggler-analyzer bind=0.0.0.0:39091 required_slow_iterations=3 iteration_top_k=3 dingtalk_enabled=true suppressed_anomaly_log=/tmp/suppressed.jsonl
{"event":"straggler_iteration_timing","iteration_id":1,"actual_iteration_ms":100.0,"expected_iteration_ms":100.0,"overrun_ratio":0.0,"overrun_threshold":0.2,"slow":false,"consecutive_slow_iterations":0}
{"event":"straggler_iteration_timing","iteration_id":2,"actual_iteration_ms":140.0,"expected_iteration_ms":100.0,"overrun_ratio":0.4,"overrun_threshold":0.2,"slow":true,"consecutive_slow_iterations":1}
{"event":"straggler_iteration_timing","iteration_id":3,"actual_iteration_ms":150.0,"expected_iteration_ms":100.0,"overrun_ratio":0.5,"overrun_threshold":0.2,"slow":true,"consecutive_slow_iterations":2}
{"event":"straggler_iteration_timing","iteration_id":4,"actual_iteration_ms":160.0,"expected_iteration_ms":100.0,"overrun_ratio":0.6,"overrun_threshold":0.2,"slow":true,"consecutive_slow_iterations":3}
{"event":"straggler_iteration_slowdown","iteration_id":4,"actual_iteration_ms":160.0,"expected_iteration_ms":100.0,"overrun_ratio":0.6,"candidates":[{"global_rank":26,"hostname":"node-a","host_ip":"10.0.0.1","region":"forward_device_span","ratio":2.5,"explanation_ratio":0.7,"bad_windows":4,"high_confidence":true,"impact_confidence":true,"pair_peer":{"global_rank":18}}]}
{"event":"straggler_iteration_timing","iteration_id":5,"actual_iteration_ms":101.0,"expected_iteration_ms":100.0,"overrun_ratio":0.01,"overrun_threshold":0.2,"slow":false,"consecutive_slow_iterations":0}
{"event":"straggler_iteration_timing","iteration_id":6,"actual_iteration_ms":130.0,"expected_iteration_ms":100.0,"overrun_ratio":0.3,"overrun_threshold":0.2,"slow":true,"consecutive_slow_iterations":1}
straggler-analyzer dingtalk_notification_retrying job_id=job iteration_id=4 region=iteration_wall_time failed_round=1 retry_in_ms=5000 reason=request timeout
straggler-analyzer dingtalk_notification_sent_after_retry job_id=job iteration_id=4 region=iteration_wall_time failed_rounds=1
{"event":"straggler_analyzer_stats","regions":{"forward_device_span":{"records_received":64,"alerts_emitted":2,"alerts_suppressed_active_episode":3,"decisions_ratio_below_threshold":50}},"iteration_gate":{"hook_anomalies_recorded":0,"hook_anomalies_write_failed":0},"dingtalk":{"enqueued":1,"sent":1,"failed":1,"recovered_after_retry":1,"pending_delivery":0,"suppressed_cooldown":0,"dropped_queue_full":0}}
"""


class AnalyzerDashboardTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.log_path = Path(self.tempdir.name) / "analyzer.log"
        self.log_path.write_text(SAMPLE, encoding="utf-8")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_parse_and_derive(self):
        parsed = dashboard.parse_log(self.log_path)
        model = dashboard.build_model(parsed)

        self.assertEqual(len(model["iterations"]), 6)
        self.assertEqual(model["slow_count"], 4)
        self.assertEqual(model["normal_count"], 2)
        self.assertEqual(model["expected_ms"], 100.0)
        self.assertEqual(model["threshold_ms"], 120.0)
        self.assertEqual(model["min_actual_ms"], 100.0)
        self.assertEqual(model["max_actual_ms"], 160.0)
        self.assertEqual(
            [(item["start"], item["end"]) for item in model["episodes"]],
            [(2, 4), (6, 6)],
        )
        self.assertEqual(model["episodes"][0]["first_alert"], 4)
        self.assertIsNone(model["episodes"][1]["first_alert"])
        self.assertEqual(model["rank_summary"][0]["global_rank"], 26)
        self.assertEqual(model["rank_summary"][0]["peer_rank"], 18)
        self.assertEqual(len(model["dingtalk_events"]), 2)
        self.assertEqual(model["dingtalk_stats"]["sent"], 1)

    def test_render_is_self_contained(self):
        model = dashboard.build_model(dashboard.parse_log(self.log_path))
        report = dashboard.render_html(model, "Test Dashboard")

        self.assertIn("Test Dashboard", report)
        self.assertIn('id="iteration-chart"', report)
        self.assertIn("global 26", report)
        self.assertIn("第 1 轮失败", report)
        self.assertIn("const DATA=", report)
        self.assertNotIn("<script src=", report)
        self.assertNotIn("<link rel=", report)

    def test_jsonl_parser_counts_malformed_lines(self):
        path = Path(self.tempdir.name) / "suppressed.jsonl"
        path.write_text('{"event":"ok"}\nnot-json\n', encoding="utf-8")
        records, malformed = dashboard.parse_jsonl(path)
        self.assertEqual(records, [{"event": "ok"}])
        self.assertEqual(malformed, 1)

    def test_count_jsonl_matches_parser_with_bounded_memory(self):
        path = Path(self.tempdir.name) / "large.jsonl"
        payload = json.dumps({"payload": "x" * 1024}) + "\n"
        with path.open("w") as handle:
            for _ in range(10000):
                handle.write(payload)
            handle.write('\n[]\nnull\ninvalid\n')
        tracemalloc.start()
        try:
            self.assertEqual(dashboard.count_jsonl(path), (10000, 3))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 1024 * 1024)
        count, source, malformed = dashboard.load_suppressed(path, {})
        self.assertEqual((count, source, malformed), (10000, path, 3))
        model = dashboard.build_model(dashboard.parse_log(self.log_path), count)
        self.assertEqual(model["suppressed_count"], 10000)
        self.assertEqual(model["suppressed_records"], [])

    def test_sampling_preserves_extrema_anomalies_and_order(self):
        rows = [{"iteration_id": i, "actual_iteration_ms": 100, "slow": False}
                for i in range(10000)]
        rows[123]["actual_iteration_ms"] = 9999
        rows[456]["actual_iteration_ms"] = 1
        rows[789]["slow"] = True
        alerts = [{"iteration_id": 987}]
        sampled = dashboard.sample_iterations(rows, alerts, 100)
        ids = [row["iteration_id"] for row in sampled]
        self.assertEqual(ids, sorted(set(ids)))
        self.assertTrue({0, 9999, 123, 456, 789, 987}.issubset(ids))
        self.assertLessEqual(len(sampled), 102)
        self.assertIs(dashboard.sample_iterations(rows, alerts, 0), rows)
        self.assertEqual(dashboard.sample_iterations([], [], 100), [])
        for row in rows:
            row["slow"] = True
        self.assertEqual(dashboard.sample_iterations(rows, alerts, 100), rows)

    def test_sampling_changes_chart_only(self):
        model = dashboard.build_model(dashboard.parse_log(self.log_path))
        before = model["iterations"].copy()
        report = dashboard.render_html(model, "Sampled", 4)
        data = json.loads(re.search(r"const DATA=(.*);", report).group(1))
        self.assertEqual(model["iterations"], before)
        self.assertEqual(len(model["iterations"]), 6)
        self.assertTrue({2, 3, 4, 6}.issubset({row["id"] for row in data["iterations"]}))
        self.assertIn("统计基于全量数据", report)
        with self.assertRaises(ValueError):
            dashboard.render_html(model, "Invalid", 3)



if __name__ == "__main__":
    unittest.main()
