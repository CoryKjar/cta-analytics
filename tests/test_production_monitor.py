import unittest

from src.monitoring.verify_production import event_kind


class ProductionMonitorTests(unittest.TestCase):
    def test_only_clean_completion_is_accepted(self):
        self.assertEqual(event_kind("cta-analytics", "event=pipeline_completed uploaded_files=143 loaded_files=140 zero_arrival_files=3 parse_failures=0"), "collector")
        self.assertEqual(event_kind("cta-analytics", "event=pipeline_completed uploaded_files=142 loaded_files=142 parse_failures=0"), "error")
        self.assertEqual(event_kind("cta-analytics", "event=pipeline_completed parse_failures=1"), "error")
        self.assertIsNone(event_kind("cta-analytics", "event=pipeline_started"))

    def test_health_issues_and_recovery_failures_fail_gate(self):
        self.assertEqual(event_kind("cta-monitor", "Health check complete: issues=0"), "monitor")
        self.assertEqual(event_kind("cta-monitor", "Health check complete: issues=1"), "error")
        self.assertEqual(event_kind("cta-recovery", "load_failures: 1"), "error")
        self.assertEqual(event_kind("cta-recovery", "Unprocessed candidates skipped due to attempt cap: 0"), "recovery")


if __name__ == "__main__":
    unittest.main()