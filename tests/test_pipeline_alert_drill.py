import unittest

from scripts.run_pipeline_alert_drill import run_alert_drill


class FakeTrackingStore:
    def __init__(self) -> None:
        self.work_status = "PENDING"
        self.alert_status = "NONE"
        self.rows = {}

    def begin(self) -> None:
        self.work_status = "RUNNING"

    def record_failure(self, error: Exception) -> None:
        self.work_status = "FAILED"
        self.alert_status = "OPEN"

    def record_success(self, rows: list[dict]) -> None:
        for row in rows:
            key = (row["symbol"], row["bar_start"], row["timeframe"])
            self.rows[key] = row
        self.work_status = "SUCCEEDED"
        self.alert_status = "RESOLVED"

    def snapshot(self) -> dict:
        return {
            "work_status": self.work_status,
            "alert_status": self.alert_status,
            "stored_rows": len(self.rows),
            "business_key_duplicates": 0,
        }


class Always503Client:
    def fetch(self) -> list[dict]:
        raise RuntimeError("simulated provider HTTP 503")


class FixtureClient:
    def fetch(self) -> list[dict]:
        return [
            {
                "symbol": "SPY",
                "bar_start": "2026-07-29T17:00:00Z",
                "timeframe": "1m",
            }
        ]


class PipelineAlertDrillTest(unittest.TestCase):
    def test_api_failure_opens_alert_and_retry_resolves_without_duplicates(self) -> None:
        failure, recovery = run_alert_drill(
            database=FakeTrackingStore(),
            first_client=Always503Client(),
            retry_client=FixtureClient(),
        )

        self.assertEqual(failure["work_status"], "FAILED")
        self.assertEqual(failure["alert_status"], "OPEN")
        self.assertEqual(recovery["work_status"], "SUCCEEDED")
        self.assertEqual(recovery["alert_status"], "RESOLVED")
        self.assertEqual(recovery["business_key_duplicates"], 0)
        self.assertEqual(recovery["stored_rows"], 1)

    def test_unexpected_first_success_fails_the_drill(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "did not fail"):
            run_alert_drill(
                database=FakeTrackingStore(),
                first_client=FixtureClient(),
                retry_client=FixtureClient(),
            )


if __name__ == "__main__":
    unittest.main()
