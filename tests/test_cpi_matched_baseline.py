import unittest
from datetime import UTC, date, datetime

from src.cpi_ingestion import CpiRelease
from src.cpi_matched_baseline import matched_control_time


class CpiMatchedBaselineTest(unittest.TestCase):
    def test_matches_same_new_york_wall_time_across_dst_change(self) -> None:
        release = CpiRelease(
            reference_period="2026-02",
            release_date=date(2026, 3, 11),
            released_at=datetime(2026, 3, 11, 12, 30, tzinfo=UTC),
            timezone="America/New_York",
            source_url="https://www.bls.gov/",
        )

        control = matched_control_time(release, 1)

        self.assertEqual(control, datetime(2026, 3, 4, 13, 30, tzinfo=UTC))

    def test_rejects_offset_outside_configured_three_controls(self) -> None:
        release = CpiRelease(
            reference_period="2026-07",
            release_date=date(2026, 8, 12),
            released_at=datetime(2026, 8, 12, 12, 30, tzinfo=UTC),
            timezone="America/New_York",
            source_url="https://www.bls.gov/",
        )

        with self.assertRaisesRegex(ValueError, "1, 2, or 3"):
            matched_control_time(release, 4)


if __name__ == "__main__":
    unittest.main()
