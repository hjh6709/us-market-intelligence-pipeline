import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.archive_kafka_replay import replay_archive
from src.market_trade_archive import ArchivePartition, collect_archive_partition


class OnePageClient:
    def fetch_page(self, **_kwargs):
        return ([{"i": 1, "x": "V", "p": 100.0, "s": 10, "c": ["@"], "t": "2026-08-12T12:30:00Z", "z": "C"}], None)


class RecordingPublisher:
    def __init__(self):
        self.envelopes = []

        self.keys = []

    def publish(self, envelope, *, key=None):
        self.envelopes.append(envelope)

        self.keys.append(key)


class ArchiveKafkaReplayTest(unittest.TestCase):
    def test_replays_all_rows_as_trace_scoped_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            manifest = collect_archive_partition(
                OnePageClient(),
                ArchivePartition(
                    "CPI",
                    "2026-08-12",
                    "NVDA",
                    "2026-08-12T11:30:00Z",
                    "2026-08-12T13:31:00Z",
                ),
                archive_root=Path(temp),
            )
            publisher = RecordingPublisher()

            result = replay_archive(
                [manifest],
                publisher=publisher,
                trace_id="load-1",
                clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            )

        self.assertEqual(result.published_trades, 1)
        self.assertEqual(publisher.envelopes[0]["trace_id"], "load-1")
        self.assertEqual(publisher.envelopes[0]["feed"], "sip")

        self.assertEqual(
            publisher.keys,
            ["CPI|2026-08-12|NVDA|segment-04"],
        )

    def test_uses_deterministic_fifteen_minute_segments(self) -> None:
        class ThreeTradesClient:
            def fetch_page(self, **_kwargs):
                return (
                    [
                        {"i": 1, "x": "V", "p": 100.0, "s": 10, "c": ["@"], "t": "2026-08-12T11:30:00Z", "z": "C"},
                        {"i": 2, "x": "V", "p": 100.1, "s": 10, "c": ["@"], "t": "2026-08-12T11:44:59Z", "z": "C"},
                        {"i": 3, "x": "V", "p": 100.2, "s": 10, "c": ["@"], "t": "2026-08-12T11:45:00Z", "z": "C"},
                    ],
                    None,
                )

        with tempfile.TemporaryDirectory() as temp:
            manifest = collect_archive_partition(
                ThreeTradesClient(),
                ArchivePartition(
                    "CPI",
                    "2026-08-12",
                    "NVDA",
                    "2026-08-12T10:30:00Z",
                    "2026-08-12T13:31:00Z",
                ),
                archive_root=Path(temp),
            )
            publisher = RecordingPublisher()
            replay_archive(
                [manifest],
                publisher=publisher,
                trace_id="load-2",
                clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            )

        self.assertEqual(
            publisher.keys,
            [
                "CPI|2026-08-12|NVDA|segment-04",
                "CPI|2026-08-12|NVDA|segment-04",
                "CPI|2026-08-12|NVDA|segment-05",
            ],
        )


if __name__ == "__main__":
    unittest.main()
