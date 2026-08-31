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

    def publish(self, envelope):
        self.envelopes.append(envelope)


class ArchiveKafkaReplayTest(unittest.TestCase):
    def test_replays_all_rows_as_trace_scoped_envelopes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            manifest = collect_archive_partition(
                OnePageClient(),
                ArchivePartition("CPI", "2026-08-12", "NVDA", "start", "end"),
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


if __name__ == "__main__":
    unittest.main()
