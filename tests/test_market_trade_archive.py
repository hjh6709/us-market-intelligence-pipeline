import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.market_trade_archive import (
    ArchivePartition,
    collect_archive_partition,
    read_archive_records,
)


class PagingClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls = 0

    def fetch_page(self, **_kwargs):
        page = self.pages[self.calls]
        self.calls += 1
        return page


def trade(trade_id: int, timestamp: str) -> dict:
    return {
        "i": trade_id,
        "x": "V",
        "p": 100.0 + trade_id,
        "s": 10,
        "c": ["@"],
        "t": timestamp,
        "z": "C",
    }


class MarketTradeArchiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.spec = ArchivePartition(
            event_type="CPI",
            release_date="2026-08-12",
            symbol="NVDA",
            start="2026-08-12T11:30:00Z",
            end="2026-08-12T13:31:00Z",
            feed="sip",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_manifest_count_and_hash_match_parquet_rows(self) -> None:
        client = PagingClient(
            [
                ([trade(1, "2026-08-12T11:30:01Z")], "next"),
                ([trade(2, "2026-08-12T11:30:02Z")], None),
            ]
        )

        manifest = collect_archive_partition(client, self.spec, archive_root=self.root)

        self.assertEqual(manifest.row_count, 2)
        self.assertEqual(manifest.page_count, 2)
        self.assertEqual(len(manifest.sha256), 64)
        records = list(read_archive_records(manifest))
        self.assertEqual([item["i"] for item in records], [1, 2])

    def test_completed_hash_matching_partition_skips_api(self) -> None:
        first_client = PagingClient(
            [([trade(1, "2026-08-12T11:30:01Z")], None)]
        )
        first = collect_archive_partition(first_client, self.spec, archive_root=self.root)

        second = collect_archive_partition(
            PagingClient([]), self.spec, archive_root=self.root
        )

        self.assertEqual(second, first)

    def test_page_limit_never_leaves_completed_archive(self) -> None:
        client = PagingClient(
            [([trade(1, "2026-08-12T11:30:01Z")], "still-more")]
        )

        with self.assertRaisesRegex(RuntimeError, "page limit"):
            collect_archive_partition(
                client, self.spec, archive_root=self.root, max_pages=1
            )

        partition_dir = self.root / "event_type=CPI" / "release_date=2026-08-12" / "symbol=NVDA"
        self.assertFalse((partition_dir / "trades.parquet").exists())
        self.assertFalse((partition_dir / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
