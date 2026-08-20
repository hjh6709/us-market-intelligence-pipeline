import hashlib
import unittest
from argparse import ArgumentTypeError
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.evidence.export_actual_market_bars import (
    canonical_row,
    local_output_path,
    write_export,
)


class AssignmentDocumentationTest(unittest.TestCase):
    def test_readme_has_assignment_summary_in_explanation_order(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        headings = [
            "## 이번 과제 목표",
            "## 이번 과제 데이터셋",
            "## 이번 과제 아키텍처",
            "## 이번 과제 실행 방법",
            "## 현재 구현 범위",
            "## 다음 단계",
            "## 고민한 부분과 현재 이슈",
        ]

        positions = [readme.index(heading) for heading in headings]

        self.assertEqual(positions, sorted(positions))

    def test_readme_separates_live_and_historical_evidence(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")

        self.assertIn("WebSocket → Kafka", readme)
        self.assertIn("실제 IEX 거래 10건", readme)
        self.assertIn("Historical REST → Kafka → Spark → PostgreSQL", readme)
        self.assertIn(
            "실제 IEX 거래 427건 발행, 그중 174건이 final 1분 봉 3건에 반영",
            readme,
        )
        self.assertIn("WebSocket → Kafka → Spark → PostgreSQL", readme)
        self.assertIn("다음 미국 정규장에 검증", readme)

    def test_architecture_source_distinguishes_current_and_planned_flow(self) -> None:
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(encoding="utf-8")

        self.assertIn("Alpaca IEX", diagram)
        self.assertIn("Kafka raw.market.v1", diagram)
        self.assertIn("Spark Streaming", diagram)
        self.assertIn("market_bars", diagram)
        self.assertIn("현재 구현 경로", diagram)
        self.assertIn("WebSocket · Kafka까지 검증", diagram)
        self.assertIn("Historical REST · DB까지 검증", diagram)
        self.assertIn("다음 단계", diagram)
        self.assertIn("Airflow Batch", diagram)
        self.assertIn("Analysis / BI", diagram)

    def test_gitignore_excludes_local_secrets_and_runtime_artifacts(self) -> None:
        patterns = Path(".gitignore").read_text(encoding="utf-8").splitlines()

        for required in (
            ".env*",
            "!.env.example",
            "airflow-runtime/",
            "logs/",
            "*.db",
            "*.sqlite*",
            "*.dump",
            "*.backup",
            "docs/evidence/**/captures/",
            "/data/local/",
            "/data/raw/",
        ):
            self.assertIn(required, patterns)

        self.assertEqual(
            local_output_path("data/local/actual_market_bars.csv"),
            Path("data/local/actual_market_bars.csv"),
        )
        with self.assertRaises(ArgumentTypeError):
            local_output_path("data/sample/actual_market_bars.csv")

        data_readme = Path("data/README.md").read_text(encoding="utf-8")
        sample = Path("data/sample/market_bars.synthetic.csv").read_text(
            encoding="utf-8"
        )
        self.assertIn("합성 예시", data_readme)
        self.assertIn("synthetic,fixture", sample)

        row = {
            "symbol": "DEMO",
            "bar_start": datetime(2026, 8, 20, 13, 30, tzinfo=timezone.utc),
            "timeframe": "1m",
            "open": Decimal("100.000000"),
            "high": Decimal("101.000000"),
            "low": Decimal("99.500000"),
            "close": Decimal("100.500000"),
            "volume": 1200,
            "trade_count": 24,
            "vwap": Decimal("100.250000"),
            "source": "synthetic",
            "feed": "fixture",
            "is_final": True,
        }
        expected_hash = hashlib.sha256(canonical_row(row).encode("utf-8")).hexdigest()
        Path("data/local").mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir="data/local") as temp_dir:
            output = Path(temp_dir) / "bars.csv"
            matching = write_export([row], output, expected_hash)
            mismatching = write_export([row], output, "not-the-same-hash")
        self.assertTrue(matching["hash_matches_expected"])
        self.assertFalse(mismatching["hash_matches_expected"])


if __name__ == "__main__":
    unittest.main()
