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
    def test_readme_leads_with_core_cpi_pipeline(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        headings = [
            "## 현재 분석 범위",
            "## 데이터 흐름",
            "## 데이터 출처",
            "## 실제 구현 결과",
            "## 실행 방법",
            "## 저장 모델",
            "## Kafka·Spark 보조 경로",
            "## 다음 단계",
        ]

        positions = [readme.index(heading) for heading in headings]

        self.assertEqual(positions, sorted(positions))

    def test_kafka_assignment_is_preserved_outside_core_readme(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        assignment = Path("docs/kafka-spark-assignment.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("CPI 과거 분석의 핵심 입력은 아니며", readme)
        self.assertIn("Kafka·Spark 과제 문서", readme)
        self.assertIn("실제 IEX 거래 10건", assignment)
        self.assertIn("Producer 427건 = Consumer 427건", assignment)
        self.assertIn("입력 427건, validation 오류 0건", assignment)
        self.assertIn("확정 1분봉 3건", assignment)

    def test_readme_documents_kafka_spark_assignment_contract(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        assignment = Path("docs/kafka-spark-assignment.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("### 4차시 과제 제출 요약", readme)
        self.assertIn("Producer 427건 = Consumer 427건", readme)
        self.assertIn("src.spark_market_processor", readme)
        self.assertIn("PostgreSQL `market.market_bars`", readme)
        self.assertIn("`raw.market.v1`", assignment)
        self.assertIn("`trace_id`", assignment)
        self.assertIn("`market.trade.raw`", assignment)
        self.assertIn("| 필드 | 타입 | 의미 |", assignment)
        self.assertIn('"event_type": "market.trade.raw"', assignment)
        self.assertIn("## 최종 저장 명세", assignment)
        self.assertIn("## 현재 구현과 다음 단계", assignment)
        self.assertIn("Spark Structured Streaming", assignment)
        self.assertIn("PostgreSQL market_bars", assignment)

        result = Path("docs/evidence/actual-ingestion/result.json").read_text(
            encoding="utf-8"
        )
        self.assertIn('"published_trades": 427', result)
        self.assertIn('"consumer_received_trades": 427', result)
        self.assertIn('"spark_input_trades": 427', result)
        self.assertIn('"spark_validation_error_trades": 0', result)

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
