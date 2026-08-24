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
    def test_readme_connects_core_cpi_and_kafka_spark_paths(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        headings = [
            "## 프로젝트 목표",
            "## 현재 분석 범위",
            "## 데이터 흐름",
            "## 데이터 출처",
            "## 실제 구현 결과",
            "## 실행 방법",
            "## 저장 모델",
            "## 다음 단계",
            "## 구현·과제 증거",
        ]

        positions = [readme.index(heading) for heading in headings]

        self.assertEqual(positions, sorted(positions))
        self.assertIn(
            "![전체 프로젝트 데이터 파이프라인 아키텍처]"
            "(docs/diagrams/pipeline-architecture.png)",
            readme,
        )
        self.assertNotIn(
            "![CPI 발표 구간 SIP Kafka Spark 처리 경로]",
            readme,
        )

    def test_kafka_assignment_uses_the_cpi_release_window(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        assignment = Path("docs/kafka-spark-assignment.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("README는 프로젝트 전체 구조와 실행 진입점만 설명", readme)
        self.assertIn("4차시 Kafka·Spark 과제 문서", readme)
        self.assertIn("실제 IEX 거래 10건", assignment)
        self.assertIn("IEX와 SIP 중 왜 SIP를 사용했는가", assignment)
        self.assertIn("Historical Trades REST API", assignment)
        self.assertIn("SIP WebSocket을 실시간으로 구독한 결과도 아니며", assignment)
        self.assertIn("SIP의 전체 범위와 이번 API 조회 범위는 다르다", assignment)
        self.assertIn("미국 전체 종목의 모든 데이터를 이번 실행에서 한꺼번에 받았다는 뜻은 아니다", assignment)
        self.assertIn("호가, 전체 주문장·미체결 주문", assignment)
        self.assertIn("Producer 58,036건 = Consumer 58,036건", assignment)
        self.assertIn("입력 58,036건 → validation 오류 0건", assignment)
        self.assertIn("1분봉 121건", assignment)
        self.assertIn("volume/trade_count 반영 58,034건", assignment)
        self.assertIn("OHLC/VWAP 가격 형성 반영 8,752건", assignment)
        self.assertIn("`I(Odd Lot)`", assignment)
        self.assertIn("OHLC·volume·trade_count·VWAP 불일치 모두 0건", assignment)

    def test_readme_documents_kafka_spark_assignment_contract(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        assignment = Path("docs/kafka-spark-assignment.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("## 구현·과제 증거", readme)
        self.assertNotIn("### 4차시 과제 제출 요약", readme)
        self.assertIn("docs/kafka-spark-assignment.md", readme)
        self.assertIn("`raw.market-sip.v1`", assignment)
        self.assertIn("`trace_id`", assignment)
        self.assertIn("`market.trade.raw`", assignment)
        self.assertIn("| 필드 | 타입 | 의미 |", assignment)
        self.assertIn('"event_type": "market.trade.raw"', assignment)
        self.assertIn("## 최종 저장 명세", assignment)
        self.assertIn("## 현재 구현과 다음 단계", assignment)
        self.assertIn("Spark batch", assignment)
        self.assertIn("PostgreSQL market_bars", assignment)

        result = Path("docs/evidence/cpi-kafka-spark/result.json").read_text(
            encoding="utf-8"
        )
        self.assertIn('"published_trades": 58036', result)
        self.assertIn('"consumer_received_trades": 58036', result)
        self.assertIn('"spark_input_trades": 58036', result)
        self.assertIn('"spark_validation_error_trades": 0', result)
        self.assertIn('"spark_volume_eligible_trades": 58034', result)
        self.assertIn('"spark_price_eligible_trades": 8752', result)
        self.assertIn('"spark_output_bars": 121', result)
        self.assertIn('"requested_end": "2026-08-12T13:31:00Z"', result)
        self.assertIn('"provider_bar_rows": 121', result)
        self.assertIn('"provider_bar_trade_count_sum": 58034', result)
        self.assertIn('"provider_bar_timeframe": "1Min"', result)
        self.assertIn('"provider_replay_ohlc_mismatch_bars": 0', result)

        evidence_sql = Path(
            "scripts/evidence/cpi_sip_kafka_spark_evidence.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("source = 'alpaca_replay'", evidence_sql)
        self.assertIn("feed = 'sip'", evidence_sql)
        self.assertIn("HAVING count(*) > 1", evidence_sql)

    def test_architecture_source_distinguishes_current_and_planned_flow(self) -> None:
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(encoding="utf-8")

        self.assertIn("BLS · ALFRED · Alpaca SIP", diagram)
        self.assertIn("raw.market-sip.v1", diagram)
        self.assertIn("Spark Batch / Streaming", diagram)
        self.assertIn("market_bars", diagram)
        self.assertIn("economic_events", diagram)
        self.assertIn("macro_event_impacts", diagram)
        self.assertIn("현재 구현", diagram)
        self.assertIn("후속 확장", diagram)
        self.assertIn("Airflow orchestration", diagram)

        assignment_diagram = Path(
            "docs/diagrams/cpi-sip-kafka-spark-assignment.svg"
        ).read_text(encoding="utf-8")
        self.assertIn("volume·trade_count 58,034", assignment_diagram)
        self.assertIn("OHLC·VWAP 8,752", assignment_diagram)
        self.assertIn("provider parity", assignment_diagram)

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
