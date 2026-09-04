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
    def test_seventh_assignment_documents_actual_serving_and_boundaries(self) -> None:
        assignment = Path("docs/serving-layer-assignment.md").read_text(
            encoding="utf-8"
        )
        script = Path("docs/09.07_대본.md").read_text(encoding="utf-8")
        readme = Path("README.md").read_text(encoding="utf-8")
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(
            encoding="utf-8"
        )

        for phrase in (
            "입력 → 처리 → 저장 → 읽기",
            "RESEARCH_ONLY",
            "NO_TRADE",
            "모의주문",
            "실전 자동매매",
            "0.30초",
            "180개",
        ):
            self.assertIn(phrase, assignment)
        self.assertIn("docs/serving-layer-assignment.md", readme)
        self.assertIn("Serving API · Dashboard", diagram)
        self.assertIn("향후 주문 계층", diagram)
        self.assertIn("RESEARCH_ONLY / NO_TRADE", script)
        self.assertNotIn("\nㅋ\n", script)

    def test_sixth_assignment_separates_executed_paths_and_remaining_work(self) -> None:
        assignment = Path("docs/load-recovery-assignment.md").read_text(
            encoding="utf-8"
        )
        readme = Path("README.md").read_text(encoding="utf-8")
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(
            encoding="utf-8"
        )
        script = Path("docs/09.03_대본.md").read_text(encoding="utf-8")

        for phrase in (
            "# 6차시 과제 — 부하·복구 결과 보완 및 전체 흐름 점검",
            "## 1. 기준 실행과 부하·복구 결과",
            "## 2. 이번에 확장한 실제 데이터",
            "## 3. 장애 재현과 복구",
            "## 4. Airflow 자동화와 확인 결과",
            "## 6. 최신 데이터 모델과 남은 작업",
            "118,118",
            "7,360,804",
            "22,260",
            "202회 × 10종목",
            "308,512",
            "112,593",
            "70,090",
            "30,250",
            "FAILED / OPEN → SUCCEEDED / RESOLVED",
            "경제발표 1건",
            "33.9%",
            "전망치와 실제값",
            "백테스트",
        ):
            self.assertIn(phrase, assignment)

        self.assertIn("docs/load-recovery-assignment.md", readme)
        self.assertIn("Kafka v2 · 6 partitions", diagram)
        self.assertIn("CPI 55 · 고용 55 · PCE 55 · FOMC 37", diagram)
        self.assertIn("Surprise · 통제군 → Paper trading", diagram)
        self.assertIn("7,360,804", script)
        self.assertIn("OPEN", script)
        self.assertIn("RESOLVED", script)

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
        self.assertIn("### A. Kafka·Spark 원시 체결 처리", readme)
        self.assertIn("### B. 202개 발표·10종목 시장 데이터", readme)
        self.assertIn("### C. Airflow 자동화", readme)
        self.assertIn("발표-종목 구간", readme)
        self.assertIn("308,512", readme)
        self.assertIn("118,118 / 118,118", readme)
        self.assertIn("uv sync --extra airflow", readme)
        self.assertIn("airflow dags test market_context_backfill_pipeline", readme)
        self.assertIn("market_context_backfill_orchestrator", readme)

    def test_kafka_assignment_uses_the_cpi_release_window(self) -> None:
        readme = Path("README.md").read_text(encoding="utf-8")
        assignment = Path("docs/kafka-spark-assignment.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("3차시 Kafka·Spark 과제", readme)
        self.assertIn("# 3차시 Kafka·Spark 과제", assignment)
        self.assertIn("실제 IEX 거래 10건", assignment)
        self.assertIn("## 전체 프로젝트에서의 과제 경계", assignment)
        self.assertIn("CPI 발표 시각과 분석 맥락을 제공. Kafka·Spark 입력이 아님", assignment)
        self.assertIn("Kafka에 들어가는 메시지는 NVDA의 개별 SIP 체결뿐", assignment)
        self.assertIn("현재 A의 영향 분석 입력을 대체하지 않고", assignment)
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
        self.assertIn("## Spark 전처리·1분봉 집계", assignment)
        self.assertIn("Kafka 58,036 → parsed 58,036", assignment)
        self.assertIn("valid 58,036 → unique 58,036, duplicate 0", assignment)
        self.assertIn("### Validation 실패 처리", assignment)
        self.assertIn("### 거래 조건에 따른 필드별 반영", assignment)
        self.assertIn("Odd Lot 등 가격 제외 체결", assignment)
        self.assertIn("### 1분봉 계산 규칙", assignment)
        self.assertIn("condition_policy=alpaca_sip_minute_v1", assignment)

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

    def test_airflow_assignment_documents_multi_symbol_mapped_run(self) -> None:
        assignment = Path("docs/airflow-assignment.md").read_text(encoding="utf-8")
        evidence = Path(
            "docs/evidence/airflow-market-replay/README.md"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "# 4차시 과제 — 지금 만든 작업을 Airflow로 자동화하기",
            assignment,
        )
        self.assertIn(
            "[`dags/market_replay_pipeline.py`](../dags/market_replay_pipeline.py)",
            assignment,
        )
        self.assertIn("한 번의 DAG 실행에 종목 목록", assignment)
        self.assertIn('`tickers=[SPY, QQQ, SMH, NVDA]`', assignment)
        self.assertIn("**118,118**", assignment)
        self.assertIn("총 472개의 1분봉", assignment)
        self.assertIn("왜 121개보다 적은 종목이 있는가", assignment)
        self.assertIn("SPY | 07:33 | 9 | 9 | 0", assignment)
        self.assertIn("SPY | 07:37 | 31 | 31 | 0", assignment)
        self.assertIn("SMH | 08:22 | 3 | 3 | 0", assignment)
        self.assertIn("이번 12개 분의 모든 체결에는 `c` 배열 안에 대문자 `I`", assignment)
        self.assertIn("소문자 `i` 필드는 개별 체결을 식별하는", assignment)
        self.assertIn("`c`(trade conditions) 배열 안의 대문자 `I`", assignment)
        self.assertIn("Alpaca 1분봉 API 교차 확인(저장 아님)", assignment)
        self.assertIn("교차 확인한 Alpaca 1분봉 API에도", assignment)
        self.assertIn("2026-08-12T11:30:00Z", assignment)
        self.assertIn("2026-08-12T13:31:00Z", assignment)
        self.assertNotIn("10분 구간", assignment)
        self.assertIn('`tickers=[SPY, QQQ]`', assignment)
        self.assertIn(
            "![Airflow 실행 A — 네 종목 mapped task 성공 화면]"
            "(evidence/airflow-market-replay/airflow-run-a-four-symbols.png)",
            assignment,
        )
        self.assertIn(
            "![Airflow 실행 B — SPY와 QQQ로 입력값을 바꾼 재실행 화면]"
            "(evidence/airflow-market-replay/airflow-run-b-changed-input.png)",
            assignment,
        )
        self.assertIn("한 번의 DAG 실행에서 종목별 작업", assignment)
        self.assertIn("DAG 코드를 수정하지 않고", assignment)
        self.assertNotIn('"ticker":"NVDA"', assignment)
        self.assertNotIn("ticker만 SPY로 변경", assignment)

        self.assertIn("manual__2026-08-27T07:56:50.333255+00:00", evidence)
        self.assertIn("manual__2026-08-27T07:58:07.693993+00:00", evidence)
        self.assertIn("21,270 | 27,638 | 11,174 | 58,036 | 118,118", evidence)
        self.assertTrue(
            Path(
                "docs/evidence/airflow-market-replay/airflow-run-a-four-symbols.png"
            ).is_file()
        )
        self.assertTrue(
            Path(
                "docs/evidence/airflow-market-replay/airflow-run-b-changed-input.png"
            ).is_file()
        )

    def test_architecture_source_distinguishes_current_and_planned_flow(self) -> None:
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(encoding="utf-8")

        self.assertIn("BLS · BEA · Federal Reserve", diagram)
        self.assertIn("Kafka v2 · 6 partitions", diagram)
        self.assertIn("Spark", diagram)
        self.assertIn("market_bars", diagram)
        self.assertIn("Official event catalog", diagram)
        self.assertIn("macro_event_contexts", diagram)
        self.assertIn("Surprise · 통제군 → Paper trading", diagram)
        self.assertIn("비용 포함 backtest", diagram)
        self.assertIn("Airflow / Python", diagram)

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
