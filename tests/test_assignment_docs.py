import unittest
from pathlib import Path


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
        self.assertIn("실제 IEX 거래 427건 → final 1분 봉 3건", readme)
        self.assertIn("WebSocket → Kafka → Spark → PostgreSQL", readme)
        self.assertIn("다음 미국 정규장에 검증", readme)

    def test_architecture_source_distinguishes_current_and_planned_flow(self) -> None:
        diagram = Path("docs/diagrams/pipeline-architecture.svg").read_text(encoding="utf-8")

        self.assertIn("Alpaca IEX", diagram)
        self.assertIn("Kafka raw.market.v1", diagram)
        self.assertIn("Spark Streaming", diagram)
        self.assertIn("market_bars", diagram)
        self.assertIn("현재 구현·실행 가능", diagram)
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
        ):
            self.assertIn(required, patterns)


if __name__ == "__main__":
    unittest.main()
