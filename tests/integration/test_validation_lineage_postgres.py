import os
import unittest
from pathlib import Path

import psycopg


RUN_POSTGRES_INTEGRATION = os.environ.get("RUN_POSTGRES_INTEGRATION") == "1"
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://market:market@localhost:55432/market"
)


@unittest.skipUnless(
    RUN_POSTGRES_INTEGRATION,
    "set RUN_POSTGRES_INTEGRATION=1 to test a local PostgreSQL service",
)
class ValidationLineagePostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            for migration in sorted(Path("db/migrations").glob("*.sql")):
                connection.execute(migration.read_text(encoding="utf-8"))

    def setUp(self) -> None:
        with self.connection() as connection:
            connection.execute(
                "TRUNCATE validation_reconstructed_bars, validation_runs RESTART IDENTITY CASCADE"
            )

    def connection(self):
        return psycopg.connect(DATABASE_URL, autocommit=True)

    def record_run(self, connection, checkpoint="checkpoint:one") -> str:
        return connection.execute(
            """
            SELECT record_validation_run(
                'validation:run:1', 'W2_OPENING_HOUR',
                'raw_sip_reconstruction_v1', %s, 'manifest:sha256:one'
            )
            """,
            (checkpoint,),
        ).fetchone()[0]

    def record_bar(self, connection, close="102.0") -> int:
        return connection.execute(
            """
            SELECT record_validation_reconstructed_bar(
                'validation:run:1', 'NVDA', '2026-08-19 13:30:00+00', '1m',
                100, 105, 99, %s, 11, 4, 101.7, 'alpaca', 'sip', TRUE,
                'all_valid_trades_v1', 42
            )
            """,
            (close,),
        ).fetchone()[0]

    def test_T32_same_validation_run_different_checkpoint_is_rejected(self) -> None:
        with self.connection() as connection:
            self.record_run(connection)
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "VALIDATION_RUN_CONFLICT"):
                self.record_run(connection, checkpoint="checkpoint:other")

    def test_T33_same_reconstructed_bar_content_is_idempotent(self) -> None:
        with self.connection() as connection:
            self.record_run(connection)
            first = self.record_bar(connection)
            second = self.record_bar(connection)
            count = connection.execute(
                "SELECT count(*) FROM validation_reconstructed_bars"
            ).fetchone()[0]
        self.assertEqual(first, second)
        self.assertEqual(count, 1)

    def test_T34_same_reconstructed_bar_identity_different_ohlc_conflicts(self) -> None:
        with self.connection() as connection:
            self.record_run(connection)
            self.record_bar(connection)
            with self.assertRaisesRegex(psycopg.errors.UniqueViolation, "VALIDATION_BAR_DETERMINISM_CONFLICT"):
                self.record_bar(connection, close="103.0")

    def test_validation_run_and_bar_history_are_append_only(self) -> None:
        with self.connection() as connection:
            self.record_run(connection)
            self.record_bar(connection)
            with self.assertRaisesRegex(
                psycopg.errors.ObjectNotInPrerequisiteState, "append-only"
            ):
                connection.execute("UPDATE validation_runs SET workload_id='changed'")
            with self.assertRaisesRegex(
                psycopg.errors.ObjectNotInPrerequisiteState, "append-only"
            ):
                connection.execute("UPDATE validation_reconstructed_bars SET close=103")


if __name__ == "__main__":
    unittest.main()
