import unittest
from pathlib import Path
from uuid import UUID

from src.cpi_w1_repository import Claim, CpiW1Repository, _validate_reason


SOURCE = Path("src/cpi_w1_repository.py").read_text(encoding="utf-8")


class CpiW1RepositoryTest(unittest.TestCase):
    def test_claim_sql_uses_skip_locked_and_due_or_expired_work(self) -> None:
        self.assertIn("FOR UPDATE OF w SKIP LOCKED", SOURCE)
        self.assertIn("w.next_claim_at <= CURRENT_TIMESTAMP", SOURCE)
        self.assertIn("w.lease_until <= CURRENT_TIMESTAMP", SOURCE)

    def test_claim_can_filter_by_work_family_prefix(self) -> None:
        self.assertIn("LEFT(w.work_key, CHAR_LENGTH(%s)) = %s", SOURCE)
        self.assertNotIn("w.work_key LIKE %s", SOURCE)
        self.assertIn("work_key_prefix", SOURCE)

    def test_current_claim_fencing_uses_generation_token_lease_and_attempt(self) -> None:
        for fragment in (
            "w.claim_generation = %s",
            "w.claim_token = %s",
            "w.lease_until > CURRENT_TIMESTAMP",
            "a.state = 'RUNNING'",
            "a.attempt_number = w.claim_generation",
        ):
            self.assertIn(fragment, SOURCE)

    def test_claim_starts_parent_run_with_database_clock(self) -> None:
        self.assertIn("state='RUNNING'", SOURCE)
        self.assertIn("started_at=COALESCE(started_at, CURRENT_TIMESTAMP)", SOURCE)

    def test_reclaim_closes_expired_attempt_before_new_attempt(self) -> None:
        self.assertIn("LEASE_EXPIRED_RECLAIM", SOURCE)
        close_pos = SOURCE.index("LEASE_EXPIRED_RECLAIM")
        new_attempt_pos = SOURCE.index("INSERT INTO ingestion_attempts", close_pos)
        self.assertLess(close_pos, new_attempt_pos)

    def test_retry_closes_attempt_before_returning_work_to_pending(self) -> None:
        retry_pos = SOURCE.index("def retry_claim")
        retry_body = SOURCE[retry_pos:]
        self.assertLess(
            retry_body.index("UPDATE ingestion_attempts"),
            retry_body.index("UPDATE ingestion_work_items"),
        )
        self.assertIn("state='PENDING'", retry_body)
        self.assertIn("reason_code=NULL", retry_body)

    def test_terminalization_orders_attempt_before_work(self) -> None:
        attempt_pos = SOURCE.index("UPDATE ingestion_attempts a")
        work_pos = SOURCE.index("UPDATE ingestion_work_items", attempt_pos)
        self.assertLess(attempt_pos, work_pos)

    def test_reason_contract(self) -> None:
        _validate_reason("SUCCEEDED", None)
        _validate_reason("FAILED", "SOURCE_MISMATCH")
        with self.assertRaises(ValueError):
            _validate_reason("FAILED", None)
        with self.assertRaises(ValueError):
            _validate_reason("FAILED", "lowercase")
        with self.assertRaises(ValueError):
            _validate_reason("SUCCEEDED", "SHOULD_NOT_EXIST")

    def test_claim_is_immutable_value_object(self) -> None:
        claim = Claim(
            work_item_id=UUID(int=1),
            run_id=UUID(int=2),
            attempt_id=UUID(int=3),
            execution_scope="ECONOMIC_PROMOTE",
            claim_generation=1,
            claim_token=UUID(int=4),
            input_artifact_id=UUID(int=5),
            work_key="CPI_RELEASE_ENVELOPE_PROMOTE:test",
        )
        with self.assertRaises(Exception):
            claim.claim_generation = 2  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
