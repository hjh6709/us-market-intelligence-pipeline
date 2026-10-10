"""R1 DB checks only in UUID-named disposable databases, never in the supplied DB."""
import os
from pathlib import Path
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from src.cpi_w1_evidence_policy import canonical_evidence_bytes
from src import cpi_w1_evidence_snapshot_v2
from tests.test_cpi_w1_evidence_snapshot_v2 import v2_case


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_INTEGRATION") == "1", "isolated PostgreSQL opt-in required")
class CpiEvidenceV2PostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_dsn = os.environ.get("DATABASE_URL", "postgresql://market:market@localhost:55435/market")
        cls.name = "cpi_r1_v2_" + uuid4().hex[:12]
        with psycopg.connect(cls.admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(cls.name)))
        parameters = conninfo_to_dict(cls.admin_dsn)
        parameters["dbname"] = cls.name
        cls.dsn = make_conninfo(**parameters)
        try:
            with psycopg.connect(cls.dsn) as connection:
                for path in sorted(Path("db/migrations").glob("[0-9][0-9][0-9]_*.sql")):
                    connection.execute(path.read_text())
                cls.initial_registrations = connection.execute("SELECT count(*) FROM promotion_capability_evidence_policy_registrations").fetchone()[0]
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        with psycopg.connect(cls.admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.name)))

    def setUp(self):
        self.connection = psycopg.connect(self.dsn)
        self.addCleanup(self.connection.close)

    def require_v2(self):
        present = self.connection.execute("SELECT to_regprocedure('validate_cpi_evidence_canonical_bytes(bytea)')").fetchone()[0]
        self.assertIsNotNone(present, "missing migration 014 canonical bytes guard")

    def test_python_postgres_canonical_bytes_vectors(self):
        self.require_v2()
        for value in ({"z": [True, None, 3], "a": {"text": 'é\n"\\'}}, "e\u0301", "é", {"b": {"z": 1, "a": 2}, "a": []}):
            raw = canonical_evidence_bytes(value)
            parsed = self.connection.execute("SELECT validate_cpi_evidence_canonical_bytes(%s)", (raw,)).fetchone()[0]
            self.assertEqual(parsed, value)

    def test_duplicate_nested_escaped_and_forbidden_numbers_rejected_before_jsonb(self):
        self.require_v2()
        for raw in (b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}', b'{"a":1,"\\u0061":2}',
                    b'1.0', b'1e0', b'-0', b'NaN', b'{"b":2,"a":1}', b'{"a":"\\u00e9"}'):
            with self.subTest(raw=raw), self.connection.transaction():
                with self.assertRaises(psycopg.Error):
                    with self.connection.transaction():
                        self.connection.execute("SELECT validate_cpi_evidence_canonical_bytes(%s)", (raw,))

    def test_v2_policy_review_tables_exist_and_registration_is_not_seeded(self):
        self.require_v2()
        for table in ("promotion_capability_evidence_policies", "promotion_capability_evidence_policy_registrations", "promotion_release_review_artifacts"):
            self.assertIsNotNone(self.connection.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0])
        self.assertEqual(self.initial_registrations, 0)

    def store_policy(self, policy):
        from psycopg.types.json import Jsonb
        self.connection.execute("""INSERT INTO promotion_capability_evidence_policies
            (policy_digest, release_subject_digest, promotion_capability_id, schema_version,
             policy_version, complete, canonical_payload, payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (policy.policy_digest, policy.release_subject_digest, policy.promotion_capability_id,
             policy.payload()["schema"], policy.payload()["policy_version"], policy.complete,
             policy.canonical_bytes, Jsonb(policy.payload())))

    def test_concurrent_expected_version_has_one_winner(self):
        self.require_v2()
        self.assertIsNotNone(self.connection.execute("SELECT to_regprocedure('register_cpi_capability_evidence_policy(text,text,text,bigint,text)')").fetchone()[0])
        _, _, policy, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        self.store_policy(policy)
        self.connection.commit()
        barrier = Barrier(2)
        def attempt():
            try:
                with psycopg.connect(self.dsn) as connection:
                    connection.execute("SET lock_timeout = '5s'")
                    barrier.wait(timeout=5)
                    return connection.execute("SELECT register_cpi_capability_evidence_policy(%s,%s,%s,0,'test')",
                        (policy.release_subject_digest, policy.promotion_capability_id, policy.policy_digest)).fetchone()[0]
            except psycopg.Error as error:
                return error.sqlstate
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: attempt(), range(2)))
        self.assertEqual(sum(not isinstance(value, str) for value in results), 1, results)
        self.assertIn("40001", results)
        self.assertEqual(self.connection.execute("SELECT max(registration_version),count(*) FROM promotion_capability_evidence_policy_registrations WHERE release_subject_digest=%s", (policy.release_subject_digest,)).fetchone(), (1, 1))

    def test_review_json_columns_cannot_disagree_with_bytes(self):
        self.require_v2()
        from psycopg.types.json import Jsonb
        _, _, _, review, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        with self.assertRaises(psycopg.errors.CheckViolation), self.connection.transaction():
            self.connection.execute("""INSERT INTO promotion_release_review_artifacts
                (review_ref,review_digest,raw_bytes,payload,schema_version,purpose,bindings)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (review.review_ref, review.review_digest,
                review.raw_bytes, Jsonb(review.payload()), 'cpi-w1-review-artifact-v1',
                'PROMOTION_AUTHORIZATION', Jsonb({})))

    def test_unknown_policy_keys_and_missing_schema_rejected(self):
        self.require_v2()
        import hashlib
        from psycopg.types.json import Jsonb
        _, _, policy, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        for payload in (dict(policy.payload(), unknown=True), {key: value for key, value in policy.payload().items() if key != 'schema'},
                        dict(policy.payload(), required_official_evidence_classes=[]),
                        dict(policy.payload(), coverage_rule={}),
                        dict(policy.payload(), incomplete_reasons=['INCOMPLETE']),
                        dict(policy.payload(), schema=None)):
            raw = canonical_evidence_bytes(payload)
            with self.subTest(payload=payload), self.assertRaises(psycopg.errors.CheckViolation):
                with self.connection.transaction():
                    self.connection.execute("""INSERT INTO promotion_capability_evidence_policies
                        (policy_digest,release_subject_digest,promotion_capability_id,schema_version,policy_version,complete,canonical_payload,payload)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (hashlib.sha256(raw).hexdigest(), policy.release_subject_digest,
                        policy.promotion_capability_id, policy.payload()['schema'], policy.payload()['policy_version'],
                        policy.complete, raw, Jsonb(payload)))

    def test_review_unknown_binding_keys_are_rejected(self):
        self.require_v2()
        import hashlib
        from psycopg.types.json import Jsonb
        _, _, _, review, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        payload = review.payload()
        payload['bindings']['unknown'] = 'test'
        raw = canonical_evidence_bytes(payload)
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.transaction():
                self.connection.execute("""INSERT INTO promotion_release_review_artifacts
                    (review_ref,review_digest,raw_bytes,payload,schema_version,purpose,bindings)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""", ('test-review.json', hashlib.sha256(raw).hexdigest(),
                    raw, Jsonb(payload), payload['schema'], payload['purpose'], Jsonb(payload['bindings'])))

    def test_v2_repository_writes_bind_exact_payload_and_preserve_null_build(self):
        self.require_v2()
        from src.cpi_w1_repository import CpiW1Repository
        repository = CpiW1Repository()
        self.assertTrue(hasattr(repository, 'store_capability_evidence_policy'), 'V2 repository writers missing')
        values, evidence, policy, review, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        self.assertEqual(repository.store_capability_evidence_policy(self.connection, policy), policy.policy_digest)
        self.assertEqual(repository.store_review_artifact(self.connection, review), (review.review_ref, review.review_digest))
        first = repository.create_promotion_evidence_snapshot_v2(self.connection, evidence, 'test')
        self.assertEqual(repository.create_promotion_evidence_snapshot_v2(self.connection, evidence, 'test'), first)
        blocked = cpi_w1_evidence_snapshot_v2.PromotionEvidenceSnapshotV2.from_mapping(dict(values,
            tested_executor_source_revision=None, tested_workload_artifact_digest=None))
        second = repository.create_promotion_evidence_snapshot_v2(self.connection, blocked, 'test')
        row = self.connection.execute("SELECT corpus_snapshot_digest,tested_source_revision,tested_executor_source_revision FROM promotion_release_evidence_snapshots WHERE evidence_snapshot_id=%s", (second,)).fetchone()
        self.assertEqual(row, (None, None, None))
        for query in ("UPDATE promotion_release_evidence_snapshots SET created_by_subject='other' WHERE evidence_snapshot_id=%s",
                      "DELETE FROM promotion_release_evidence_snapshots WHERE evidence_snapshot_id=%s"):
            with self.assertRaises(psycopg.Error):
                with self.connection.transaction():
                    self.connection.execute(query, (first,))

    def test_missing_policy_and_stale_direct_registration_rejected(self):
        self.require_v2()
        _, _, policy, *_ = v2_case(cpi_w1_evidence_snapshot_v2)
        # Another subject avoids the committed race fixture.
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with self.connection.transaction():
                self.connection.execute("SELECT register_cpi_capability_evidence_policy(%s,%s,%s,0,'test')",
                    ('f'*64, policy.promotion_capability_id, '0'*64))
        with self.assertRaises(psycopg.errors.SerializationFailure):
            with self.connection.transaction():
                self.connection.execute("""INSERT INTO promotion_capability_evidence_policy_registrations
                    (release_subject_digest,promotion_capability_id,policy_digest,expected_registration_version,registration_version,actor)
                    VALUES (%s,%s,%s,99,100,'test')""", (policy.release_subject_digest, policy.promotion_capability_id, policy.policy_digest))


if __name__ == "__main__":
    unittest.main()
