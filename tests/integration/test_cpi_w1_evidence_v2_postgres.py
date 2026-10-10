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

    def test_v2_material_exact_build_and_review_binding(self):
        from src.cpi_w1_repository import CpiW1Repository
        from src.cpi_w1_authorization import PromotionAuthorizationMaterialV2
        repository = CpiW1Repository()
        self.assertTrue(hasattr(repository, 'create_promotion_authorization_material_v2'), 'V2 authorization writer missing')
        _, evidence, policy, review, executor, gate = v2_case(cpi_w1_evidence_snapshot_v2)
        repository.store_capability_evidence_policy(self.connection, policy)
        current=self.connection.execute('SELECT max(registration_version) FROM promotion_capability_evidence_policy_registrations WHERE release_subject_digest=%s', (policy.release_subject_digest,)).fetchone()[0]
        if current is None:
            repository.register_capability_evidence_policy(self.connection,policy,0,'test')
        repository.store_review_artifact(self.connection, review)
        evidence_id = repository.create_promotion_evidence_snapshot_v2(self.connection, evidence, 'test')
        material = PromotionAuthorizationMaterialV2.from_review(evidence=evidence, current_policy=policy,
            review=review, executor=executor, gate_decision=gate, source_contract_digest=evidence.source_contract_digest)
        material_id = repository.create_promotion_authorization_material_v2(self.connection,
            material=material, evidence_snapshot_id=evidence_id, created_by_subject='test')
        authorization = repository.create_promotion_authorization(self.connection,
            authorization_material_id=material_id, release_subject_digest=policy.release_subject_digest,
            grant_reason_code='TEST_ONLY', created_by_subject='test')
        repository.apply_promotion_release_control(self.connection, authorization_id=authorization,
            expected_control_version=0,state='APPROVED',reason_code='TEST_ONLY',actor_subject='test',
            review_ref=review.review_ref,review_digest=review.review_digest)
        self.connection.execute('SELECT assert_cpi_v2_authorization_binding(%s)', (authorization,))
        self.assertEqual(repository.resolve_promotion_release_authorization(self.connection,
            release_subject_digest=policy.release_subject_digest,executor=executor), authorization)
        # Valid canonical bytes and valid SHA do not excuse a different tested job/build.
        import hashlib
        for field in ('executor_source_revision','executor_workload_artifact_digest','executor_job_contract_version'):
            payload=material.payload()
            payload[field] = '0'*64 if field.endswith('digest') else 'other-v1'
            raw=canonical_evidence_bytes(payload)
            with self.subTest(field=field), self.assertRaises(psycopg.Error):
                with self.connection.transaction():
                    self.connection.execute("""INSERT INTO promotion_release_authorization_materials
                        (authorization_material_id,release_subject_digest,evidence_snapshot_id,evidence_snapshot_digest,
                         gate_decision_digest,gate_policy_version,authorization_policy_version,executor_source_revision,
                         executor_workload_artifact_digest,executor_job_contract_version,review_ref,review_digest,
                         authorization_material_digest,created_by_subject,schema_version,canonical_payload)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'test',%s,%s)""",
                        (uuid4(),payload['release_subject_digest'],evidence_id,payload['evidence_snapshot_digest'],
                         payload['gate_decision_digest'],payload['gate_policy_version'],payload['authorization_policy_version'],
                         payload['executor_source_revision'],payload['executor_workload_artifact_digest'],payload['executor_job_contract_version'],
                         payload['review_ref'],payload['review_digest'],hashlib.sha256(raw).hexdigest(),payload['schema'],raw))


@unittest.skipUnless(os.environ.get('RUN_POSTGRES_INTEGRATION') == '1', 'isolated PostgreSQL opt-in required')
class CpiV1TransitionPostgresTest(unittest.TestCase):
    def test_active_v1_history_survives_but_authorization_is_retired(self):
        from src.cpi_w1_repository import CpiW1Repository
        from src.cpi_w1_evidence_snapshot import PromotionEvidenceSnapshotV1
        from src.cpi_w1_authorization import PromotionAuthorizationMaterialV1
        from tests.integration.test_cpi_w1_postgres import CpiW1PostgresTest, TEST_EXECUTOR, TEST_OBSERVATION_SUBJECT
        base=os.environ.get('DATABASE_URL','postgresql://market:market@localhost:55435/market')
        name='cpi_r1_history_'+uuid4().hex[:12]
        with psycopg.connect(base,autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
        args=conninfo_to_dict(base); args['dbname']=name
        try:
            with psycopg.connect(make_conninfo(**args)) as connection:
                for path in sorted(Path('db/migrations').glob('[0-9][0-9][0-9]_*.sql')):
                    if int(path.name[:3]) < 14:
                        connection.execute(path.read_text())
                repository=CpiW1Repository()
                snapshot=PromotionEvidenceSnapshotV1(release_subject_digest=TEST_OBSERVATION_SUBJECT.release_subject_digest,
                    corpus_snapshot_digest='1'*64,expected_diff_approvals_digest='2'*64,replay_result_digest='3'*64,
                    tested_job_contract_version=TEST_EXECUTOR.job_contract_version,tested_source_revision='historical-content',
                    tested_workload_artifact_digest=None,evidence_policy_version='cpi-w1-evidence-v1')
                evidence_id=repository.create_promotion_evidence_snapshot(connection,snapshot=snapshot,created_by_subject='historical')
                material=PromotionAuthorizationMaterialV1(release_subject_digest=snapshot.release_subject_digest,
                    evidence_snapshot_digest=snapshot.evidence_snapshot_digest,gate_decision_digest='4'*64,
                    gate_policy_version='cpi-w1-gate-v2',authorization_policy_version='cpi-w1-authorization-v1',
                    executor_source_revision=TEST_EXECUTOR.source_revision,executor_workload_artifact_digest=TEST_EXECUTOR.workload_artifact_digest,
                    executor_job_contract_version=TEST_EXECUTOR.job_contract_version,review_ref='historical-prose',review_digest='5'*64)
                values=material.payload(); values.pop('schema')
                material_id=uuid4()
                values.update(authorization_material_id=material_id,evidence_snapshot_id=evidence_id,
                    authorization_material_digest=material.authorization_material_digest,created_by_subject='historical')
                insert=sql.SQL('INSERT INTO promotion_release_authorization_materials ({}) VALUES ({})').format(
                    sql.SQL(',').join(map(sql.Identifier,values)),sql.SQL(',').join(sql.Placeholder() for _ in values))
                connection.execute(insert,tuple(values.values()))
                authorization=uuid4()
                connection.execute("INSERT INTO promotion_release_authorizations VALUES (%s,%s,%s,'HISTORICAL','historical',CURRENT_TIMESTAMP)",
                    (authorization,material_id,snapshot.release_subject_digest))
                repository.apply_promotion_release_control(connection,authorization_id=authorization,expected_control_version=0,
                    state='APPROVED',reason_code='HISTORICAL',actor_subject='historical',review_ref='old-prose',review_digest='6'*64)
                helper=CpiW1PostgresTest()
                run_id=helper.insert_run(connection,execution_scope='ECONOMIC_PROMOTE')
                helper.insert_work(connection,run_id,execution_scope='ECONOMIC_PROMOTE',release_subject=TEST_OBSERVATION_SUBJECT)
                claim=repository.claim_work_item(connection,execution_scope='ECONOMIC_PROMOTE',executor=TEST_EXECUTOR)
                self.assertIsNotNone(claim, 'pre-014 V1 active claim required')
                columns=tuple(values)
                query=sql.SQL('SELECT {} FROM promotion_release_authorization_materials WHERE authorization_material_id=%s').format(sql.SQL(',').join(map(sql.Identifier,columns)))
                before=connection.execute(query,(material_id,)).fetchone()
                connection.commit()
                connection.execute(Path('db/migrations/014_cpi_w1_release_evidence_v2.sql').read_text())
                self.assertEqual(connection.execute(query,(material_id,)).fetchone(),before)
                self.assertEqual(connection.execute('SELECT tested_workload_artifact_digest FROM promotion_release_evidence_snapshots WHERE evidence_snapshot_id=%s',(evidence_id,)).fetchone(),(None,))
                self.assertIsNone(repository.resolve_promotion_release_authorization(connection,
                    release_subject_digest=snapshot.release_subject_digest,executor=TEST_EXECUTOR))
                from src.cpi_w1_repository import RepositoryInvariantError
                with self.assertRaises(RepositoryInvariantError):
                    repository.assert_current_promotion_authorization(connection,claim)
                with self.assertRaises(psycopg.Error) as admission_error:
                    with connection.transaction():
                        connection.execute("""INSERT INTO ingestion_attempts
                            (attempt_id,work_item_id,execution_scope,data_domain,attempt_number,release_authorization_id,
                             release_control_decision_id,executor_source_revision,executor_workload_artifact_digest,executor_job_contract_version)
                            SELECT %s,work_item_id,execution_scope,data_domain,attempt_number,release_authorization_id,
                                   release_control_decision_id,executor_source_revision,executor_workload_artifact_digest,executor_job_contract_version
                              FROM ingestion_attempts WHERE attempt_id=%s""", (uuid4(),claim.attempt_id))
                self.assertEqual(admission_error.exception.sqlstate,'23514')
                self.assertIn('LEGACY_AUTHORIZATION_RETIRED',str(admission_error.exception))
                for operation in ('material','grant'):
                    with self.subTest(operation=operation),self.assertRaises(psycopg.errors.CheckViolation):
                        with connection.transaction():
                            if operation=='material':
                                values['authorization_material_id']=uuid4()
                                connection.execute(insert,tuple(values.values()))
                            else:
                                connection.execute("INSERT INTO promotion_release_authorizations VALUES (%s,%s,%s,'HISTORICAL','historical',CURRENT_TIMESTAMP)",
                                    (uuid4(),material_id,snapshot.release_subject_digest))
                repository.apply_promotion_release_control(connection,authorization_id=authorization,expected_control_version=1,
                    state='REVOKED',reason_code='LEGACY_RETIRED',actor_subject='test',review_ref='revocation',review_digest='7'*64)
                with self.assertRaises(RepositoryInvariantError):
                    repository.renew_claim(connection,claim)
                self.assertEqual(connection.execute('SELECT state FROM ingestion_work_items WHERE work_item_id=%s',(claim.work_item_id,)).fetchone()[0],'PAUSED')
                self.assertIsNone(repository.claim_work_item(connection,execution_scope='ECONOMIC_PROMOTE',executor=TEST_EXECUTOR))
        finally:
            with psycopg.connect(base,autocommit=True) as admin:
                admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


if __name__ == "__main__":
    unittest.main()
