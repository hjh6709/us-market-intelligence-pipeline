# CPI W1 Data & Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved CPI W1 evidence, ingestion, governance, selector, and BLS source foundation additively above migration 009 without changing current legacy serving.

**Architecture:** Keep the current Python/FastAPI application and legacy tables operational while introducing a separate CPI W1 ingestion/evidence path in PostgreSQL. The new path uses immutable capture artifacts, split release-envelope versus observation-bundle promotion, governed interpretation decisions, append-only serving controls, and a selector that supports both official-source reconstruction and system-known PIT. Spring/Next product serving and production IAM remain outside this plan.

**Tech Stack:** Python 3.13 in CI, PostgreSQL 17.6, psycopg 3.2+, httpx 0.28+, BeautifulSoup 4, openpyxl 3.1+, unittest, existing GitHub Actions CI.

**Spec:** `docs/superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md`

## Global Constraints

- Existing migrations `001` through `009` are immutable.
- Current FastAPI/legacy serving must continue to pass unchanged tests throughout this plan.
- CPI event identity is `event_type=CPI + reference_month`; release timestamps are never event identity.
- Schedule evidence, disclosure evidence, observation evidence, operational workflow state, interpretation state, and serving-control state remain separate.
- No LLM or fuzzy matching participates in canonical CPI extraction.
- No latest-wins or majority-vote rule resolves conflicting official material.
- Source artifacts are one capture event per row; W1 raw-object storage does not deduplicate deletable objects across artifact rows.
- Primary release HTML promotion is split into `CPI_RELEASE_ENVELOPE_PROMOTE` and `CPI_OBSERVATION_BUNDLE_PROMOTE`.
- Core 4 observation promotion is atomic for the `EVENT_RELEASE` contract.
- `SUPPLEMENTAL_DISCLOSURE` cannot contribute CPI W1 Core 4 canonical actuals.
- Governable evidence row UUID equals `interpretation_subjects.subject_id`.
- `OFFICIAL_SOURCE_RECONSTRUCTION` and `SYSTEM_KNOWN_PIT` are distinct selector modes.
- Serving-control decisions do not rewrite knowledge; live governed consumers apply the control overlay after knowledge selection.
- Official CPI decimal values use `Decimal`/PostgreSQL `NUMERIC`, never binary floating point as canonical representation.
- Backfill/replay never triggers live customer side effects.
- New target ingestion objects remain distinct from legacy `pipeline_*` telemetry.
- No Spring Product API, Next.js UI, billing, or broad platform refactor belongs in this plan.

## Review Focus

- A release envelope is valid but one Core 4 observation cannot be mapped: release becomes DISCLOSED while the observation bundle quarantines and no partial actuals are inserted.
- Two official representations parse successfully but disagree: both evidences persist, ingestion may succeed, and selector returns CONFLICT without latest-wins behavior.
- A replayed old artifact is accepted today: SYSTEM_KNOWN_PIT before today's accepted_at cannot see it, and source chronology is not rewritten to today.
- An EVENT_RELEASE relation is invalidated after observations were accepted: dependent observations remain stored but become selector-ineligible transitively.
- A stale worker loses its lease and later attempts to commit: fencing prevents canonical evidence and work-terminal mutation from the stale claim generation.

---

## File Structure

Create focused flat modules to match the repository's existing `src/*.py` package pattern instead of introducing a new subpackage during this migration.

New/modified responsibilities:

- `db/migrations/010_cpi_w1_ingestion_subjects.sql` — source reference, ingestion workflow, raw artifacts, interpretation subject registry.
- `db/migrations/011_cpi_w1_event_disclosure.sql` — CPI occurrence, schedule evidence, disclosure graph, marker assertions.
- `db/migrations/012_cpi_w1_observations.sql` — observation definitions and official observation assertions.
- `db/migrations/013_cpi_w1_governance_serving.sql` — interpretation requests/approvals/decisions, business audit, serving-control decisions, atomic activation functions.
- `src/cpi_w1_contracts.py` — W1 enums, immutable dataclasses, material fingerprints, selector input/output types.
- `src/cpi_w1_repository.py` — PostgreSQL persistence, claim/fencing, evidence inserts, selector queries.
- `src/cpi_w1_artifacts.py` — artifact-store protocol plus local immutable filesystem implementation used by development/tests.
- `src/cpi_w1_source.py` — BLS source contract, safe HTTP retrieval, allowlist/redirect/size handling.
- `src/cpi_w1_schedule.py` — BLS CPI schedule/ICS parsing into typed schedule candidates.
- `src/cpi_w1_release.py` — release-envelope validation and CPI Table 1 Core 4 extraction.
- `src/cpi_w1_promoter.py` — release-envelope and observation-bundle promotion families.
- `src/cpi_w1_selector.py` — source reconstruction, PIT filtering, transitive eligibility, material resolution, serving overlay.
- `src/cpi_w1_governance.py` — request/approval/activation and serving-control application API over DB functions.
- `scripts/collect_cpi_w1.py` — controlled collector entrypoint for local/Internal Alpha use.
- `scripts/replay_cpi_w1_corpus.py` — corpus differential replay tool.
- `config/cpi_w1_source_contract.json` — machine-readable source URLs/roles/limits/version subordinate to the approved spec.
- `tests/fixtures/cpi_w1/corpus.json` — hash-pinned corpus manifest.
- `tests/fixtures/cpi_w1/html/` — small rights-safe deterministic unit fixtures; full corpus may be materialized outside Git by the replay script.
- `tests/test_cpi_w1_*.py` — unit contract/parser/selector tests.
- `tests/integration/test_cpi_w1_postgres.py` — migration, idempotency, governance, PIT, and fencing integration tests.
- `docs/architecture/AUTHORITY.md` and existing authority/status docs — repository authority migration.

---

### Task 1: Migrate Repository Authority Before New Runtime Semantics

**Files:**
- Create: `docs/architecture/AUTHORITY.md`
- Create: `tests/test_architecture_authority.py`
- Modify: `docs/README.md`
- Modify: `README.md`
- Modify: `docs/architecture/platform-contract.md`
- Modify: `docs/architecture/data-contracts.md`
- Modify: `docs/architecture/operations-and-deployment.md`
- Modify: `docs/engineering/api-contracts.md`
- Modify: `docs/engineering/current-vs-target.md`
- Modify: `docs/configuration/README.md`
- Modify: `docs/architecture/current-system.md`
- Modify: `docs/superpowers/specs/2026-09-10-canonical-docs-event-data-foundation-design.md`
- Modify: `docs/superpowers/specs/2026-09-10-operational-pipelines-storage-design.md`
- Modify: `docs/superpowers/specs/2026-09-10-serving-deployment-product-design.md`

**Interfaces:**
- Consumes: approved CPI W1 spec and executable main baseline.
- Produces: one routing authority index and explicit historical/legacy labels so later tasks cannot accidentally implement superseded semantics.

- [ ] **Step 1: Write the failing authority-routing test**

Create `tests/test_architecture_authority.py`:

```python
import unittest
from pathlib import Path


class ArchitectureAuthorityTest(unittest.TestCase):
    def test_authority_index_routes_current_target_and_legacy_contracts(self) -> None:
        authority = Path("docs/architecture/AUTHORITY.md")
        self.assertTrue(authority.exists())
        text = authority.read_text(encoding="utf-8")
        self.assertIn("CPI W1 Data & Governance", text)
        self.assertIn("2026-09-19-cpi-w1-data-governance-design.md", text)
        self.assertIn("CURRENT_IMPLEMENTATION", text)
        self.assertIn("HISTORICAL_SUPERSEDED", text)

    def test_old_target_specs_are_visibly_superseded(self) -> None:
        for path in (
            "docs/superpowers/specs/2026-09-10-canonical-docs-event-data-foundation-design.md",
            "docs/superpowers/specs/2026-09-10-operational-pipelines-storage-design.md",
            "docs/superpowers/specs/2026-09-10-serving-deployment-product-design.md",
        ):
            text = Path(path).read_text(encoding="utf-8")
            self.assertIn("HISTORICAL_SUPERSEDED", text)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
.venv/bin/python -m unittest tests.test_architecture_authority -v
```

Expected: FAIL because `AUTHORITY.md` does not exist and old specs are not yet marked superseded.

- [ ] **Step 3: Create the authority index and reclassify legacy documents**

`AUTHORITY.md` must route concerns instead of redefining them:

```markdown
# Architecture Authority Index

| Concern | Status | Authority |
| --- | --- | --- |
| Current executable behavior | CURRENT_IMPLEMENTATION | code + migrations + tests at the commit being inspected |
| CPI W1 data/governance target | TARGET_CANONICAL | ../superpowers/specs/2026-09-19-cpi-w1-data-governance-design.md |
| Legacy migration-009 event model | HISTORICAL_SUPERSEDED | data-contracts.md |
| Current FastAPI API | CURRENT_IMPLEMENTATION | ../engineering/api-contracts.md |
| Product API target | NOT_YET_SPECIFIED | separate future Product Serving spec |
| Runtime/security target | NOT_YET_SPECIFIED | separate future Runtime/Security spec |
| Current/target progress | STATUS_LEDGER | ../engineering/current-vs-target.md |
```

Add explicit status headers to old target docs/specs. Do not delete historical content.

- [ ] **Step 4: Update README routing**

Replace language that calls the 2026-09-10 platform contract the approved universal target. Make the README distinguish current implementation, CPI W1 target, and historical target docs.

- [ ] **Step 5: Run authority and existing documentation tests**

Run:

```bash
.venv/bin/python -m unittest tests.test_architecture_authority tests.test_assignment_docs -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add README.md docs tests/test_architecture_authority.py
git commit -m "docs: establish CPI W1 architecture authority routing"
```

---

### Task 2: Add Ingestion, Artifact, and Governance-Subject Foundation

**Files:**
- Create: `db/migrations/010_cpi_w1_ingestion_subjects.sql`
- Create: `tests/test_cpi_w1_ingestion_migration.py`
- Create: `tests/integration/test_cpi_w1_postgres.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Produces tables: `data_sources`, `ingestion_runs`, `ingestion_work_items`, `ingestion_attempts`, `source_artifacts`, `interpretation_subjects`.
- Later tasks depend on UUID keys, `execution_scope`, `data_domain`, claim generation, artifact provenance, and subject identity.

- [ ] **Step 1: Write SQL contract tests for the six foundation tables**

Create `tests/test_cpi_w1_ingestion_migration.py` that asserts migration 010 contains:

```python
TABLES = (
    "data_sources",
    "ingestion_runs",
    "ingestion_work_items",
    "ingestion_attempts",
    "source_artifacts",
    "interpretation_subjects",
)
```

Also assert presence of:
- `LIVE/BACKFILL/REPLAY`
- `CREATED/RUNNING/TERMINAL`
- `PENDING/CLAIMED/TERMINAL`
- `QUARANTINED`
- `claim_generation`, `claim_token`, `lease_until`
- `source_contract_version`
- `content_sha256`
- `RETAINED/NOT_RETAINED/DELETED_BY_POLICY`
- composite uniqueness needed by child FKs.

- [ ] **Step 2: Verify the contract test fails**

Run:

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_ingestion_migration -v
```

Expected: FAIL because migration 010 does not exist.

- [ ] **Step 3: Implement migration 010**

Use PostgreSQL `UUID`, `TIMESTAMPTZ`, `TEXT`, and `CHECK` constraints. Do not create new schemas.

Key constraints to include:

```sql
CHECK (run_mode IN ('LIVE','BACKFILL','REPLAY')),
CHECK (state IN ('CREATED','RUNNING','TERMINAL')),
CHECK (execution_scope IN ('ECONOMIC_COLLECT','ECONOMIC_PROMOTE')),
CHECK (data_domain = 'ECONOMIC')
```

For artifacts, enforce retained-object consistency:

```sql
CHECK (
  (content_state = 'RETAINED' AND storage_uri IS NOT NULL AND storage_generation IS NOT NULL)
  OR
  (content_state IN ('NOT_RETAINED','DELETED_BY_POLICY'))
)
```

Create the W1 `BLS` source row deterministically with `INSERT ... ON CONFLICT DO NOTHING`.

- [ ] **Step 4: Add integration setup and state-machine tests**

Extend `tests/integration/test_cpi_w1_postgres.py` to apply all sorted migrations like the existing event-session integration suite.

Test:
- duplicate `run_id + work_key` rejected;
- REPLAY requires `replay_of_run_id`;
- cross-domain/scope FK rejected;
- retained artifact without generation rejected;
- duplicate artifact row within same attempt/locator/hash converges through the repository contract in a later task, while different attempts are allowed distinct artifact rows.

- [ ] **Step 5: Add the new integration module to CI**

In `.github/workflows/ci.yml`, append:

```yaml
tests.integration.test_cpi_w1_postgres
```

to the PostgreSQL integration-test invocation.

- [ ] **Step 6: Run unit + PostgreSQL integration tests**

Run:

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_ingestion_migration -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add db/migrations/010_cpi_w1_ingestion_subjects.sql tests .github/workflows/ci.yml
git commit -m "feat: add CPI W1 ingestion evidence foundation"
```

---

### Task 3: Add Event, Schedule, Disclosure, and Marker Evidence

**Files:**
- Create: `db/migrations/011_cpi_w1_event_disclosure.sql`
- Create: `tests/test_cpi_w1_event_migration.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces: `core_event_occurrences`, `event_schedule_assertions`, `event_disclosures`, `event_disclosure_links`, `event_disclosure_artifacts`, `disclosure_marker_assertions`.
- Consumes: `source_artifacts`, `ingestion_attempts`, `interpretation_subjects`.

- [ ] **Step 1: Write failing SQL contract tests**

Assert:
- CPI reference month must be first day of month;
- event identity unique on `event_type, reference_month`;
- schedule statuses exactly `SCHEDULED/DATE_PENDING/CANCELED`;
- schedule precision supports `EXACT/DATE_ONLY`;
- disclosure link kinds exactly `EVENT_RELEASE/SUPPLEMENTAL_DISCLOSURE`;
- disclosure artifact kinds exactly `RELEASE_REPRESENTATION/CORROBORATING_REPRESENTATION/CORRECTION_NOTICE`;
- all governable tables use their row UUID as an FK-compatible interpretation subject id;
- event/disclosure links include `accepted_at`.

- [ ] **Step 2: Verify failure**

Run:

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_event_migration -v
```

Expected: FAIL.

- [ ] **Step 3: Implement migration 011 with provenance constraints**

Use composite unique keys so observation/marker migrations can later pin exact graph edges.

For example:

```sql
UNIQUE (disclosure_link_id, event_occurrence_id, disclosure_id, relation_kind)
```

and:

```sql
UNIQUE (disclosure_artifact_link_id, disclosure_id, artifact_id, relation_kind)
```

Do not store a `PRIMARY` disclosure flag. Do not store `RESCHEDULED`.

- [ ] **Step 4: Add PostgreSQL integration tests**

Test:
- schedule row absence is possible and not converted to DATE_PENDING;
- exact timestamp and date-only schedule are distinct;
- two schedule assertions can coexist for rescheduling;
- one disclosure can link to multiple event occurrences with explicit relation kinds;
- EVENT_RELEASE and SUPPLEMENTAL_DISCLOSURE for the same event/disclosure are distinct immutable evidences;
- marker provenance cannot point at an unrelated artifact link.

- [ ] **Step 5: Run tests**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_event_migration -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
```

- [ ] **Step 6: Commit**

```bash
git add db/migrations/011_cpi_w1_event_disclosure.sql tests
git commit -m "feat: add CPI W1 event disclosure evidence model"
```

---

### Task 4: Add Core 4 Official Observation Assertions

**Files:**
- Create: `db/migrations/012_cpi_w1_observations.sql`
- Create: `tests/test_cpi_w1_observation_migration.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces: `observation_definitions`, `official_observation_assertions`.
- Consumes exact `disclosure_link_id` and `disclosure_artifact_link_id` topology from Task 3.

- [ ] **Step 1: Write failing observation-contract tests**

Require four seeded codes:

```python
EXPECTED = {
    "CPI_HEADLINE_MOM",
    "CPI_HEADLINE_YOY",
    "CPI_CORE_MOM",
    "CPI_CORE_YOY",
}
```

Require assertion states `VALUE` and `EXPLICIT_UNAVAILABLE`, `NUMERIC` canonical value, and provenance-link fields.

- [ ] **Step 2: Verify failure**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_observation_migration -v
```

- [ ] **Step 3: Implement migration 012**

Enforce:

```sql
CHECK (
  (assertion_state = 'VALUE' AND normalized_value IS NOT NULL)
  OR
  (assertion_state = 'EXPLICIT_UNAVAILABLE' AND normalized_value IS NULL)
)
```

Add composite FKs ensuring the observation's event/disclosure/artifact IDs match its pinned link rows.

Seed the four observation definitions with canonical unit `PERCENT`.

- [ ] **Step 4: Add integration tests**

Test:
- `Decimal("2.9")` round-trips exactly;
- VALUE with NULL fails;
- EXPLICIT_UNAVAILABLE with numeric value fails;
- cross-event/disclosure link mismatch fails;
- cross-artifact link mismatch fails;
- SUPPLEMENTAL_DISCLOSURE cannot be used by the repository's Core 4 promotion path in Task 10 even though the storage layer can retain supplemental evidence.

- [ ] **Step 5: Run tests and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_observation_migration -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
git add db/migrations/012_cpi_w1_observations.sql tests
git commit -m "feat: add CPI W1 official observation evidence"
```

---

### Task 5: Add Interpretation Governance and Serving-Control History

**Files:**
- Create: `db/migrations/013_cpi_w1_governance_serving.sql`
- Create: `tests/test_cpi_w1_governance_migration.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces: `interpretation_requests`, `interpretation_approvals`, `interpretation_decisions`, `business_audit_events`, `economic_serving_control_decisions`.
- Produces atomic DB functions used by `src/cpi_w1_governance.py` later.

- [ ] **Step 1: Write failing migration tests**

Require function names:

```text
apply_interpretation_decision
apply_economic_serving_control
```

Require:
- proposer != approver;
- one vote per approver/request;
- decision version uniqueness per subject;
- control version uniqueness per scope;
- domain scope has NULL event ID;
- event scope requires event ID;
- audit table append-only to normal application paths.

- [ ] **Step 2: Implement migration 013**

`apply_interpretation_decision` must:
1. lock the subject's current decision version;
2. verify request is unexpired;
3. verify expected version;
4. reject requested state equal to current effective state;
5. verify exactly one independent APPROVE and no REJECT for governance policy v1;
6. insert decision version +1;
7. insert business audit;
8. commit or fail atomically.

`apply_economic_serving_control` must:
1. lock one control scope;
2. verify expected control version;
3. reject no-op state;
4. insert control version +1;
5. insert business audit atomically.

Do not add production GRANT/role topology in this migration; that belongs to the Runtime/Security plan.

- [ ] **Step 3: Add concurrency tests**

Using `ThreadPoolExecutor` as in the existing integration suite, test two requests trying to activate against the same expected decision version. Exactly one may commit.

Repeat for serving-control version.

- [ ] **Step 4: Test no-op and self-approval failure**

Verify:
- INVALID -> INVALID second activation fails;
- proposer approving own request fails;
- approval after expiration cannot activate;
- a REJECT blocks activation.

- [ ] **Step 5: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_governance_migration -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
git add db/migrations/013_cpi_w1_governance_serving.sql tests
git commit -m "feat: add CPI interpretation governance and serving controls"
```

---

### Task 6: Define Python CPI W1 Contracts and Deterministic Material Fingerprints

**Files:**
- Create: `src/cpi_w1_contracts.py`
- Create: `tests/test_cpi_w1_contracts.py`
- No dependency-file changes in this task; parser dependencies are introduced just-in-time in Tasks 8/9.

**Interfaces:**
- Produces enums/dataclasses shared by collectors, promoters, selectors.
- Produces `material_fingerprint(payload: Mapping[str, object]) -> str`.

- [ ] **Step 1: Keep semantic contracts dependency-free**

Do not add HTML/XLSX parser libraries here. This module uses only the Python standard library.
Introduce BeautifulSoup in Task 8 and openpyxl in Task 9, where their attack surface and
lockfile changes are exercised by parser-specific tests.

- [ ] **Step 2: Write failing contract tests**

Tests must pin enums including:
- `RunMode`
- `WorkOutcome`
- `ScheduleStatus`
- `TimePrecision`
- `DisclosureRelationKind`
- `DisclosureArtifactRelationKind`
- `ObservationState`
- `KnowledgeMode`
- `ReleaseProjectionState`
- `ServingControlState`
- `PromotionFamily`

Fingerprint tests:

```python
self.assertEqual(
    material_fingerprint({"value": "2.9", "unit": "PERCENT"}),
    material_fingerprint({"unit": "PERCENT", "value": "2.9"}),
)
```

and prove provenance fields are not passed into typed material builders.

- [ ] **Step 3: Implement immutable contracts**

Use `StrEnum`, frozen dataclasses, `Decimal`, UTC-aware datetimes, and canonical JSON serialization with sorted keys.

Define:

```python
class PromotionFamily(StrEnum):
    CPI_RELEASE_ENVELOPE_PROMOTE = "CPI_RELEASE_ENVELOPE_PROMOTE"
    CPI_OBSERVATION_BUNDLE_PROMOTE = "CPI_OBSERVATION_BUNDLE_PROMOTE"
```

Do not edit legacy `src/platform_contracts.py` enums.

- [ ] **Step 4: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_contracts tests.test_platform_contracts -v
git add src/cpi_w1_contracts.py tests/test_cpi_w1_contracts.py docs/superpowers/plans/2026-09-19-cpi-w1-data-governance.md
git commit -m "feat: define CPI W1 semantic contracts"
```

---

### Task 7: Implement Safe Artifact Storage and BLS Retrieval Contract

**Files:**
- Create: `config/cpi_w1_source_contract.json`
- Create: `src/cpi_w1_artifacts.py`
- Create: `src/cpi_w1_source.py`
- Create: `tests/test_cpi_w1_artifacts.py`
- Create: `tests/test_cpi_w1_source.py`

**Interfaces:**
- Produces:
  - `ArtifactStore.put(artifact_id: UUID, body: bytes, sha256: str) -> StoredArtifact`
  - `BlsCpiSourceClient.fetch(locator: SourceLocator) -> CapturedResponse`
- No DB writes in source client.

- [ ] **Step 1: Create the machine-readable source contract**

Use contract version `bls-cpi-source-v1`.

Include:
- HTTPS only;
- allowlisted host `www.bls.gov` for the initial W1 contract; any additional official host requires an explicit source-contract change and test before use;
- redirect target must remain allowlisted;
- bounded redirect count;
- connect/read timeout;
- maximum compressed response size;
- maximum HTML/XLSX raw size;
- schedule surface roles;
- current-release and CPI-schedule locators.

Do not put credentials in this file.

- [ ] **Step 2: Write retrieval security tests**

Test:
- non-HTTPS rejected;
- redirect to non-BLS host rejected;
- oversized response rejected;
- HTML parser never fetches subresources;
- 429 maps to typed retryable source failure;
- 404 after due does not become CANCELED/NO_RELEASE_EXPECTED.

- [ ] **Step 3: Implement `FilesystemArtifactStore`**

Each artifact gets its own path:

```text
<data-root>/<artifact-uuid>/<sha256>.bin
```

Use create-exclusive semantics. If the path exists, verify exact hash before reuse.

Never share one deletable object between two different artifact UUIDs.

- [ ] **Step 4: Implement the BLS source client**

Use `httpx.Client(follow_redirects=False)`, manually validate each redirect, and stream with byte limits.

Return response metadata but never infer publication semantics in this layer.

- [ ] **Step 5: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_artifacts tests.test_cpi_w1_source -v
git add config/cpi_w1_source_contract.json src/cpi_w1_artifacts.py src/cpi_w1_source.py tests
git commit -m "feat: add bounded BLS CPI source capture"
```

---

### Task 8: Implement CPI Schedule Parsing and Schedule Candidate Semantics

**Files:**
- Create: `src/cpi_w1_schedule.py`
- Create: `tests/test_cpi_w1_schedule.py`
- Create: `tests/fixtures/cpi_w1/schedule/`

**Interfaces:**
- Consumes captured BLS schedule/ICS bytes.
- Produces `ScheduleCandidate` objects only; no canonical DB writes.

- [ ] **Step 1: Add deterministic fixtures**

Create small HTML/ICS fixtures covering:
- exact 08:30 ET schedule;
- rescheduled date;
- cancellation notice;
- date-only schedule;
- authoritative schedule vs stale ICS disagreement.

- [ ] **Step 2: Write failing parser tests**

Examples:

```python
candidate = parse_cpi_schedule_html(body, contract_version="bls-cpi-source-v1")
self.assertEqual(candidate.reference_month.isoformat(), "2026-08-01")
self.assertEqual(candidate.timezone, "America/New_York")
self.assertEqual(candidate.time_precision, TimePrecision.EXACT)
```

Test that stale ICS is tagged `FALLBACK_CORROBORATION`, not authoritative.

- [ ] **Step 3: Implement semantic-anchor parsing**

Parse labels/headings, not positional column numbers alone.

Do not synthesize midnight for date-only schedules.

- [ ] **Step 4: Add material fingerprint tests**

Changing `source_effective_at` alone must not change schedule material fingerprint when typed schedule material itself is unchanged.

- [ ] **Step 5: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_schedule -v
git add src/cpi_w1_schedule.py tests/fixtures/cpi_w1/schedule tests/test_cpi_w1_schedule.py
git commit -m "feat: parse CPI schedule evidence"
```

---

### Task 9: Implement Release Envelope and Core 4 Extractor

**Files:**
- Create: `src/cpi_w1_release.py`
- Create: `tests/test_cpi_w1_release.py`
- Create: `tests/fixtures/cpi_w1/html/`
- Create later in Task 9B after capture of an official binary fixture:
  `tests/fixtures/cpi_w1/xlsx/`

**Interfaces:**
- Produces:
  - `ReleaseEnvelopeCandidate`
  - `ObservationBundleCandidate`
- Does not write PostgreSQL.

- [ ] **Step 1: Build fixtures for normal and exceptional semantics**

Include:
- normal CPI release with four values;
- negative MoM;
- zero value;
- wrong reference month;
- unknown table headings;
- 2025-style explicit MoM unavailable case;
- dash without supporting unavailable semantics;
- same-release XLSX match;
- same-release XLSX conflicting value.

- [ ] **Step 2: Write envelope tests**

Envelope validation must succeed independently of the observation bundle.

Test:

```python
envelope = extract_release_envelope(html_bytes, expected_reference_month=date(2026, 8, 1))
self.assertEqual(envelope.event_type, "CPI")
self.assertEqual(envelope.reference_month, date(2026, 8, 1))
```

Unknown observation headings must not make an otherwise valid release envelope disappear.

- [ ] **Step 3: Write Core 4 mapping tests**

Map semantic labels:
- All items + one-month seasonally adjusted -> headline MoM
- All items + 12-month unadjusted -> headline YoY
- All items less food and energy + one-month seasonally adjusted -> core MoM
- same row + 12-month unadjusted -> core YoY

Never implement "rightmost four cells".

- [ ] **Step 4: Enforce explicit-unavailable semantics**

A dash alone fails mapping. Explicit source context can produce `EXPLICIT_UNAVAILABLE`.

- [ ] **Step 5: Commit Task 9A HTML extraction first**

Do not invent an XLSX fixture from an assumed workbook layout. Task 9A is complete when
the official release HTML can independently establish the release envelope and safely
resolve/quarantine the Core 4 bundle.

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_release -v
git add src/cpi_w1_release.py tests/fixtures/cpi_w1/html tests/test_cpi_w1_release.py
git commit -m "feat: extract CPI release HTML evidence"
```

- [ ] **Step 6: Task 9B — add XLSX corroboration from a captured official workbook**

Before implementing the XLSX parser:
- capture and retain at least one official BLS News Release Table 1 XLSX fixture;
- treat `https://www.bls.gov/web/cpi/cpipress1.xlsx` as a mutable current locator,
  never a permanent disclosure identity;
- record its source URL, capture timestamp, content hash, and independently verified
  reference month in fixture metadata;
- require the workbook reference month to match the already-established CPI event
  occurrence before it can be linked as corroborating representation;
- inspect ZIP members and workbook relationships from the real file rather than
  assuming worksheet names/cell coordinates.

The XLSX reader must then:
- cap ZIP entry count, per-entry size, and total uncompressed size;
- reject macro-enabled workbook content;
- reject external relationships;
- reject formulas for canonical values;
- map semantic headers/row labels, never fixed cell coordinates;
- treat XLSX as corroborating representation, not historical release authority when
  the file was retrieved later than the release.

Run and commit Task 9B separately so an XLSX failure cannot obscure the already-tested
HTML envelope/extractor contract.

---

### Task 10: Implement Ingestion Repository, Claim Fencing, and Split Promotion

**Files:**
- Create: `src/cpi_w1_repository.py`
- Create: `src/cpi_w1_promoter.py`
- Create: `tests/test_cpi_w1_repository.py`
- Create: `tests/test_cpi_w1_promoter.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces:
  - `claim_work_item(...)->Claim`
  - `record_source_artifact(...)->UUID`
  - `promote_release_envelope(...)->PromotionResult`
  - `promote_observation_bundle(...)->PromotionResult`
- Uses short transactions and current claim-generation fencing.

- [ ] **Step 1: Write repository unit tests around SQL contracts**

Verify SQL always includes claim-generation predicates for canonical promotion and work terminalization.

- [ ] **Step 2: Implement claim/reclaim behavior**

Claim:
- use transaction + `FOR UPDATE SKIP LOCKED`;
- select due PENDING or expired CLAIMED work;
- increment claim_generation;
- assign new claim_token/lease;
- create attempt_number aligned to the new execution generation.
- same-generation lease renewal preserves the existing claim_token;
- expired-lease reclaim advances generation and assigns a different claim_token.
- close the expired prior RUNNING attempt as FAILED/LEASE_EXPIRED_RECLAIM before
  creating the new attempt generation;
- retry-to-PENDING terminalizes the current attempt with a durable reason before
  clearing ownership and setting next_claim_at.
- Retry scheduling accepts only a bounded relative delay (W1: 1 second to 24 hours)
  and derives next_claim_at from the database clock; callers do not supply an
  absolute retry timestamp.

Promotion work uses a deterministic promotion-family-prefixed work key:
`<promotion_family>:<artifact_id>:<extractor_contract_version>`.
Workers claim only their family prefix, and the promoter revalidates the exact key so
one promotion family cannot execute another family's work.

Every typed promotion candidate carries `artifact_content_sha256`. The promoter
must compare it to the durable `source_artifacts.content_sha256` for the claimed
input artifact and reject cross-wired candidates. Artifact kind also constrains the
allowed extractor-contract version.

All later mutation methods require matching `work_item_id + claim_generation + claim_token`.
Work terminalization requires the current generation attempt to be TERMINAL with the
same outcome; SKIPPED is the only terminal work outcome with no attempt.

- [ ] **Step 3: Implement source-artifact recording**

Same attempt + locator + hash must converge to one artifact row. Different attempts may create distinct artifact rows for the same bytes.
A metadata retry must match the full immutable capture tuple, not only the hash. Reject
drift in source/contract/content type/retrieval URL/captured_at/retention/object
generation. Preserve the original capture timestamp.

- [ ] **Step 4: Implement `CPI_RELEASE_ENVELOPE_PROMOTE`**

One transaction:
- verify current claim;
- generate one transaction-owned `accepted_at` knowledge timestamp; callers/parsers
  cannot supply or backdate it;
- create/verify CPI event occurrence;
- create/verify disclosure identity;
- create/verify EVENT_RELEASE link;
- create/verify RELEASE_REPRESENTATION link;
- create/verify marker assertion;
- create required interpretation_subject rows using the evidence UUIDs;
- terminalize the current attempt SUCCEEDED;
- terminalize work SUCCEEDED with the same outcome.

No observations are inserted here.

- [ ] **Step 5: Implement `CPI_CORROBORATING_REPRESENTATION_PROMOTE`**

For a validated same-release Table 1 XLSX representation:
- verify current claim and `CPI_TABLE1_XLSX` artifact contract;
- require exactly one valid BLS EVENT_RELEASE for the same reference month;
- create/verify only the `CORROBORATING_REPRESENTATION` disclosure-artifact link
  and its interpretation subject;
- insert no Core 4 assertion;
- terminalize attempt/work SUCCEEDED atomically.

This family remains unusable for live XLSX until Task 9B provides a validated real
workbook candidate; tests may construct the typed candidate directly to verify the
repository/promotion contract without pretending the XLSX parser is complete.

- [ ] **Step 6: Implement `CPI_OBSERVATION_BUNDLE_PROMOTE`**

One transaction:
- verify current claim;
- generate one transaction-owned `accepted_at` knowledge timestamp; callers/parsers
  cannot supply or backdate it;
- verify valid EVENT_RELEASE and disclosure-artifact topology for the input artifact;
- require all four expected observation semantics resolved as VALUE or EXPLICIT_UNAVAILABLE;
- insert/verify all four assertions and their interpretation subjects;
- never accept SUPPLEMENTAL_DISCLOSURE for Core 4;
- terminalize the current attempt SUCCEEDED;
- terminalize work SUCCEEDED with the same outcome.

If typed extraction is unsafe, terminalize the current attempt `QUARANTINED` with
a durable reason_code and then terminalize work `QUARANTINED` with the same outcome,
without inserting any partial Core 4 assertions.

- [ ] **Step 7: Add the key split-outcome integration test**

Scenario:
1. enqueue envelope + observation work for one artifact;
2. envelope candidate valid;
3. observation candidate raises semantic mapping quarantine;
4. envelope work becomes SUCCEEDED;
5. observation work becomes QUARANTINED;
6. disclosure/EVENT_RELEASE/marker rows exist;
7. zero Core 4 assertion rows exist.

- [ ] **Step 8: Add accepted-at trust-boundary tests**

Verify promoter/repository public interfaces expose no caller-selected `accepted_at`.
Insert evidence through promotion, then prove the stored knowledge time is generated
by the promotion transaction. Replay/backfill of an older artifact must not backdate
`accepted_at`.

- [ ] **Step 9: Add stale-worker fencing test**

Claim work as generation 1, expire/reclaim as generation 2, then attempt generation-1 promotion. Expected: stale mutation rejected and no evidence inserted.

- [ ] **Step 10: Add conflicting official representation test**

Promote HTML Core 4 value 0.3, then an eligible corroborating representation value 0.4. Both work items may SUCCEED; both evidence rows remain.

- [ ] **Step 11: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_repository tests.test_cpi_w1_promoter -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
git add src/cpi_w1_repository.py src/cpi_w1_promoter.py tests
git commit -m "feat: add fenced CPI W1 promotion workflow"
```

---

### Task 11: Implement CPI Selector, PIT Reconstruction, and Serving Overlay

**Files:**
- Create: `src/cpi_w1_selector.py`
- Create: `tests/test_cpi_w1_selector.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces:
  - `select_event(event_id, mode, as_of=None) -> CpiEventKnowledge`
  - `apply_serving_overlay(knowledge, decision_time) -> GovernedCpiEvent`

- [ ] **Step 1: Encode canonical selector vectors as tests**

Create table-driven tests for every vector in spec section 31.3.

- [ ] **Step 2: Implement knowledge-mode filtering**

For `SYSTEM_KNOWN_PIT(T)`, filter every governable evidence type by its own `accepted_at <= T`, then apply only interpretation decisions with `applied_at <= T`.

Never use storage `created_at` as PIT knowledge time.

- [ ] **Step 3: Implement transitive provenance eligibility**

An observation is eligible only if:
- observation subject valid;
- pinned event-disclosure relation visible/valid;
- pinned disclosure-artifact relation visible/valid.

A marker similarly requires its artifact relation.

- [ ] **Step 4: Implement semantic material grouping**

Use `material_fingerprint` from Task 6:
- 0 materials -> UNRESOLVED;
- 1 VALUE -> VALUE;
- 1 unavailable material -> EXPLICIT_UNAVAILABLE;
- >1 distinct materials -> CONFLICT.

Different provenance rows with the same material remain one resolved semantic material.

- [ ] **Step 5: Implement schedule authority-tier selection**

For every visible/valid schedule assertion:
- join its source artifact;
- derive surface role from `source_contract_version + artifact_contract_kind`
  using the versioned CPI source contract;
- if any eligible AUTHORITATIVE evidence exists, FALLBACK_CORROBORATION does not
  participate in public applicable-schedule selection;
- within the active authority tier, first collapse identical semantic material;
- when differing material has comparable source-effective chronology, apply the
  later applicable source chronology;
- when chronology is unknown/date-only and cannot be safely ordered, return
  schedule conflict rather than using accepted_at or synthetic time;
- use captured_at only when the source contract explicitly permits capture
  chronology. CPI schedule HTML and global ICS v1 both set this to false.

- [ ] **Step 6: Implement release projection**

Cover:
- NOT_YET_DUE;
- DUE_DATE_UNTIMED;
- AWAITING_CONFIRMATION;
- DISCLOSED;
- NO_RELEASE_EXPECTED;
- UNRESOLVED;
- CONFLICT.

A valid envelope + quarantined observations yields DISCLOSED with observation resolution UNRESOLVED.

- [ ] **Step 7: Define the selector knowledge fingerprint**

Build a canonical selector-owned payload and SHA-256 fingerprint over:
- selector contract version;
- event occurrence identity;
- release projection state;
- each Core 4 resolution state;
- sorted distinct eligible semantic material fingerprints for each Core 4 item.

Exclude request/as-of time, storage timestamps, duplicate provenance, and all
serving-control history. Add stability/change-vector tests.

- [ ] **Step 8: Implement serving overlay separately**

Knowledge result is unchanged by serving-control history.

Live governed overlay:
- calculate latest CPI_DOMAIN and EVENT_OCCURRENCE control states;
- deny-overrides;
- WITHHELD removes numeric values from governed consumer model.

- [ ] **Step 9: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_selector -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
git add src/cpi_w1_selector.py tests
git commit -m "feat: add CPI source and PIT selectors"
```

---

### Task 12: Implement Governance Client and Emergency Serving Control

**Files:**
- Create: `src/cpi_w1_governance.py`
- Create: `tests/test_cpi_w1_governance.py`
- Modify: `tests/integration/test_cpi_w1_postgres.py`

**Interfaces:**
- Produces:
  - `create_interpretation_request(...)`
  - `record_interpretation_approval(...)`
  - `activate_interpretation_request(...)`
  - `apply_serving_control(...)`

- [ ] **Step 1: Write failing application-layer tests**

Test application refuses:
- empty workforce subject;
- proposer as approver;
- caller-supplied proposer/approver/actor identity that differs from the
  authenticated IdP subject;
- display-name/email identity in place of the immutable IdP subject identifier;
- arbitrary expiry supplied by caller;
- arbitrary decision/control version supplied by caller.

- [ ] **Step 2: Implement request creation with policy-owned expiry**

For governance policy v1, compute expiry inside the service from one constant policy duration and persist `governance_policy_version="cpi-governance-v1"`.

Do not let CLI/API callers set approval count.
Derive proposer/approver/actor subjects from authenticated workforce context; public
method/request payloads must not accept arbitrary actor identities.

- [ ] **Step 3: Implement activation wrappers**

Call only the migration's guarded DB functions. Do not duplicate race-sensitive activation logic in Python.

For EVENT_OCCURRENCE re-enable, recompute the current selector knowledge fingerprint
immediately before activation, require a lowercase 64-character SHA-256 operator-
verified fingerprint, compare the two, and refuse activation on mismatch or while
knowledge remains CONFLICT/UNRESOLVED.

- [ ] **Step 4: Add end-to-end correction test**

1. accept one observation;
2. selector returns VALUE;
3. serving control -> WITHHELD;
4. selector knowledge still VALUE but governed result WITHHELD;
5. create INVALID request;
6. separate approver approves;
7. activate INVALID;
8. source reconstruction excludes assertion;
9. accept corrected evidence;
10. verify selector;
11. serving control -> ENABLED.

- [ ] **Step 5: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_governance -v
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest tests.integration.test_cpi_w1_postgres -v
git add src/cpi_w1_governance.py tests
git commit -m "feat: add CPI governance application boundary"
```

---

### Task 13: Build Corpus Manifest and Differential Replay Tool

**Mandatory exceptional corpus coverage:** include the October 2025 CPI
nonpublication case using explicit official BLS archive/exception evidence. Prove
that schedule-row absence, HTTP 404, and stale current-release content cannot produce
CANCELED/NO_RELEASE_EXPECTED, while explicit scoped BLS nonpublication evidence can
support the governed nonpublication path.


**Files:**
- Create: `tests/fixtures/cpi_w1/corpus.json`
- Create: `scripts/replay_cpi_w1_corpus.py`
- Create: `tests/test_cpi_w1_corpus.py`
- Modify: `config/cpi_releases.json` only if comments/metadata are externalized; do not change its legacy semantics.

**Interfaces:**
- Produces deterministic replay report JSON.
- Does not mutate production/legacy data.

- [ ] **Step 1: Define corpus manifest schema**

Each entry contains:
- reference_month;
- artifact_contract_kind;
- source_contract_version;
- official locator;
- expected SHA-256 when pinned;
- local materialized path when present;
- expected semantic summary;
- exceptional-case tags.

Baseline reference-month window: 2022-01 through 2026-08.

- [ ] **Step 2: Write corpus-validation tests**

Require explicit entries/tags for:
- negative MoM;
- zero;
- January seasonal-revision releases;
- 2025-10 cancellation;
- 2025-11 explicit unavailable;
- post-shutdown recovery;
- current/latest baseline release.

- [ ] **Step 3: Implement replay command**

Command:

```bash
.venv/bin/python scripts/replay_cpi_w1_corpus.py   --manifest tests/fixtures/cpi_w1/corpus.json   --extractor-version bls-cpi-extractor-v1   --output /tmp/cpi-replay.json
```

Report statuses:
- SEMANTIC_UNCHANGED
- EXPECTED_CHANGED
- UNEXPECTED_CHANGED
- NEWLY_FAILED
- NEWLY_ACCEPTED

Exit non-zero for unexpected changes/new failures unless explicitly approved in a checked-in expected-diff file.

- [ ] **Step 4: Ensure corpus replay never writes canonical DB**

Unit test monkey-patches `psycopg.connect` and fails if called.

- [ ] **Step 5: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_cpi_w1_corpus -v
git add tests/fixtures/cpi_w1/corpus.json scripts/replay_cpi_w1_corpus.py tests/test_cpi_w1_corpus.py
git commit -m "test: add CPI W1 golden corpus replay"
```

---

### Task 14: Add Controlled Collector/Orchestrator Entry Point

**Files:**
- Create: `scripts/collect_cpi_w1.py`
- Create: `tests/test_collect_cpi_w1.py`
- Modify: `.env.example`

**Interfaces:**
- Uses source client, artifact store, repository, and promotion-family work creation.
- Does not call legacy `src.cpi_ingestion.upsert_cpi_data`.

- [ ] **Step 1: Write failing CLI tests**

Test:
- `--mode live`, `--mode backfill`, `--mode replay`;
- replay requires source artifact/extractor version inputs;
- dry-run does not mutate DB;
- backfill does not enable customer side-effect flags;
- live old current-page returns DATA_NOT_AVAILABLE rather than CANCELED.

- [ ] **Step 2: Implement collector flow**

Flow:

```text
create run/work
 -> claim collector work
 -> bounded BLS fetch
 -> hash/store artifact
 -> persist source_artifact
 -> terminalize collector work
 -> orchestrator creates expected promotion-family work
```

For primary release HTML create both:
- envelope promotion work;
- observation-bundle promotion work.

Do not execute promoter logic inside the collector transaction.

- [ ] **Step 3: Add reconciliation command mode**

`--reconcile-promotions` scans committed CPI artifacts and inserts missing deterministic
promotion work. Duplicate execution converges by work key.

The same reconciliation pass calls repository
`finalize_ingestion_run_if_complete` for non-terminal CPI W1 runs. This closes the
crash window where all work items are terminal but the orchestrator died before
terminalizing the parent run. Run outcome remains database-derived from durable work
history; reconciliation does not invent an outcome.

- [ ] **Step 4: Run and commit**

```bash
.venv/bin/python -m unittest tests.test_collect_cpi_w1 -v
git add scripts/collect_cpi_w1.py tests/test_collect_cpi_w1.py .env.example
git commit -m "feat: add controlled CPI W1 collector orchestration"
```

---

### Task 15: Verify Legacy Coexistence, Full CI, and Migration Safety

**Files:**
- Modify: `tests/test_cpi_ingestion.py`
- Modify: `tests/test_event_session_foundation_migration.py` only to add non-regression assertions; never change expected migration 009 contents.
- Create: `tests/test_cpi_w1_legacy_coexistence.py`
- Modify: `docs/engineering/current-vs-target.md`
- Modify: `docs/architecture/current-system.md`

**Interfaces:**
- Produces evidence that W1 foundation is additive and current serving remains legacy until a later Product Serving migration.

- [ ] **Step 1: Add explicit legacy non-regression tests**

Assert:
- `src.cpi_ingestion.CpiRelease.event_id` legacy format remains unchanged for current legacy callers;
- migration 009 checksum/content is untouched;
- new W1 modules do not import legacy `upsert_cpi_data`;
- legacy `WorkItemOutcome` still has its existing values and has not silently gained QUARANTINED.

- [ ] **Step 2: Run the entire unit suite**

```bash
.venv/bin/python -m compileall -q src scripts tests
.venv/bin/python -m unittest discover -s tests -v
node --test tests/research_ui.cjs
```

Expected: PASS.

- [ ] **Step 3: Run all PostgreSQL integration suites used by CI**

```bash
RUN_POSTGRES_INTEGRATION=1 DATABASE_URL=postgresql://market:market@localhost:55432/market   .venv/bin/python -m unittest   tests.integration.test_event_session_foundation_postgres   tests.integration.test_postgres_market_bars   tests.integration.test_validation_lineage_postgres   tests.integration.test_cpi_w1_postgres -v
```

Expected: PASS.

- [ ] **Step 4: Prove fresh-schema and upgrade-path equivalence**

On two clean PostgreSQL databases:
- Database A: apply 001 through 013 in order.
- Database B: apply current 001 through 009 baseline first, then 010 through 013.

Compare:
- new W1 table/constraint/function inventory;
- deterministic reference rows;
- selector integration outputs.

Do not compare sequence counters or timestamps that are intentionally runtime-generated.

- [ ] **Step 5: Update current/target docs truthfully**

Mark:
- CPI W1 DB foundation implemented only after migrations/tests actually exist;
- legacy FastAPI still serves legacy data;
- target Product API not implemented;
- production runtime/security roles not implemented;
- BLS W1 collector/promoter status according to actual completed tasks.

- [ ] **Step 6: Commit**

```bash
git add tests docs
git commit -m "test: verify CPI W1 legacy coexistence"
```

---

### Task 16: Final Branch Verification and Evidence Package

**Files:**
- Create: `docs/evidence/cpi-w1-foundation/README.md`
- Create: `docs/evidence/cpi-w1-foundation/verification.txt`

**Interfaces:**
- Produces review evidence only; no semantic authority.

- [ ] **Step 1: Run clean verification**

Record exact commands and outputs for:
- compileall;
- full unittest suite;
- Node UI regression;
- PostgreSQL W1 integration;
- migration fresh/upgrade equivalence;
- corpus replay.

- [ ] **Step 2: Record expected current limitations**

Evidence README must explicitly state:
- Spring Product API not implemented;
- Next.js UI not implemented;
- GCP production workload identities/grants not implemented;
- Cloud object-store production adapter not implemented;
- public beta readiness not claimed;
- current FastAPI remains legacy serving.

- [ ] **Step 3: Inspect git diff for forbidden scope creep**

Run:

```bash
git diff --stat main...HEAD
git diff --name-only main...HEAD
```

Expected: no Spring/Next/billing/market-data broad refactor files.

- [ ] **Step 4: Commit**

```bash
git add docs/evidence/cpi-w1-foundation
git commit -m "docs: record CPI W1 foundation verification"
```

- [ ] **Step 5: Run final status check**

```bash
git status --short
```

Expected: empty output.

