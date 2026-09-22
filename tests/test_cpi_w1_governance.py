import inspect
import unittest
from pathlib import Path
from uuid import UUID

from src.cpi_w1_contracts import ServingControlState
from src.cpi_w1_governance import (
    CpiW1Governance,
    GovernanceIdentityError,
    WorkforcePrincipal,
)


SOURCE = Path("src/cpi_w1_governance.py").read_text(encoding="utf-8")


class CpiW1GovernanceTest(unittest.TestCase):
    def test_workforce_principal_rejects_display_name_email_and_whitespace(self) -> None:
        with self.assertRaises(GovernanceIdentityError):
            WorkforcePrincipal("corp", "Jane Doe")
        with self.assertRaises(GovernanceIdentityError):
            WorkforcePrincipal("corp", "jane@example.com")
        with self.assertRaises(GovernanceIdentityError):
            WorkforcePrincipal("corp ", "immutable-sub")
        principal = WorkforcePrincipal("https://idp.example.com", "opaque:immutable-sub")
        self.assertTrue(principal.database_subject.startswith("idp:"))
        self.assertNotIn("https://", principal.database_subject)
        self.assertNotIn("opaque:immutable-sub", principal.database_subject)

    def test_oidc_issuer_url_and_opaque_subject_are_supported(self) -> None:
        principal = WorkforcePrincipal(
            "https://login.example.com/tenant/v2.0",
            "opaque:subject/123",
        )
        self.assertTrue(principal.database_subject.startswith("idp:"))

    def test_public_methods_do_not_accept_raw_actor_or_version_fields(self) -> None:
        for method_name in (
            "create_interpretation_request",
            "record_interpretation_approval",
            "activate_interpretation_request",
            "apply_serving_control",
        ):
            parameters = inspect.signature(
                getattr(CpiW1Governance, method_name)
            ).parameters
            for forbidden in (
                "proposer_subject",
                "approver_subject",
                "actor_subject",
                "expected_decision_version",
                "expected_control_version",
                "expires_at",
                "approval_count",
            ):
                self.assertNotIn(forbidden, parameters)

    def test_owned_transaction_helper_does_not_recurse(self) -> None:
        helper_start = SOURCE.index("def _owned_governance_transaction")
        helper_end = SOURCE.index("def _reason_code", helper_start)
        helper = SOURCE[helper_start:helper_end]
        self.assertIn("with connection.transaction():", helper)
        self.assertNotIn(
            "with _owned_governance_transaction(connection):",
            helper,
        )

    def test_governance_owns_mutation_transaction(self) -> None:
        self.assertIn("TransactionStatus.IDLE", SOURCE)
        self.assertIn("_owned_governance_transaction", SOURCE)
        self.assertIn(
            "CPI governance requires an idle connection",
            SOURCE,
        )

    def test_application_calls_guarded_database_functions(self) -> None:
        self.assertIn("SELECT apply_interpretation_decision", SOURCE)
        self.assertIn("SELECT apply_economic_serving_control", SOURCE)
        self.assertNotIn("INSERT INTO interpretation_decisions", SOURCE)
        self.assertNotIn("INSERT INTO economic_serving_control_decisions", SOURCE)

    def test_request_expiry_is_policy_owned_not_caller_owned(self) -> None:
        parameters = inspect.signature(
            CpiW1Governance.create_interpretation_request
        ).parameters
        self.assertNotIn("requested_at", parameters)
        self.assertNotIn("expires_at", parameters)
        self.assertIn('_POLICY_VERSION = "cpi-governance-v1"', SOURCE)

    def test_event_reenable_uses_same_event_fence_as_promoter(self) -> None:
        self.assertIn("self.repository.lock_cpi_event", SOURCE)
        self.assertIn("_select_current_event_in_caller_transaction", SOURCE)
        self.assertLess(
            SOURCE.index("self.repository.lock_cpi_event"),
            SOURCE.index("_select_current_event_in_caller_transaction"),
        )

    def test_event_reenable_recomputes_and_compares_knowledge_fingerprint(self) -> None:
        self.assertIn("knowledge.knowledge_fingerprint", SOURCE)
        self.assertIn("operator-verified knowledge fingerprint is stale", SOURCE)
        self.assertIn(
            "event cannot be re-enabled while CPI knowledge is unresolved/conflicting",
            SOURCE,
        )

    def test_domain_reenable_requires_verification_reference(self) -> None:
        source = inspect.signature(CpiW1Governance.apply_serving_control).parameters
        self.assertIn("verification_ref", source)

    def test_serving_state_is_typed(self) -> None:
        self.assertEqual(ServingControlState.WITHHELD.value, "WITHHELD")


if __name__ == "__main__":
    unittest.main()
