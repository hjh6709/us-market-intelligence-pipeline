import unittest
from decimal import Decimal

from src.paper_execution import OrderIntent
from src.paper_web import ConfiguredPaperWebGateway, PaperConfirmationError, PaperWebService


class FakeBroker:
    def account(self):
        return {
            "id": "secret-account-id",
            "status": "ACTIVE",
            "currency": "USD",
            "equity": "100000",
            "trading_blocked": False,
            "account_blocked": False,
        }

    def clock(self):
        return {"timestamp": "2026-09-09T00:00:00+00:00", "is_open": False,
                "next_open": "2026-09-09T13:30:00+00:00", "next_close": "2026-09-09T20:00:00+00:00"}


class FakeOrderService:
    def __init__(self):
        self.calls = []

    def run(self, intent, action, *, enabled=False):
        self.calls.append((intent, action, enabled))
        return {"request_id": intent.request_id, "state": "accepted", "filled_qty": "0", "last_error": None}

    def recover(self):
        return {"journal_orders": 0, "journal_status": "EMPTY", "position_reconciliation_verified": False, "new_orders_submitted": 0, "orders": [], "positions": []}


class PaperWebServiceTest(unittest.TestCase):
    def setUp(self):
        self.orders = FakeOrderService()
        self.service = PaperWebService(FakeBroker(), self.orders, enabled=True)
        self.intent = OrderIntent(request_id="web-1", symbol="NVDA", qty=1, limit_price=Decimal("100"))

    def test_review_has_no_broker_write_and_exposes_real_guardrails(self):
        result = self.service.review(self.intent)

        self.assertEqual(self.orders.calls, [])
        self.assertEqual(result["intent"]["symbol"], "NVDA")
        self.assertEqual(result["guardrails"]["maximum_notional_usd"], 1000)
        self.assertEqual(result["required_confirmation"], "SUBMIT PAPER ORDER")

    def test_submit_requires_exact_confirmation_and_server_enablement(self):
        with self.assertRaises(PaperConfirmationError):
            self.service.submit(self.intent, "yes")
        self.assertEqual(self.orders.calls, [])

        result = self.service.submit(self.intent, "SUBMIT PAPER ORDER")

        self.assertEqual(result["state"], "accepted")
        self.assertEqual(self.orders.calls, [(self.intent, "submit", True)])

    def test_account_response_is_sanitized_and_recovery_is_get_only(self):
        account = self.service.account()
        recovery = self.service.recover()

        self.assertNotIn("id", account["account"])
        self.assertFalse(account["live_trading_enabled"])
        self.assertEqual(recovery["new_orders_submitted"], 0)
        self.assertFalse(recovery["position_reconciliation_verified"])

    def test_reconcile_is_read_only_and_cancel_requires_confirmation(self):
        self.service.reconcile(self.intent)
        self.assertEqual(self.orders.calls[-1], (self.intent, "reconcile", False))

        with self.assertRaises(PaperConfirmationError):
            self.service.cancel(self.intent, "cancel")
        self.service.cancel(self.intent, "CANCEL PAPER ORDER")
        self.assertEqual(self.orders.calls[-1], (self.intent, "cancel", True))

    def test_web_gateway_accepts_existing_paper_keys_without_exposing_them(self):
        gateway = ConfiguredPaperWebGateway(
            "postgresql://unused",
            environ={"APCA_API_KEY_ID": "paper-key", "APCA_API_SECRET_KEY": "paper-secret"},
        )

        self.assertEqual(gateway._credentials(), ("paper-key", "paper-secret"))


if __name__ == "__main__":
    unittest.main()
