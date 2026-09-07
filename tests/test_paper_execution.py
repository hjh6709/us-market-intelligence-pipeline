import unittest
from decimal import Decimal

import httpx

from src.paper_execution import AlpacaPaperBroker, OrderIntent, BrokerUnavailable


class PaperBrokerTest(unittest.TestCase):
    def intent(self, **kwargs):
        return OrderIntent(request_id="probe-1", symbol="NVDA", qty=1,
                           limit_price=Decimal("100"), **kwargs)

    def test_only_paper_host_and_limit_buy_are_sent(self):
        seen = []
        def handle(request):
            seen.append(request)
            return httpx.Response(200, json={"id": "broker-1"})
        broker = AlpacaPaperBroker("key", "secret", transport=httpx.MockTransport(handle))
        broker.submit("paper-123", self.intent())
        import json
        self.assertEqual(str(seen[0].url), "https://paper-api.alpaca.markets/v2/orders")
        self.assertEqual(json.loads(seen[0].content), {
            "client_order_id": "paper-123", "symbol": "NVDA", "qty": "1",
            "side": "buy", "type": "limit", "time_in_force": "day",
            "limit_price": "100", "extended_hours": False,
        })

    def test_timeout_never_retries_or_exposes_credentials(self):
        calls = []
        def handle(request):
            calls.append(request)
            raise httpx.ReadTimeout("secret-value", request=request)
        broker = AlpacaPaperBroker("key", "secret", transport=httpx.MockTransport(handle))
        with self.assertRaises(BrokerUnavailable) as error:
            broker.submit("paper-123", self.intent())
        self.assertEqual(len(calls), 1)
        self.assertNotIn("secret-value", str(error.exception))

    def test_redirect_is_not_followed(self):
        calls = []
        def handle(request):
            calls.append(request)
            return httpx.Response(307, headers={"Location": "https://api.alpaca.markets/v2/orders"})
        broker = AlpacaPaperBroker("key", "secret", transport=httpx.MockTransport(handle))
        with self.assertRaises(BrokerUnavailable):
            broker.submit("paper-123", self.intent())
        self.assertEqual(len(calls), 1)

    def test_server_failure_and_invalid_json_do_not_retry(self):
        for response in (httpx.Response(503, text="secret-error-body"),
                         httpx.Response(200, text="not-json")):
            calls = []
            def handle(request):
                calls.append(request)
                return response
            broker = AlpacaPaperBroker("key", "secret", transport=httpx.MockTransport(handle))
            with self.assertRaises(BrokerUnavailable) as error:
                broker.submit("paper-123", self.intent())
            self.assertEqual(len(calls), 1)
            self.assertNotIn("secret-error-body", str(error.exception))

    def test_invalid_limits_are_rejected(self):
        for qty, price in [(0, "10"), (11, "10"), (2, "600"), (1, "NaN"), (1, "0")]:
            with self.subTest(qty=qty, price=price), self.assertRaises(ValueError):
                OrderIntent(request_id="probe", symbol="NVDA", qty=qty, limit_price=price)


if __name__ == "__main__":
    unittest.main()
