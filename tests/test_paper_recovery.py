import unittest

import httpx

from src.paper_execution import AlpacaPaperBroker, BrokerUnavailable


class PaperReadSnapshotTest(unittest.TestCase):
    def broker(self, response):
        self.calls = []
        def handle(request):
            self.calls.append(request)
            return response
        return AlpacaPaperBroker("test", "test", transport=httpx.MockTransport(handle))

    def test_position_reads_signed_quantity_only(self):
        broker = self.broker(httpx.Response(200, json={"symbol": "NVDA", "qty": "-1.5"}))
        self.assertEqual(str(broker.position_qty("NVDA")), "-1.5")
        self.assertEqual(self.calls[0].method, "GET")
        self.assertEqual(str(self.calls[0].url), "https://paper-api.alpaca.markets/v2/positions/NVDA")

    def test_only_position_404_means_zero(self):
        self.assertEqual(self.broker(httpx.Response(404)).position_qty("NVDA"), 0)
        for response in (httpx.Response(503), httpx.Response(204),
                         httpx.Response(200, json={"symbol": "AAPL", "qty": "1"}),
                         httpx.Response(200, json={"symbol": "NVDA", "qty": "NaN"}),
                         httpx.Response(200, json={})):
            with self.subTest(response=response), self.assertRaises(BrokerUnavailable):
                self.broker(response).position_qty("NVDA")

    def test_position_symbol_cannot_change_path(self):
        broker = self.broker(httpx.Response(404))
        with self.assertRaises(ValueError):
            broker.position_qty("../orders")
        self.assertEqual(self.calls, [])

    def test_clock_validates_timezone_and_bool(self):
        valid = {"timestamp": "2026-09-07T01:00:00-04:00", "is_open": False,
                 "next_open": "2026-09-08T09:30:00-04:00",
                 "next_close": "2026-09-08T16:00:00-04:00"}
        self.assertEqual(self.broker(httpx.Response(200, json=valid)).clock(), valid)
        for changes in ({"is_open": "false"}, {"timestamp": "2026-09-07T01:00:00"},
                        {"next_open": None}):
            with self.assertRaises(BrokerUnavailable):
                self.broker(httpx.Response(200, json={**valid, **changes})).clock()


if __name__ == "__main__":
    unittest.main()
