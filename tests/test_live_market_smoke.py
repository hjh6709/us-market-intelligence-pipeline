import json
import tempfile
import unittest
from pathlib import Path

from src.live_market_smoke import (
    AlpacaStreamError,
    build_auth_message,
    build_subscribe_message,
    is_complete_trade,
    load_credentials,
    require_success,
)


class LiveMarketSmokeTest(unittest.TestCase):
    def test_builds_alpaca_websocket_auth_message(self) -> None:
        message = json.loads(build_auth_message("key-id", "secret"))

        self.assertEqual(
            message,
            {"action": "auth", "key": "key-id", "secret": "secret"},
        )

    def test_builds_trade_subscription_for_requested_symbols(self) -> None:
        message = json.loads(build_subscribe_message(["SPY", "QQQ", "NVDA"]))

        self.assertEqual(
            message,
            {"action": "subscribe", "trades": ["SPY", "QQQ", "NVDA"]},
        )

    def test_recognizes_complete_alpaca_trade_contract(self) -> None:
        trade = {
            "T": "t",
            "S": "NVDA",
            "i": 12345,
            "x": "V",
            "p": 182.10,
            "s": 100,
            "c": ["@"],
            "t": "2026-08-19T13:30:00.123456Z",
            "z": "C",
        }

        self.assertTrue(is_complete_trade(trade))
        self.assertFalse(is_complete_trade({key: value for key, value in trade.items() if key != "i"}))

    def test_loads_credentials_from_ignored_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "APCA_API_KEY_ID=paper-key\nAPCA_API_SECRET_KEY=paper-secret\n",
                encoding="utf-8",
            )

            credentials = load_credentials({}, env_path)

        self.assertEqual(credentials, ("paper-key", "paper-secret"))

    def test_rejects_missing_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("APCA_API_KEY_ID=\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Alpaca credentials are missing"):
                load_credentials({}, env_path)

    def test_stops_when_alpaca_rejects_authentication(self) -> None:
        response = [{"T": "error", "code": 406, "msg": "connection limit exceeded"}]

        with self.assertRaisesRegex(AlpacaStreamError, "406.*connection limit exceeded"):
            require_success(response, "authenticated")


if __name__ == "__main__":
    unittest.main()
