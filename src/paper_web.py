"""Web-safe facade for the isolated Alpaca Paper order subsystem."""

from __future__ import annotations

import os
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel

from src.paper_execution import AlpacaPaperBroker, OrderIntent, PaperOrderService


CONFIRMATION_TEXT = "SUBMIT PAPER ORDER"
CANCEL_CONFIRMATION_TEXT = "CANCEL PAPER ORDER"


class PaperConfirmationError(ValueError):
    pass


class PaperConfigurationError(RuntimeError):
    pass


class PaperOrderNotFoundError(LookupError):
    pass


class PaperSubmitRequest(BaseModel):
    intent: OrderIntent
    confirmation: str


class PaperConfirmationRequest(BaseModel):
    confirmation: str


class PaperWebService:
    def __init__(self, broker, order_service, *, enabled: bool) -> None:
        self.broker = broker
        self.order_service = order_service
        self.enabled = enabled

    def account(self) -> dict[str, Any]:
        account = self.broker.account()
        clock = self.broker.clock()
        ready = (
            account.get("status") == "ACTIVE"
            and account.get("trading_blocked") is False
            and account.get("account_blocked") is False
        )
        return {
            "broker": "alpaca-paper",
            "account": {
                "status": account.get("status"),
                "currency": account.get("currency"),
                "equity": account.get("equity"),
                "ready": ready,
            },
            "clock": clock,
            "paper_order_submission_enabled": self.enabled,
            "live_trading_enabled": False,
        }

    def review(self, intent: OrderIntent) -> dict[str, Any]:
        return {
            "intent": intent.canonical(),
            "notional_usd": str(intent.qty * intent.limit_price),
            "guardrails": {
                "broker": "Alpaca Paper only",
                "side": "BUY",
                "order_type": "LIMIT",
                "time_in_force": "DAY",
                "extended_hours": False,
                "quantity_range": [1, 10],
                "maximum_notional_usd": 1000,
                "automatic_post_retry": False,
            },
            "required_confirmation": CONFIRMATION_TEXT,
            "broker_request_sent": False,
            "research_input_accepted": False,
        }

    def submit(self, intent: OrderIntent, confirmation: str) -> dict[str, Any]:
        if not self.enabled:
            raise PaperConfirmationError("paper web order submission is disabled")
        if confirmation != CONFIRMATION_TEXT:
            raise PaperConfirmationError("exact paper confirmation is required")
        return self.order_service.run(intent, "submit", enabled=True)

    def recover(self) -> dict[str, Any]:
        return {
            **self.order_service.recover(),
            "broker": "alpaca-paper",
            "recovery_mode": "GET_ONLY",
            "live_trading_enabled": False,
        }

    def reconcile(self, intent: OrderIntent) -> dict[str, Any]:
        return self.order_service.run(intent, "reconcile", enabled=False)

    def cancel(self, intent: OrderIntent, confirmation: str) -> dict[str, Any]:
        if not self.enabled:
            raise PaperConfirmationError("paper web cancellation is disabled")
        if confirmation != CANCEL_CONFIRMATION_TEXT:
            raise PaperConfirmationError("exact paper cancellation confirmation is required")
        return self.order_service.run(intent, "cancel", enabled=True)


class ConfiguredPaperWebGateway:
    """Build a paper-only service from server configuration for each request."""

    def __init__(self, database_url: str, *, environ=None, connect=psycopg.connect):
        self.database_url = database_url
        self.environ = environ if environ is not None else os.environ
        self.connect = connect

    def _credentials(self) -> tuple[str, str]:
        key = self.environ.get("ALPACA_PAPER_KEY_ID") or self.environ.get(
            "APCA_API_KEY_ID", ""
        )
        secret = self.environ.get("ALPACA_PAPER_SECRET_KEY") or self.environ.get(
            "APCA_API_SECRET_KEY", ""
        )
        return key, secret

    def _scope(self) -> str:
        account_id = self.environ.get("ALPACA_PAPER_ACCOUNT_ID", "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", account_id):
            raise PaperConfigurationError("ALPACA_PAPER_ACCOUNT_ID must pin the local paper journal account")
        return "alpaca-paper:" + account_id

    def _service(self, *, verify_account: bool = False) -> tuple[PaperWebService, str]:
        scope = self._scope()
        key, secret = self._credentials()
        if not key or not secret:
            raise PaperConfigurationError("dedicated paper credentials are not configured")
        broker = AlpacaPaperBroker(key, secret)
        if verify_account:
            account = broker.account()
            if scope != "alpaca-paper:" + str(account.get("id", "")):
                raise PaperConfigurationError("configured paper account does not match broker credentials")
        enabled = self.environ.get("ENABLE_PAPER_WEB_ORDERS") == "true"
        service = PaperWebService(
            broker,
            PaperOrderService(self.database_url, broker, scope, connect=self.connect),
            enabled=enabled,
        )
        return service, scope

    def account(self):
        service, _scope = self._service(verify_account=True)
        return service.account()

    def review(self, intent: OrderIntent):
        service, _scope = self._service()
        return service.review(intent)

    def submit(self, intent: OrderIntent, confirmation: str):
        service, _scope = self._service(verify_account=True)
        return service.submit(intent, confirmation)

    def recover(self):
        service, _scope = self._service(verify_account=True)
        return service.recover()

    def _load_intent(self, scope: str, request_id: str) -> OrderIntent:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", request_id):
            raise PaperOrderNotFoundError(request_id)
        with self.connect(
            self.database_url, row_factory=dict_row, connect_timeout=5
        ) as connection:
            row = connection.execute(
                "SELECT intent FROM paper_order_intents WHERE scope=%s AND request_id=%s",
                (scope, request_id),
            ).fetchone()
        if row is None:
            raise PaperOrderNotFoundError(request_id)
        return OrderIntent(**row["intent"])

    def reconcile(self, request_id: str):
        service, scope = self._service(verify_account=True)
        return service.reconcile(self._load_intent(scope, request_id))

    def cancel(self, request_id: str, confirmation: str):
        service, scope = self._service(verify_account=True)
        return service.cancel(self._load_intent(scope, request_id), confirmation)

    def orders(self) -> list[dict[str, Any]]:
        scope = self._scope()
        with self.connect(
            self.database_url, row_factory=dict_row, connect_timeout=5
        ) as connection:
            rows = connection.execute(
                """SELECT request_id, intent, state, filled_qty, last_error,
                          created_at, updated_at
                   FROM paper_order_intents WHERE scope = %s
                   ORDER BY created_at DESC, request_id DESC LIMIT 100""",
                (scope,),
            ).fetchall()
        return [
            {
                **row,
                "filled_qty": str(row["filled_qty"]),
                "broker": "alpaca-paper",
            }
            for row in rows
        ]
