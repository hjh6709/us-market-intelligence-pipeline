"""Explicit paper connectivity probes. Never consumes historical strategy signals."""

import hashlib
from decimal import Decimal
from typing import Literal

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, model_validator


class OrderIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.]{0,9}$")
    qty: int = Field(strict=True, ge=1, le=10)
    limit_price: Decimal = Field(gt=0, allow_inf_nan=False, decimal_places=2)
    purpose: Literal["connectivity_probe"] = "connectivity_probe"

    @model_validator(mode="after")
    def validate_notional(self):
        if self.qty * self.limit_price > 1000:
            raise ValueError("paper probe notional exceeds USD 1000")
        return self

    def canonical(self) -> dict:
        data = self.model_dump(mode="json")
        data["limit_price"] = format(self.limit_price.normalize(), "f")
        return data


class BrokerUnavailable(RuntimeError):
    pass


class AlpacaPaperBroker:
    """Fixed paper endpoint, no redirects, no automatic POST retry."""

    def __init__(self, key: str, secret: str, *, transport=None):
        if not key.strip() or not secret.strip():
            raise ValueError("paper credentials are missing")
        self._headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        self._transport = transport

    def _request(self, method: str, path: str, **kwargs):
        try:
            with httpx.Client(transport=self._transport, timeout=10,
                              follow_redirects=False, trust_env=False) as client:
                response = client.request(method, "https://paper-api.alpaca.markets" + path,
                                          headers=self._headers, **kwargs)
                if method == "GET" and response.status_code == 404:
                    return None
                if not 200 <= response.status_code < 300:
                    raise BrokerUnavailable(f"paper broker HTTP {response.status_code}")
                if response.status_code == 204:
                    return None
                result = response.json()
                if not isinstance(result, dict):
                    raise BrokerUnavailable("invalid paper broker response")
                return result
        except (httpx.HTTPError, ValueError):
            raise BrokerUnavailable("paper broker response unavailable") from None

    def account(self):
        result = self._request("GET", "/v2/account")
        if not result or not result.get("id"):
            raise BrokerUnavailable("paper account unavailable")
        return result

    def submit(self, client_order_id: str, intent: OrderIntent):
        return self._request("POST", "/v2/orders", json={
            "client_order_id": client_order_id, "symbol": intent.symbol,
            "qty": str(intent.qty), "side": "buy", "type": "limit",
            "time_in_force": "day", "limit_price": intent.canonical()["limit_price"],
            "extended_hours": False,
        })

    def lookup(self, client_order_id: str):
        return self._request("GET", "/v2/orders:by_client_order_id",
                             params={"client_order_id": client_order_id})

    def cancel(self, broker_order_id: str):
        # IDs originate from Alpaca responses; never allow them to change the path.
        from uuid import UUID
        order_id = str(UUID(broker_order_id))
        self._request("DELETE", f"/v2/orders/{order_id}")


TERMINAL = frozenset({"filled", "canceled", "expired", "rejected", "replaced"})
BROKER_STATES = TERMINAL | frozenset({
    "new", "accepted", "pending_new", "partially_filled", "pending_cancel",
    "pending_replace", "done_for_day", "accepted_for_bidding", "stopped",
    "suspended", "calculated",
})


class PaperOrderService:
    def __init__(self, database_url: str, broker: AlpacaPaperBroker, scope: str,
                 *, connect=psycopg.connect):
        if not scope.startswith(("alpaca-paper:", "local-mock:")):
            raise ValueError("explicit paper or local-mock scope required")
        self.database_url = database_url
        self.broker = broker
        self.scope = scope
        self.connect = connect

    def run(self, intent: OrderIntent, action: str = "reconcile", *, enabled=False):
        if action not in {"submit", "reconcile", "cancel"}:
            raise ValueError("invalid paper action")
        digest = hashlib.sha256(f"{self.scope}|{intent.request_id}".encode()).hexdigest()
        client_id = "paper-" + digest[:40]
        lock_key = int(digest[:16], 16)
        if lock_key >= 2**63:
            lock_key -= 2**64
        with self.connect(self.database_url, autocommit=True, row_factory=dict_row,
                          connect_timeout=5) as db:
            # Session lock survives each durable statement and releases on connection close.
            db.execute("SET lock_timeout = '15s'")
            db.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
            row = db.execute(
                "SELECT * FROM paper_order_intents WHERE scope=%s AND request_id=%s",
                (self.scope, intent.request_id),
            ).fetchone()
            if row is not None and row["intent"] != intent.canonical():
                raise ValueError("request ID already belongs to a different intent")
            fresh = row is None
            if fresh:
                if action != "submit" or not enabled:
                    raise ValueError("new paper submission requires explicit enablement")
                # Commit BEFORE crossing the broker boundary. A crash cannot permit replay.
                row = db.execute(
                    """INSERT INTO paper_order_intents
                       (scope, request_id, client_order_id, intent)
                       VALUES (%s,%s,%s,%s) RETURNING *""",
                    (self.scope, intent.request_id, client_id, Jsonb(intent.canonical())),
                ).fetchone()
            try:
                result = self.broker.submit(client_id, intent) if fresh else self.broker.lookup(client_id)
                if result is None:
                    return self._save_error(db, intent, "not_found_no_resubmit")
                row = self._record(db, intent, row, result)
                if action == "cancel" and row["state"] not in TERMINAL:
                    if not enabled:
                        raise ValueError("paper cancellation requires explicit enablement")
                    self.broker.cancel(row["broker_order_id"])
                    # DELETE acknowledgement is not a canceled fill state.
                    result = self.broker.lookup(client_id)
                    if result is None:
                        return self._save_error(db, intent, "cancel_status_unknown")
                    row = self._record(db, intent, row, result)
                return self._public(row)
            except BrokerUnavailable:
                return self._save_error(db, intent, "broker_unavailable_reconcile_required")

    def _record(self, db, intent, row, result):
        try:
            from uuid import UUID
            order_id = str(UUID(result["id"]))
            filled = Decimal(str(result["filled_qty"]))
            status = result["status"]
            valid = (
                result["client_order_id"] == row["client_order_id"]
                and result["symbol"] == intent.symbol
                and result["side"] == "buy" and result["type"] == "limit"
                and result["time_in_force"] == "day"
                and Decimal(str(result["qty"])) == intent.qty
                and Decimal(str(result["limit_price"])) == intent.limit_price
                and filled.is_finite() and row["filled_qty"] <= filled <= intent.qty
                and status in BROKER_STATES
                and (status != "filled" or filled == intent.qty)
                and (status != "partially_filled" or 0 < filled < intent.qty)
                and (row["broker_order_id"] in (None, order_id))
                and (row["state"] not in TERMINAL or row["state"] == status)
            )
        except (KeyError, ValueError, TypeError, ArithmeticError):
            valid = False
        if not valid:
            raise BrokerUnavailable("broker snapshot does not match journal")
        return db.execute(
            """UPDATE paper_order_intents SET state=%s, broker_order_id=%s,
               filled_qty=%s, last_error=NULL, updated_at=CURRENT_TIMESTAMP
               WHERE scope=%s AND request_id=%s RETURNING *""",
            (status, order_id, filled, self.scope, intent.request_id),
        ).fetchone()

    def _save_error(self, db, intent, error):
        row = db.execute(
            """UPDATE paper_order_intents SET last_error=%s, updated_at=CURRENT_TIMESTAMP
               WHERE scope=%s AND request_id=%s RETURNING *""",
            (error, self.scope, intent.request_id),
        ).fetchone()
        return self._public(row)

    @staticmethod
    def _public(row):
        return {key: str(row[key]) if key == "filled_qty" else row[key]
                for key in ("request_id", "client_order_id", "state", "filled_qty", "last_error")}
