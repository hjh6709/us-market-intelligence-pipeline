import json
import unittest
from copy import deepcopy

from pyspark.sql import functions as F

from src.preprocess import (
    aggregate_minute_bars,
    apply_minute_bar_condition_policy,
    parse_market_events,
    prepare_streaming_trades,
    split_valid_invalid,
    validate_market_trades,
)
from src.spark_session import create_local_spark


def canonical_event() -> dict:
    return {
        "event_id": "sha256:one",
        "event_type": "market.trade.raw",
        "schema_version": 1,
        "source": "alpaca",
        "feed": "iex",
        "source_event_id": "23",
        "event_timestamp": "2026-08-19T13:30:00.102733Z",
        "ingested_at": "2026-08-19T13:30:01Z",
        "trace_id": "run-1",
        "payload": {
            "T": "t",
            "S": "NVDA",
            "i": 23,
            "x": "V",
            "p": 221.69,
            "s": 5,
            "c": ["@", "I"],
            "z": "C",
            "t": "2026-08-19T13:30:00.102733Z",
        },
    }


class PreprocessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_local_spark("preprocess-test")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def kafka_frame(self, values: list[str]):
        return self.spark.createDataFrame([(value,) for value in values], ["value"])

    def test_parses_canonical_trade_without_changing_market_meaning(self) -> None:
        parsed = parse_market_events(
            self.kafka_frame([json.dumps(canonical_event())])
        )
        valid, invalid = split_valid_invalid(
            validate_market_trades(parsed, ["SPY", "NVDA"])
        )

        row = valid.collect()[0]
        self.assertEqual(row.symbol, "NVDA")
        self.assertEqual(str(row.price), "221.690000")
        self.assertEqual(row.size, 5)
        self.assertEqual(row.exchange, "V")
        self.assertEqual(row.conditions, ["@", "I"])
        self.assertEqual(row.source_event_id, "23")
        self.assertEqual(invalid.count(), 0)

    def test_assigns_bounded_reason_codes_to_invalid_rows(self) -> None:
        cases: list[tuple[str, str]] = [("not-json", "MALFORMED_JSON")]

        mutations = [
            ("INVALID_EVENT_TYPE", lambda event: event.update(event_type="market.quote.raw")),
            ("UNSUPPORTED_SCHEMA_VERSION", lambda event: event.update(schema_version=2)),
            ("MISSING_EVENT_ID", lambda event: event.update(event_id=None)),
            ("MISSING_SOURCE_EVENT_ID", lambda event: event.update(source_event_id=None)),
            ("MISSING_SOURCE", lambda event: event.update(source="")),
            ("MISSING_FEED", lambda event: event.update(feed=None)),
            (
                "SOURCE_EVENT_ID_MISMATCH",
                lambda event: event.update(source_event_id="999"),
            ),
            ("SYMBOL_NOT_ALLOWED", lambda event: event["payload"].update(S="TSLA")),
            (
                "INVALID_PROVIDER_TRADE_ID",
                lambda event: event["payload"].update(i=None),
            ),
            ("INVALID_EXCHANGE", lambda event: event["payload"].update(x="")),
            ("INVALID_CONDITIONS", lambda event: event["payload"].update(c=None)),
            ("INVALID_TAPE", lambda event: event["payload"].update(z=None)),
            ("INVALID_PRICE", lambda event: event["payload"].update(p=0)),
            ("INVALID_SIZE", lambda event: event["payload"].update(s=-1)),
            ("INVALID_TIMESTAMP", lambda event: event["payload"].update(t="bad-time")),
            (
                "INVALID_TIMESTAMP",
                lambda event: (
                    event.update(event_timestamp="2026-08-19T13:30:00"),
                    event["payload"].update(t="2026-08-19T13:30:00"),
                ),
            ),
            (
                "TIMESTAMP_MISMATCH",
                lambda event: event["payload"].update(t="2026-08-19T13:31:00Z"),
            ),
        ]
        for expected_reason, mutate in mutations:
            event = deepcopy(canonical_event())
            event["event_id"] = f"sha256:{expected_reason.lower()}"
            mutate(event)
            cases.append((json.dumps(event), expected_reason))

        validated = validate_market_trades(
            parse_market_events(self.kafka_frame([value for value, _ in cases])),
            ["NVDA"],
        )
        valid, invalid = split_valid_invalid(validated)
        reasons_by_raw = {
            row.raw_value: set(row.reason_codes)
            for row in invalid.select("raw_value", "reason_codes").collect()
        }

        self.assertEqual(valid.count(), 0)
        for raw_value, expected_reason in cases:
            self.assertIn(
                expected_reason,
                reasons_by_raw.get(raw_value, set()),
                raw_value,
            )

    def valid_trades(self, events: list[dict]):
        parsed = parse_market_events(
            self.kafka_frame([json.dumps(event) for event in events])
        )
        valid, _ = split_valid_invalid(validate_market_trades(parsed, ["NVDA"]))
        return valid

    def test_aggregates_out_of_order_trades_by_event_time(self) -> None:
        event_specs = [
            ("sha256:middle", "2026-08-19T13:30:30Z", 105.0, 2),
            ("sha256:open", "2026-08-19T13:30:10Z", 100.0, 3),
            ("sha256:close", "2026-08-19T13:30:50Z", 102.0, 5),
        ]
        events = []
        for event_id, timestamp, price, size in event_specs:
            event = canonical_event()
            event["event_id"] = event_id
            event["source_event_id"] = str(len(events) + 1)
            event["event_timestamp"] = timestamp
            event["payload"].update(
                i=len(events) + 1, p=price, s=size, c=["@"], t=timestamp
            )
            events.append(event)

        bars = aggregate_minute_bars(self.valid_trades(events))
        row = bars.collect()[0]

        self.assertEqual(str(row.open), "100.000000")
        self.assertEqual(str(row.high), "105.000000")
        self.assertEqual(str(row.low), "100.000000")
        self.assertEqual(str(row.close), "102.000000")
        self.assertEqual(row.volume, 10)
        self.assertEqual(row.trade_count, 3)
        self.assertEqual(str(row.vwap), "102.000000")
        self.assertEqual(row.timeframe, "1m")
        self.assertTrue(row.is_final)
        self.assertEqual(row.condition_policy, "alpaca_sip_minute_v1")
        utc_bar_start = bars.select(
            F.date_format("bar_start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("value")
        ).collect()[0].value
        self.assertEqual(utc_bar_start, "2026-08-19T13:30:00Z")

    def test_rejects_zero_size_trade(self) -> None:
        event = canonical_event()
        event["payload"]["s"] = 0

        parsed = parse_market_events(self.kafka_frame([json.dumps(event)]))
        valid, invalid = split_valid_invalid(validate_market_trades(parsed, ["NVDA"]))

        self.assertEqual(valid.count(), 0)
        self.assertIn("INVALID_SIZE", invalid.collect()[0].reason_codes)

    def test_applies_strictest_sip_condition_to_ohlcv(self) -> None:
        specs = [
            ("regular", "2026-08-19T13:30:10Z", 100.0, 10, ["@"]),
            ("odd-lot", "2026-08-19T13:30:20Z", 50.0, 2, ["I"]),
            ("official-open", "2026-08-19T13:30:30Z", 200.0, 7, ["Q"]),
            ("strictest", "2026-08-19T13:30:40Z", 300.0, 3, ["@", "I"]),
            ("close", "2026-08-19T13:30:50Z", 110.0, 5, ["@"]),
        ]
        events = []
        for index, (name, timestamp, price, size, conditions) in enumerate(specs, 1):
            event = canonical_event()
            event["event_id"] = f"sha256:{name}"
            event["source_event_id"] = str(index)
            event["event_timestamp"] = timestamp
            event["payload"].update(
                i=index, p=price, s=size, c=conditions, t=timestamp
            )
            events.append(event)

        row = aggregate_minute_bars(self.valid_trades(events)).collect()[0]

        self.assertEqual(str(row.open), "100.000000")
        self.assertEqual(str(row.high), "110.000000")
        self.assertEqual(str(row.low), "100.000000")
        self.assertEqual(str(row.close), "110.000000")
        self.assertEqual(row.volume, 20)
        self.assertEqual(row.trade_count, 4)
        self.assertEqual(str(row.vwap), "103.333333")

    def test_excludes_unknown_condition_instead_of_silently_including_it(self) -> None:
        event = canonical_event()
        event["payload"]["c"] = ["UNKNOWN"]

        row = apply_minute_bar_condition_policy(
            self.valid_trades([event])
        ).collect()[0]

        self.assertEqual(row.unsupported_conditions, ["UNKNOWN"])
        self.assertFalse(row.updates_volume)
        self.assertFalse(row.updates_ohlc)

    def test_rejects_batch_dataframe_at_streaming_state_boundary(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires a streaming DataFrame"):
            prepare_streaming_trades(self.valid_trades([canonical_event()]))


if __name__ == "__main__":
    unittest.main()
