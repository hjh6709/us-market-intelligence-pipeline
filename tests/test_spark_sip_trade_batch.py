import unittest

from src.spark_sip_trade_batch import _spark_offset_json


class SparkSipTradeBatchTest(unittest.TestCase):
    def test_builds_exact_kafka_start_and_end_offsets(self) -> None:
        ranges = [
            {"topic": "raw.market-sip.v1", "partition": 2, "start": 41, "end": 43},
            {"topic": "raw.market-sip.v1", "partition": 0, "start": 10, "end": 12},
        ]

        assignment, starts, ends = _spark_offset_json("raw.market-sip.v1", ranges)

        self.assertEqual(assignment, '{"raw.market-sip.v1":[2,0]}')
        self.assertEqual(starts, '{"raw.market-sip.v1":{"2":41,"0":10}}')
        self.assertEqual(ends, '{"raw.market-sip.v1":{"2":43,"0":12}}')

    def test_rejects_ranges_for_another_topic(self) -> None:
        with self.assertRaisesRegex(ValueError, "topic"):
            _spark_offset_json(
                "raw.market-sip.v1",
                [
                    {
                        "topic": "raw.market.v1",
                        "partition": 0,
                        "start": 10,
                        "end": 12,
                    }
                ],
            )


if __name__ == "__main__":
    unittest.main()
