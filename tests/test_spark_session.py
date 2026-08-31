import unittest
import sys

from src.spark_session import create_local_spark


class SparkSessionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_local_spark("spark-session-test")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_runs_local_dataframe_action_in_utc(self) -> None:
        row_count = self.spark.createDataFrame([(1,)], ["value"]).count()

        self.assertEqual(row_count, 1)
        self.assertEqual(self.spark.conf.get("spark.sql.session.timeZone"), "UTC")
        self.assertEqual(self.spark.conf.get("spark.sql.shuffle.partitions"), "2")
        self.assertEqual(self.spark.conf.get("spark.sql.caseSensitive"), "true")
        self.assertEqual(self.spark.conf.get("spark.pyspark.python"), sys.executable)


if __name__ == "__main__":
    unittest.main()
