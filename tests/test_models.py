import unittest
from datetime import date, datetime
from ctable import ColumnDef


class TestModels(unittest.TestCase):
    def test_column_match_all(self):
        col = ColumnDef(name="user", data_type="string", max_length=50, value_source="key", value_index=0)
        val = self._get_column_values(col)
        self.assertEqual(val, ["1", "2"])

    def test_column_match(self):
        col = ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                        match_key={
                            "index": 1,
                            "value": "indicator_a"
                        })
        val = self._get_column_values(col)
        self.assertEqual(val, [1])

    def test_column_date(self):
        col = ColumnDef(name="date", data_type="date", data_format="%Y-%m-%dT%H:%M:%S.%fZ",
                        value_source="key", value_index=2)
        val = self._get_column_values(col)
        self.assertEqual(val[0], date(2013, 3, 1))
        self.assertEqual(val[1], date(2013, 3, 2))

    def test_column_datetime(self):
        col = ColumnDef(name="date", data_type="datetime", data_format="%Y-%m-%dT%H:%M:%S.%fZ",
                        value_source="key", value_index=2)
        val = self._get_column_values(col)
        self.assertEqual(val[0], datetime(2013, 3, 1, 12, 0))
        self.assertEqual(val[1], datetime(2013, 3, 2, 12, 0))

    def _get_column_values(self, col):
        data = [
            {"key": ["1", "indicator_a", "2013-03-01T12:00:00.000Z"],
             "value": {"sum": 1, "count": 3, "min": 1, "max": 1, "sumsqr": 3}},
            {"key": ["2", "indicator_b", "2013-03-02T12:00:00.000Z"],
             "value": {"sum": 2, "count": 2, "min": 1, "max": 1, "sumsqr": 2}},
        ]

        values = []
        for r in data:
            key = r["key"]
            value = r["value"]
            if col.matches(key):
                values.append(col.get_value(key, value))

        return values