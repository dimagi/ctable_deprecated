from couchdbkit import BadValueError
from datetime import date, datetime
from ctable.tests import TestBase
from ctable.models import SqlExtractMapping, KeyMatcher, ColumnDef, NOT_EQUAL


class TestModels(TestBase):

    def test_sql_extract_empty_name(self):
        with self.assertRaises(BadValueError):
            SqlExtractMapping(name="")

    def test_sql_extract_illegal_chars(self):
        with self.assertRaises(BadValueError):
            SqlExtractMapping(name="name_with_bang!")

    def test_sql_extract_table_name(self):
        e = SqlExtractMapping(name="demo_name", domains=["test"])
        self.assertEqual("test_demo_name", e.table_name)

    def test_column_validate_key_index(self):
        col = ColumnDef(name="a", data_type="string", value_source="key", value_index=1)
        col.validate()

        col = ColumnDef(name="a", data_type="string", value_source="key")
        with self.assertRaises(BadValueError):
            col.validate()

    def test_column_match_all(self):
        col = ColumnDef(name="user", data_type="string", max_length=50, value_source="key", value_index=0)
        val = self._get_column_values(col)
        self.assertEqual(val, ["1", "2", 3])

    def test_column_match(self):
        col = ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=1, value="indicator_a")])
        val = self._get_column_values(col)
        self.assertEqual(val, [1])

    def test_column_match_neq(self):
        col = ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=1, value="indicator_a", operator=NOT_EQUAL)])
        val = self._get_column_values(col)
        self.assertEqual(val, [2, 5])

    def test_column_match_json(self):
        col = ColumnDef(name="other", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=1, value="{}")])
        val = self._get_column_values(col)
        self.assertEqual(val, [5])

        col = ColumnDef(name="other", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=0, value="3")])
        val = self._get_column_values(col)
        self.assertEqual(val, [5])

    def test_column_match_multi_pass(self):
        col = ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=1, value="indicator_a"),
                                    KeyMatcher(index=0, value="1")])
        val = self._get_column_values(col)
        self.assertEqual(val, [1])

    def test_column_match_multi_fail(self):
        col = ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                        match_keys=[KeyMatcher(index=1, value="indicator_a"),
                                    KeyMatcher(index=0, value="2")])
        val = self._get_column_values(col)
        self.assertEqual(val, [])

    def test_column_date(self):
        col = ColumnDef(name="date", data_type="date", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                        value_source="key", value_index=2)
        val = self._get_column_values(col)
        self.assertEqual(val[0], date(2013, 3, 1))
        self.assertEqual(val[1], date(2013, 3, 2))

    def test_column_datetime(self):
        col = ColumnDef(name="date", data_type="datetime", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
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
            {"key": [3, {}, "2013-03-02T12:00:00.000Z"],
             "value": {"sum": 5, "count": 5, "min": 1, "max": 1, "sumsqr": 5}},
        ]

        values = []
        for r in data:
            key = r["key"]
            value = r["value"]
            if col.matches(key, value):
                values.append(col.get_value(key, value))

        return values
