from django.conf import settings
settings.configure(DEBUG=True)

import unittest
import sqlalchemy

from ctable import CtableExtractor, SqlExtract, ColumnDef
from mock_couch import MockCouch


class TestCouchPull(unittest.TestCase):
    def setUp(cls):
        cls.TEST_SQLITE_URL = 'sqlite:///:memory:'
        cls.TEST_POSTGRESQL_URL = "postgresql://postgres:password@localhost/care_benin"
        cls.connection = sqlalchemy.create_engine(cls.TEST_SQLITE_URL).connect()

    def test_basic(self):
        db = MockCouch([
            {"key": ["1", "indicator_a", "2013-03-01T12:00:00.000Z"],
             "value": {"sum": 1, "count": 3, "min": 1, "max": 1, "sumsqr": 3}},
            {"key": ["1", "indicator_b", "2013-03-01T12:00:00.000Z"],
             "value": {"sum": 2, "count": 2, "min": 1, "max": 1, "sumsqr": 2}},
            {"key": ["2", "indicator_a", "2013-03-01T12:00:00.000Z"],
             "value": {"sum": 3, "count": 3, "min": 1, "max": 1, "sumsqr": 3}},
        ]
        )

        extract = SqlExtract(columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", data_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="rename_indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                      match_key={"index": 1, "value": "indicator_a"}),
            ColumnDef(name="indicator_b", data_type="integer", value_source="value", value_attribute="sum",
                      match_key={"index": 1}),
            ColumnDef(name="indicator_c", data_type="integer", value_source="value", value_attribute="sum",
                      match_key={"index": 1})
        ])

        table_name = "test_extract"
        cpull = CtableExtractor(self.connection, db, extract)
        cpull.pull(table_name, "c/view")

        result = dict(
            [(row['username'] + "_" + row['date'], row) for row in
             self.connection.execute('SELECT * FROM %s' % table_name)])
        self.assertEqual(result['1_2013-03-01']['rename_indicator_a'], 1)
        self.assertEqual(result['1_2013-03-01']['indicator_b'], 2)
        self.assertEqual(result['2_2013-03-01']['rename_indicator_a'], 3)
        self.assertIsNone(result['2_2013-03-01']['indicator_b'])
