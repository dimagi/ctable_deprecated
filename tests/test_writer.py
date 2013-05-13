from django.conf import settings
if not settings.configured:
    settings.configure(DEBUG=True)

from unittest2 import TestCase
import sqlalchemy

from ctable import ColumnDef
from ctable.writer import SqlTableWriter, ColumnTypeException

import logging

logging.basicConfig()

TEST_DB_URL = 'postgresql://postgres:@localhost/ctable_test'
engine = sqlalchemy.create_engine(TEST_DB_URL)

TABLE = "test_table"


class TestWriter(TestCase):
    def setUp(self):
        self.connection = engine.connect()
        self.trans = self.connection.begin()
        self.writer = SqlTableWriter(self.connection)

    def tearDown(self):
        super(TestWriter, self).tearDown()
        self.trans.rollback()
        self.trans = self.connection.begin()
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % TABLE)
        self.trans.commit()
        self.connection.close()

    def test_init_table(self):
        columns = [
            ColumnDef(name="col_a", data_type="string", value_source='key'),
            ColumnDef(name="col_b", data_type="date", value_source='key'),
            ColumnDef(name="col_c", data_type="integer", value_source='value'),
            ColumnDef(name="col_d", data_type="datetime", value_source='value'),
        ]
        with self.writer:
            self.writer.init_table(TABLE, columns)

        self.assertIn(TABLE, self.writer.metadata.tables)
        table_columns = self.writer.table(TABLE).columns
        self.assertIn('col_a', table_columns)
        self.assertIn('col_b', table_columns)
        self.assertIn('col_c', table_columns)
        self.assertIn('col_d', table_columns)

    def test_update_table(self):
        self.test_init_table()
        self.test_init_table()

    def test_update_table_fail(self):
        self.test_init_table()

        columns = [
            ColumnDef(name="col_b", data_type="datetime", value_source='key'),
        ]

        with self.assertRaises(ColumnTypeException):
            with self.writer:
                self.writer.make_table_compatible(TABLE, columns)