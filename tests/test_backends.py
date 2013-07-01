from ctable.backends import SqlBackend, ColumnTypeException
from . import TestBase, engine

TABLE = "test_table"


class TestBackends(TestBase):
    def setUp(self):
        self.connection = engine.connect()
        self.trans = self.connection.begin()
        self.writer = SqlBackend(self.connection)

    def tearDown(self):
        super(TestBackends, self).tearDown()
        self.trans.rollback()
        self.trans = self.connection.begin()
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % TABLE)
        self.trans.commit()
        self.connection.close()

    def test_init_table(self):
        columns = [
            self.ColumnDef(name="col_a", data_type="string", value_source='key'),
            self.ColumnDef(name="col_b", data_type="date", value_source='key'),
            self.ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
            self.ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="d")]),
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

    def test_update_table(self):
        self.test_init_table()
        columns = [
            self.ColumnDef(name="col_a", data_type="string", value_source='key'),
            self.ColumnDef(name="col_b", data_type="date", value_source='key'),
            self.ColumnDef(name="col_e", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="e")]),
        ]
        with self.writer:
            self.writer.init_table(TABLE, columns)

        self.assertIn(TABLE, self.writer.metadata.tables)
        table_columns = self.writer.table(TABLE).columns
        self.assertIn('col_a', table_columns)
        self.assertIn('col_b', table_columns)
        self.assertIn('col_c', table_columns)
        self.assertIn('col_d', table_columns)
        self.assertIn('col_e', table_columns)

    def test_update_table_fail(self):
        self.test_init_table()

        columns = [
            self.ColumnDef(name="col_b", data_type="datetime", value_source='key'),
        ]

        with self.assertRaises(ColumnTypeException):
            with self.writer:
                self.writer.make_table_compatible(TABLE, columns)

    def test_check_table_missing_key_column_in_table(self):
        self.test_init_table()

        extract = self.SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            self.ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            self.ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
            self.ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="d")]),
        ])

        with self.writer:
            messages = self.writer.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Key column exists in table but not in mapping: col_b'])

    def test_check_table_missing_column_in_table(self):
        self.test_init_table()

        extract = self.SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            self.ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            self.ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            self.ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="d")]),
        ])

        with self.writer:
            messages = self.writer.check_mapping(extract)

        self.assertEqual(len(messages['errors']), 0)
        self.assertEqual(messages['warnings'], ['Column exists in table but not in mapping: col_c'])

    def test_check_table_missing_key_column_in_mapping(self):
        self.test_init_table()

        extract = self.SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            self.ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            self.ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            self.ColumnDef(name="col_e", data_type="date", value_source='key', value_index=2),
            self.ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
            self.ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="d")]),
        ])

        with self.writer:
            messages = self.writer.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Key column exists in mapping but not in table: col_e'])

    def test_check_table_mismatch_data_type(self):
        self.test_init_table()

        extract = self.SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            self.ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            self.ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            self.ColumnDef(name="col_c", data_type="string", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
            self.ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="d")]),
        ])

        with self.writer:
            messages = self.writer.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Column types do not match: col_c'])