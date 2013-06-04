from ctable.writer import SqlTableWriter, ColumnTypeException
from . import TestBase, engine

TABLE = "test_table"


class TestWriter(TestBase):
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
            self.ColumnDef(name="col_a", data_type="string", value_source='key'),
            self.ColumnDef(name="col_e", data_type="string", value_source='key'),
            self.ColumnDef(name="col_f", data_type="string", value_source='key', nullable=False),
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
        self.assertIn('id', table_columns)
        self.assertIn('col_a', table_columns)
        self.assertIn('col_b', table_columns)
        self.assertIn('col_c', table_columns)
        self.assertIn('col_d', table_columns)

        indexes = self.writer.table(TABLE).indexes
        self.assertEqual(len(indexes), 8)
        self._assertIndex(indexes, 'test_table_col_a_col_e_col_f_col_b_key', 4, 'col_a', 'col_b', 'col_e', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_a_null', 3, 'col_b', 'col_e', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_b_null', 3, 'col_a', 'col_e', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_e_null', 3, 'col_b', 'col_a', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_e_col_b_null', 2, 'col_a', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_b_null', 2, 'col_e', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_e_null', 2, 'col_b', 'col_f')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_e_col_b_null', 1, 'col_f')

    def test_init_table_indexes(self):
        columns = [
            self.ColumnDef(name="col_a", data_type="string", value_source='key'),
            self.ColumnDef(name="col_b", data_type="date", value_source='key'),
            self.ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
        ]
        with self.writer:
            self.writer.init_table(TABLE, columns)

        table = self.writer.table(TABLE)
        indexes = table.indexes
        self.assertEqual(len(indexes), 3)
        self._assertIndex(indexes, 'test_table_col_a_col_b_key', 2, 'col_a', 'col_b')
        self._assertIndex(indexes, 'test_table_unique_col_a_null', 1, 'col_b')
        self._assertIndex(indexes, 'test_table_unique_col_b_null', 1, 'col_a')

    def test_init_table_indexes_lots(self):
        columns = [
            self.ColumnDef(name="col_a", data_type="string", value_source='key'),
            self.ColumnDef(name="col_b", data_type="string", value_source='key'),
            self.ColumnDef(name="col_c", data_type="string", value_source='key', nullable=False),
            self.ColumnDef(name="col_d", data_type="date", value_source='key'),
            self.ColumnDef(name="col_e", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
        ]
        with self.writer:
            self.writer.init_table(TABLE, columns)

        indexes = self.writer.table(TABLE).indexes
        self.assertEqual(len(indexes), 8)
        self._assertIndex(indexes, 'test_table_col_a_col_b_col_c_col_d_key', 4, 'col_a', 'col_b', 'col_c', 'col_d')
        self._assertIndex(indexes, 'test_table_unique_col_a_null', 3, 'col_b', 'col_b', 'col_c')
        self._assertIndex(indexes, 'test_table_unique_col_b_null', 3, 'col_a', 'col_d', 'col_c')
        self._assertIndex(indexes, 'test_table_unique_col_d_null', 3, 'col_b', 'col_a', 'col_c')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_b_null', 2, 'col_c', 'col_d')
        self._assertIndex(indexes, 'test_table_unique_col_b_col_d_null', 2, 'col_c', 'col_a')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_d_null', 2, 'col_c', 'col_b')
        self._assertIndex(indexes, 'test_table_unique_col_a_col_b_col_d_null', 1, 'col_c')

    def test_init_table_indexes_non_nullable_key_column(self):
        columns = [
            self.ColumnDef(name="col_a", data_type="string", value_source='key', nullable=False),
            self.ColumnDef(name="col_b", data_type="date", value_source='key'),
            self.ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[self.KeyMatcher(index=1, value="c")]),
        ]
        with self.writer:
            self.writer.init_table(TABLE, columns)

        table = self.writer.table(TABLE)
        indexes = table.indexes
        self.assertEqual(len(indexes), 2)
        self._assertIndex(indexes, 'test_table_col_a_col_b_key', 2, 'col_a', 'col_b')
        self._assertIndex(indexes, 'test_table_unique_col_b_null', 1, 'col_a')

    def _assertIndex(self, indexes, name, length, *columns):
        index = filter(lambda x: x.name == name, indexes)
        self.assertTrue(len(index), 1)
        self.assertEquals(len(index[0].expressions), length)
        col_names = [c.name for c in index[0].expressions]
        for c in columns:
            self.assertIn(c, col_names)

    def test_update_table(self):
        self.test_init_table()
        self.test_init_table()

    def test_update_table_fail(self):
        self.test_init_table()

        columns = [
            self.ColumnDef(name="col_b", data_type="datetime", value_source='key'),
        ]

        with self.assertRaises(ColumnTypeException):
            with self.writer:
                self.writer.make_table_compatible(TABLE, columns)