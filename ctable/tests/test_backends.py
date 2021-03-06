import datetime
import sqlalchemy
from sqlalchemy.exc import ProgrammingError
from ctable.backends import SqlBackend, ColumnTypeException
from ctable.tests import TestBase
from django.conf import settings
from ctable.models import ColumnDef, KeyMatcher, SqlExtractMapping

TABLE = "{0}_test_table".format(settings.CTABLE_PREFIX)


class BackendBase(TestBase):
    def setUp(self):
        super(BackendBase, self).setUp()
        self.connection = self.engine.connect()
        self.trans = self.connection.begin()

    def tearDown(self):
        super(BackendBase, self).tearDown()
        self.trans.rollback()

    @property
    def metadata(self):
        metadata = sqlalchemy.MetaData()
        metadata.bind = self.connection
        metadata.reflect()
        return metadata

    def table(self, table_name):
        return sqlalchemy.Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)


class TestBackends(BackendBase):
    def setUp(self):
        super(TestBackends, self).setUp()
        self.backend = SqlBackend(self.connection)

    def tearDown(self):
        super(TestBackends, self).tearDown()
        self.trans = self.connection.begin()
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % TABLE)
        self.trans.commit()
        self.connection.close()



    def test_init_table(self):
        columns = [
            ColumnDef(name="col_a", data_type="string", value_source='key'),
            ColumnDef(name="col_b", data_type="date", value_source='key'),
            ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ]
        with self.backend:
            self.backend.init_table(TABLE, columns)

            self.assertIn(TABLE, self.metadata.tables)
            table_columns = self.table(TABLE).columns

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
            ColumnDef(name="col_a", data_type="string", value_source='key'),
            ColumnDef(name="col_b", data_type="date", value_source='key'),
            ColumnDef(name="col_e", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="e")]),
        ]
        with self.backend:
            self.backend.init_table(TABLE, columns)

            self.assertIn(TABLE, self.metadata.tables)
            table_columns = self.table(TABLE).columns

        self.assertIn('col_a', table_columns)
        self.assertIn('col_b', table_columns)
        self.assertIn('col_c', table_columns)
        self.assertIn('col_d', table_columns)
        self.assertIn('col_e', table_columns)

    def test_update_table_fail(self):
        self.test_init_table()

        columns = [
            ColumnDef(name="col_b", data_type="datetime", value_source='key'),
        ]

        with self.assertRaises(ColumnTypeException):
            with self.backend:
                self.backend.make_table_compatible(TABLE, columns)

    def test_check_table_missing_key_column_in_table(self):
        self.test_init_table()

        extract = SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ])

        with self.backend:
            messages = self.backend.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Key column exists in table but not in mapping: col_b'])

    def test_check_table_missing_column_in_table(self):
        self.test_init_table()

        extract = SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ])

        with self.backend:
            messages = self.backend.check_mapping(extract)

        self.assertEqual(len(messages['errors']), 0)
        self.assertEqual(messages['warnings'], ['Column exists in table but not in mapping: col_c'])

    def test_check_table_missing_key_column_in_mapping(self):
        self.test_init_table()

        extract = SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            ColumnDef(name="col_e", data_type="date", value_source='key', value_index=2),
            ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ])

        with self.backend:
            messages = self.backend.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Key column exists in mapping but not in table: col_e'])

    def test_check_table_mismatch_data_type(self):
        self.test_init_table()

        extract = SqlExtractMapping(domains=['test'], name='table', couch_view="c/view", columns=[
            ColumnDef(name="col_a", data_type="string", value_source='key', value_index=1),
            ColumnDef(name="col_b", data_type="date", value_source='key', value_index=2),
            ColumnDef(name="col_c", data_type="string", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ])

        with self.backend:
            messages = self.backend.check_mapping(extract)

        self.assertEqual(len(messages['warnings']), 0)
        self.assertEqual(messages['errors'], ['Column types do not match: col_c'])

    def test_update_only_key_columns(self):
        extract = SqlExtractMapping(domains=['test'], name='table', couch_view="c/view")
        extract.columns = [
            ColumnDef(name="date", data_type="date", value_source="key", value_index=0,
                      date_format="%Y-%m-%dT%H:%M:%SZ"),
            ColumnDef(name="user_id", data_type="string", value_source="key", value_index=1),
            ColumnDef(name="case_type", data_type="string", value_source="key", value_index=2),
            ColumnDef(name="case_updates", data_type="integer", value_source="value"),
        ]

        rows = [{'user_id': '250', 'case_updates': 1, 'case_type': 'b', 'date': datetime.date(2013, 8, 2)},
                {'user_id': 'ab5', 'case_updates': 1, 'case_type': 'c', 'date': datetime.date(2013, 8, 2)},
                {'user_id': '3ce', 'case_updates': 1, 'case_type': 'a', 'date': datetime.date(2013, 8, 2)}]

        with self.backend:
            self.backend.write_rows(rows, extract)
            self.backend.write_rows(rows, extract)


class TestBackendsMultiUser(BackendBase):
    def setUp(self):
        super(TestBackendsMultiUser, self).setUp()
        self.connection.execute("CREATE ROLE test1 LOGIN PASSWORD '123'")
        self.connection.execute("CREATE ROLE test2 LOGIN PASSWORD '345'")
        self.connection.execute("CREATE ROLE testgroup")
        self.connection.execute("GRANT testgroup TO test1, test2")
        self.trans.commit()

        self.conn_t1 = sqlalchemy.create_engine('postgresql://test1:123@localhost/ctable_test').connect()
        self.conn_t2 = sqlalchemy.create_engine('postgresql://test2:345@localhost/ctable_test').connect()
        self. backend1 = SqlBackend(self.conn_t1)
        self. backend2 = SqlBackend(self.conn_t2)

        self.trans = self.connection.begin()

    def tearDown(self):
        super(TestBackendsMultiUser, self).tearDown()
        self.trans = self.connection.begin()
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % TABLE)
        self.connection.execute('DROP ROLE test1')
        self.connection.execute('DROP ROLE test2')
        self.connection.execute('DROP ROLE testgroup')
        self.trans.commit()
        self.connection.close()

        settings.SQL_REPORTING_OBJECT_OWNER = None

    def test_multi_user_fail(self):
        columns = [
            ColumnDef(name="col_a", data_type="string", value_source='key'),
            ColumnDef(name="col_b", data_type="date", value_source='key'),
            ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ]
        with self.backend1:
            self.backend1.init_table(TABLE, columns)

            self.assertIn(TABLE, self.metadata.tables)

        with self.assertRaises(ProgrammingError):
            self.conn_t2.execute("select * from %s" % TABLE)

    def test_multi_user_group(self):
        class Mapping(object):
            table_name = TABLE

        settings.SQL_REPORTING_OBJECT_OWNER = 'testgroup'

        columns = [
            ColumnDef(name="col_a", data_type="string", value_source='key'),
            ColumnDef(name="col_b", data_type="date", value_source='key'),
            ColumnDef(name="col_c", data_type="integer", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="c")]),
            ColumnDef(name="col_d", data_type="datetime", value_source='value',
                           match_keys=[KeyMatcher(index=1, value="d")]),
        ]
        with self.backend1:
            self.backend1.init_table(TABLE, columns)

            self.assertIn(TABLE, self.metadata.tables)

        self.conn_t2.execute("select * from %s" % TABLE)

        with self.backend2:
            self.backend2.clear_all_data(Mapping)

            self.assertNotIn(TABLE, self.metadata.tables)
