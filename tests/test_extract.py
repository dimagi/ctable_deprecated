TEST_DB_URL = 'postgresql://postgres:@localhost/ctable_test'

from couchdbkit.schema import DocumentBase, DocumentSchema
from fakecouch import FakeCouchDb

DocumentBase._db = FakeCouchDb()
DocumentSchema._db = FakeCouchDb()

from django.conf import settings
if not settings.configured:
    settings.configure(DEBUG=True, SQL_REPORTING_DATABASE_URL=TEST_DB_URL)

from unittest2 import TestCase
import sqlalchemy

from datetime import date, datetime, timedelta
from ctable.models import KeyMatcher
from ctable import CtableExtractor, SqlExtractMapping, ColumnDef
from ctable.base import fluff_view
from couchdbkit.ext.django.loading import couchdbkit_handler

import logging

logging.basicConfig()

engine = sqlalchemy.create_engine(TEST_DB_URL)

DOMAIN = "test"
MAPPING_NAME = "demo_extract"
TABLE = "%s_%s" %(DOMAIN, MAPPING_NAME)


class TestCTable(TestCase):
    def setUp(self):
        self.connection = engine.connect()
        self.trans = self.connection.begin()
        self.db = FakeCouchDb()
        couchdbkit_handler._databases = {'fluff': self.db}
        self.ctable = CtableExtractor(self.connection, self.db)

    def tearDown(self):
        super(TestCTable, self).tearDown()
        self.trans.rollback()
        self.trans = self.connection.begin()
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % TABLE)
        self.connection.execute('DROP TABLE IF EXISTS "%s"' % self._get_fluff_diff()['doc_type'])
        self.trans.commit()
        self.connection.close()

    def test_basic(self):
        self.db.add_view('c/view', [
            (
                {'reduce': True, 'group': True, 'startkey': [], 'endkey': [{}]},
                [
                    {"key": ["1", "indicator_a", "2013-03-01T12:00:00.000Z"],
                     "value": {"sum": 1, "count": 3, "min": 1, "max": 1, "sumsqr": 3}},
                    {"key": ["1", "indicator_b", "2013-03-01T12:00:00.000Z"],
                     "value": {"sum": 2, "count": 2, "min": 1, "max": 1, "sumsqr": 2}},
                    {"key": ["2", "indicator_a", "2013-03-01T12:00:00.000Z"],
                     "value": {"sum": 3, "count": 3, "min": 1, "max": 1, "sumsqr": 3}},
                ]
            )
        ])

        extract = SqlExtractMapping(domain=DOMAIN, name=MAPPING_NAME, couch_view="c/view", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="rename_indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[KeyMatcher(index=1, value="indicator_a")]),
            ColumnDef(name="indicator_b", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[KeyMatcher(index=1, value="indicator_b")]),
            ColumnDef(name="indicator_c", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[KeyMatcher(index=1, value="indicator_c")])
        ])

        self.ctable.extract(extract)

        result = dict(
            [(row.username + "_" + row.date.strftime("%Y-%m-%d"), row) for row in
             self.connection.execute('SELECT * FROM %s' % extract.table_name)])
        self.assertEqual(result['1_2013-03-01']['rename_indicator_a'], 1)
        self.assertEqual(result['1_2013-03-01']['indicator_b'], 2)
        self.assertEqual(result['2_2013-03-01']['rename_indicator_a'], 3)
        self.assertIsNone(result['2_2013-03-01']['indicator_b'])

    def test_null_column(self):
        self.db.add_view('c/view', [
            (
                {'reduce': True, 'group': True, 'startkey': [], 'endkey': [{}]},
                [
                    {"key": ["1", "indicator_a", None], "value": 1},
                ]
            )
        ])

        extract = SqlExtractMapping(domain=DOMAIN, name=MAPPING_NAME, couch_view="c/view", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="indicator", data_type="integer", value_source="value",
                      match_keys=[KeyMatcher(index=1, value="indicator_a")])
        ])

        self.ctable.extract(extract)

        result = self.connection.execute('SELECT * FROM %s' % extract.table_name).first()
        self.assertEqual(result['username'], "1")
        self.assertEqual(result['date'], None)
        self.assertEqual(result['indicator'], 1)

    def test_empty_view_result(self):
        extract = SqlExtractMapping(domain=DOMAIN, name=MAPPING_NAME, couch_view="c/view", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0)
        ])

        self.ctable.extract(extract)

        metadata = sqlalchemy.MetaData(bind=engine)
        metadata.reflect()
        self.assertNotIn(extract.table_name, metadata.tables)

    def test_couch_rows_to_sql(self):
        extract = SqlExtractMapping(domain=DOMAIN, name=MAPPING_NAME, couch_view="c/view", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="indicator", data_type="integer", value_source="value",
                      match_keys=[KeyMatcher(index=1, value="indicator_a")])
        ])
        rows = [
            dict(key=['user1', 'indicator_a', '2012-02-15T00:00:00.000Z'], value=1),
            dict(key=['user2', 'indicator_a', '2012-02-15T00:00:00.000Z'], value=2),
            dict(key=['user1', 'indicator_b', '2012-02-15T00:00:00.000Z'], value=1),
        ]
        sql_rows = list(self.ctable.couch_rows_to_sql_rows(rows, extract))
        self.assertEqual(len(sql_rows), 2)
        self.assertEqual(sql_rows[0], dict(username='user1', date=date(2012, 02, 15), indicator=1))
        self.assertEqual(sql_rows[1], dict(username='user2', date=date(2012, 02, 15), indicator=2))

    def test_convert_indicator_diff_to_grains_date(self):
        diff = self._get_fluff_diff(['all_visits'],
                                    group_values=['mock', '123'],
                                    group_names=['domain', 'owner_id'])

        grains = list(self.ctable.get_fluff_grains(diff))
        self.assertEqual(2, len(grains))
        key_prefix = ['MockIndicators', 'mock', '123', 'visits_week', 'all_visits']
        self.assertEqual(grains[0], key_prefix + ["2012-02-24T00:00:00Z"])
        self.assertEqual(grains[1], key_prefix + ["2012-02-25T00:00:00Z"])
        self.assertEqual(grains[1], key_prefix + ["2012-02-25T00:00:00Z"])

    def test_convert_indicator_diff_to_grains_null(self):
        diff = self._get_fluff_diff(['null_emitter'])

        grains = list(self.ctable.get_fluff_grains(diff))
        self.assertEqual(1, len(grains))
        self.assertEqual(grains[0], ['MockIndicators', '123', 'visits_week', 'null_emitter', None])

    def test_convert_indicator_diff_to_extract_mapping(self):
        diff = self._get_fluff_diff()

        em = self.ctable.get_extract_mapping(diff)

        self.assertEqual(em.table_name, "MockIndicators")
        self.assertEqual(len(em.columns), 4)
        self.assertColumnsEqual(em.columns[0], ColumnDef(name='owner_id',
                                                         data_type='string',
                                                         value_source='key',
                                                         value_index=1))
        self.assertColumnsEqual(em.columns[1], ColumnDef(name='emitter_value',
                                                         data_type='date',
                                                         value_source='key',
                                                         value_index=4))
        self.assertColumnsEqual(em.columns[2], ColumnDef(name='visits_week_null_emitter',
                                                         data_type='integer',
                                                         value_source='value',
                                                         match_keys=[KeyMatcher(index=2, value='visits_week'),
                                                                     KeyMatcher(index=3, value='null_emitter')]))
        self.assertColumnsEqual(em.columns[3], ColumnDef(name='visits_week_all_visits',
                                                         data_type='integer',
                                                         value_source='value',
                                                         match_keys=[KeyMatcher(index=2, value='visits_week'),
                                                                     KeyMatcher(index=3, value='all_visits')]))

    def test_get_rows_for_grains(self):
        r1 = {"key": ['a', 'b', None], "value": 3}
        r2 = {"key": ['a', 'b', '2013-01-03'], "value": 2}
        self.db.add_view(fluff_view, [
            (
                {'reduce': True, 'group': True, 'startkey': r1['key'], 'endkey': r1['key'] + [{}]},
                [r1]
            ),
            (
                {'reduce': True, 'group': True, 'startkey': r2['key'], 'endkey': r2['key'] + [{}]},
                [r2]
            )
        ])

        grains = [
            ['a', 'b', None],
            ['a', 'b', '2013-01-03'],
        ]
        rows = self.ctable.recalculate_grains(grains, 'fluff')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], r1)
        self.assertEqual(rows[1], r2)

    def test_extract_fluff_diff(self):
        rows = [{"key": ['MockIndicators', '123', 'visits_week', 'null_emitter', None], "value": 3},
                {"key": ['MockIndicators', '123', 'visits_week', 'all_visits', '2012-02-24T00:00:00Z'], "value": 2},
                {"key": ['MockIndicators', '123', 'visits_week', 'all_visits', '2012-02-25T00:00:00Z'], "value": 7}]

        self.db.add_view(fluff_view, [({'reduce': True, 'group': True, 'startkey': r['key'], 'endkey': r['key'] + [{}]},
                                       [r]) for r in rows])

        diff = self._get_fluff_diff()
        self.ctable.process_fluff_diff(diff)
        result = dict(
            [('%s_%s' % (row.owner_id, row.emitter_value), row) for row in
             self.connection.execute('SELECT * FROM "%s"' % diff['doc_type'])])
        self.assertEqual(len(result), 3)
        self.assertEqual(result['123_None']['visits_week_null_emitter'], 3)
        self.assertEqual(result['123_2012-02-24']['visits_week_all_visits'], 2)
        self.assertEqual(result['123_2012-02-25']['visits_week_all_visits'], 7)

    def test_get_couch_keys(self):
        mapping = SqlExtractMapping(couch_key_prefix=['a'])
        startkey, endkey = self.ctable.get_couch_keys(mapping)

        self.assertEqual(startkey, ['a'])
        self.assertEqual(endkey, ['a', {}])

    def test_get_couch_keys_with_dates(self):
        format = '%Y-%m-%d'
        range = 10
        mapping = SqlExtractMapping(couch_key_prefix=['a'], couch_date_range=range, couch_date_format=format)
        startkey, endkey = self.ctable.get_couch_keys(mapping)

        end = datetime.utcnow()
        start = end - timedelta(days=range)
        self.assertEqual(startkey, ['a', start.strftime(format)])
        self.assertEqual(endkey, ['a', end.strftime(format), {}])

    def _get_fluff_diff(self, emitters=['all_visits', 'null_emitter'],
                        group_values=['123'],
                        group_names=['owner_id']):
        diff = dict(database='fluff',
                    doc_type='MockIndicators',
                    group_values=group_values,
                    group_names=group_names)
        indicator_changes = []
        if 'null_emitter' in emitters:
            indicator_changes.append(dict(calculator='visits_week',
                                          emitter='null_emitter',
                                          emitter_type='null',
                                          values=[None, None]))

        if 'all_visits' in emitters:
            indicator_changes.append(dict(calculator='visits_week',
                                          emitter='all_visits',
                                          emitter_type='date',
                                          values=[date(2012, 2, 24), date(2012, 2, 25)]))

        diff['indicator_changes'] = indicator_changes
        return diff

    def assertColumnsEqual(self, left, right):
        self.assertEqual(left.name, right.name)
        self.assertEqual(left.data_type, right.data_type)
        self.assertEqual(left.date_format, right.date_format)
        self.assertEqual(left.max_length, right.max_length)
        self.assertEqual(left.value_source, right.value_source)
        self.assertEqual(left.value_index, right.value_index)
        self.assertEqual(left.value_attribute, right.value_attribute)
        left_matches, right_matches = left.match_keys, right.match_keys

        self.assertEqual(len(left_matches), len(right_matches))
        for i in range(len(left_matches)):
            self.assertEqual(left_matches[i].index, right_matches[i].index)
            self.assertEqual(left_matches[i].value, right_matches[i].value)
