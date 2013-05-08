from django.conf import settings
if not settings.configured:
    settings.configure(DEBUG=True)

import unittest
import sqlalchemy

from datetime import date
from ctable.models import RowMatch
from ctable import CtableExtractor, SqlExtractMapping, ColumnDef
from fakecouch import FakeCouchDb


class TestCouchPull(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.TEST_SQLITE_URL = 'sqlite:///:memory:'
        cls.connection = sqlalchemy.create_engine(cls.TEST_SQLITE_URL).connect()

    def setUp(self):
        self.db = FakeCouchDb()
        self.ctable = CtableExtractor(self.connection, self.db)

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

        extract = SqlExtractMapping(domain="test", name="demo_extract", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", data_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="rename_indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[RowMatch(index=1, value="indicator_a")]),
            ColumnDef(name="indicator_b", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[RowMatch(index=1, value="indicator_b")]),
            ColumnDef(name="indicator_c", data_type="integer", value_source="value", value_attribute="sum",
                      match_keys=[RowMatch(index=1, value="indicator_c")])
        ])

        self.ctable.extract(extract, "c/view")

        result = dict(
            [(row['username'] + "_" + row['date'], row) for row in
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

        extract = SqlExtractMapping(domain="test", name="demo_extract1", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", data_format="%Y-%m-%dT%H:%M:%S.%fZ",
                      value_source="key", value_index=2),
            ColumnDef(name="indicator", data_type="integer", value_source="value",
                      match_keys=[RowMatch(index=1, value="indicator_a")])
        ])

        self.ctable.extract(extract, "c/view")

        result = self.connection.execute('SELECT * FROM %s' % extract.table_name).first()
        self.assertEqual(result['username'], "1")
        self.assertEqual(result['date'], None)
        self.assertEqual(result['indicator'], 1)

    def test_convert_indicator_diff_to_grains_date(self):
        diff = dict(doc_type='MockIndicators',
                    group_values=['mock', '123'],
                    group_names=['domain', 'owner_id'],
                    indicator_changes=[
                        dict(calculator='visits_week',
                             emitter='all_visits',
                             emitter_type='date',
                             values=[date(2012, 2, 24), date(2012, 2, 25)])
                    ])

        grains = list(self.ctable._get_grains(diff))
        self.assertEqual(2, len(grains))
        self.assertEqual(grains[0], ['MockIndicators', 'mock', '123', 'visits_week', 'all_visits', date(2012, 2, 24)])
        self.assertEqual(grains[1], ['MockIndicators', 'mock', '123', 'visits_week', 'all_visits', date(2012, 2, 25)])

    def test_convert_indicator_diff_to_grains_null(self):
        diff = dict(doc_type='MockIndicators',
                    group_values=['123'],
                    group_names=['owner_id'],
                    indicator_changes=[
                        dict(calculator='visits_week',
                             emitter='null_emitter',
                             emitter_type='null',
                             values=[None, None])
                    ])

        grains = list(self.ctable._get_grains(diff))
        self.assertEqual(1, len(grains))
        self.assertEqual(grains[0], ['MockIndicators', '123', 'visits_week', 'null_emitter', None])

    def test_convert_indicator_diff_to_extract_mapping(self):
        diff = dict(doc_type='MockIndicators',
                    group_values=['123'],
                    group_names=['owner_id'],
                    indicator_changes=[
                        dict(calculator='visits_week',
                             emitter='null_emitter',
                             emitter_type='null',
                             values=[None, None]),
                        dict(calculator='visits_week',
                             emitter='all_visits',
                             emitter_type='date',
                             values=[date(2012, 2, 24), date(2012, 2, 25)])
                    ])

        em = self.ctable._get_extract_mapping(diff)

        self.assertEqual(em.table_name, "MockIndicators")
        self.assertEqual(len(em.columns), 4)
        self._compare_columns(em.columns[0], ColumnDef(name='owner_id',
                                                       data_type='string',
                                                       value_source='key',
                                                       value_index=1))
        self._compare_columns(em.columns[1], ColumnDef(name='emitted_value',
                                                       data_type='date',
                                                       value_source='key',
                                                       value_index=4))
        self._compare_columns(em.columns[2], ColumnDef(name='visits_week_null_emitter',
                                                       data_type='integer',
                                                       value_source='value',
                                                       match_keys=[RowMatch(index=2, value='visits_week'),
                                                                   RowMatch(index=3, value='null_emitter')]))
        self._compare_columns(em.columns[3], ColumnDef(name='visits_week_all_visits',
                                                       data_type='integer',
                                                       value_source='value',
                                                       match_keys=[RowMatch(index=2, value='visits_week'),
                                                                   RowMatch(index=3, value='all_visits')]))

    def _compare_columns(self, left, right):
        self.assertEqual(left.name, right.name)
        self.assertEqual(left.data_type, right.data_type)
        self.assertEqual(left.data_format, right.data_format)
        self.assertEqual(left.max_length, right.max_length)
        self.assertEqual(left.value_source, right.value_source)
        self.assertEqual(left.value_index, right.value_index)
        self.assertEqual(left.value_attribute, right.value_attribute)
        left_matches, right_matches = left.match_keys, right.match_keys

        self.assertEqual(len(left_matches), len(right_matches))
        for i in range(len(left_matches)):
            self.assertEqual(left_matches[i].index, right_matches[i].index)
            self.assertEqual(left_matches[i].value, right_matches[i].value)
