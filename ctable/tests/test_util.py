from ctable import util
from ctable.models import SqlExtractMapping, ColumnDef, KeyMatcher
from ctable.tests import TestBase


class TestUtil(TestBase):

    def test_combine_rows(self):
        mapping = self.get_mapping()

        rows = [dict(username="u1", date="d1", indicator_a="1"),
                dict(username="u1", date="d1", indicator_a="2"),
                dict(username="u2", date="d1", indicator_a="1"),
                dict(username="u2", date="d1", indicator_b="1")]
        munged = util.combine_rows(rows, mapping)

        expected = [dict(username="u1", date="d1", indicator_a="2"),
                    dict(username="u2", date="d1", indicator_a="1", indicator_b="1")]
        self.assertEqual(list(munged), expected)

    def test_combine_rows_chunked(self):
        mapping = self.get_mapping()

        rows = [dict(username="u1", date="d1", indicator_a="1"),
                dict(username="u1", date="d1", indicator_b="1"),
                dict(username="u1", date="d1", indicator_a="2"),
                dict(username="u1", date="d1", indicator_b="2")]
        munged = util.combine_rows(rows, mapping, chunksize=2)

        expected = [dict(username="u1", date="d1", indicator_a="1", indicator_b="1"),
                    dict(username="u1", date="d1", indicator_a="2", indicator_b="2")]
        self.assertEqual(list(munged), expected)

    def test_combine_rows_chunked(self):
        mapping = self.get_mapping()

        rows = [dict(username="u1", date="d1", indicator_a="1"),
                dict(username="u1", date="d1", indicator_b="1"),
                dict(username="u1", date="d1", indicator_a="2"),
                dict(username="u1", date="d1", indicator_b="2")]
        munged = util.combine_rows(rows, mapping, chunksize=2)

        expected = [dict(username="u1", date="d1", indicator_a="1", indicator_b="1"),
                    dict(username="u1", date="d1", indicator_a="2", indicator_b="2")]
        self.assertEqual(list(munged), expected)

    def get_mapping(self):
        return SqlExtractMapping(domains=['test'], name='test', couch_view="c/view", columns=[
            ColumnDef(name="username", data_type="string", max_length=50, value_source="key", value_index=0),
            ColumnDef(name="date", data_type="date", date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                           value_source="key", value_index=2),
            ColumnDef(name="indicator_a", data_type="integer", value_source="value", value_attribute="sum",
                           match_keys=[KeyMatcher(index=1, value="indicator_a")]),
            ColumnDef(name="indicator_b", data_type="integer", value_source="value", value_attribute="sum",
                           match_keys=[KeyMatcher(index=1, value="indicator_b")]),
            ColumnDef(name="indicator_c", data_type="integer", value_source="value", value_attribute="sum",
                           match_keys=[KeyMatcher(index=1, value="indicator_c")])
        ])
