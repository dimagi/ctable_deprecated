from .models import SqlExtractMapping, ColumnDef, KeyMatcher
from .writer import SqlTableWriter
from couchdbkit.ext.django.loading import get_db
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("ctable")

fluff_view = 'fluff/generic'


class CtableExtractor(object):
    def __init__(self, sql_connection_or_url, couch_db):
        self.db = couch_db
        self.sql_connection_or_url = sql_connection_or_url
        self.writer = SqlTableWriter(self.sql_connection_or_url)

    def extract(self, extract_mapping):
        """
        Extract data from a CouchDb view into SQL
        """
        startkey, endkey = self.get_couch_keys(extract_mapping)

        result = self.get_couch_rows(extract_mapping.couch_view, startkey, endkey)

        total_rows = result.total_rows
        if total_rows > 0:
            logger.info("Total rows: %d", total_rows)
            rows = self.couch_rows_to_sql_rows(result, extract_mapping)
            self.write_rows_to_sql(rows, extract_mapping)

    def process_fluff_diff(self, diff):
        """
        Given a Fluff diff, update the data in SQL to reflect the changes. This will
        query CouchDB in order to re-calculate all the grains that have changed.
        """
        mapping = self.get_extract_mapping(diff)
        grains = self.get_fluff_grains(diff)
        couch_rows = self.recalculate_grains(grains, diff['database'])
        sql_rows = self.couch_rows_to_sql_rows(couch_rows, mapping)
        self.write_rows_to_sql(sql_rows, mapping)

    def get_couch_keys(self, extract_mapping):
        startkey = extract_mapping.couch_key_prefix
        endkey = extract_mapping.couch_key_prefix
        date_format = extract_mapping.couch_date_format
        date_range = extract_mapping.couch_date_range
        if date_range > 0 and date_format:
            end = datetime.utcnow()
            endkey += [end.strftime(date_format)]
            start = end - timedelta(days=date_range)
            startkey += [start.strftime(date_format)]
        endkey += [{}]
        return startkey, endkey

    def get_couch_rows(self, couch_view, startkey, endkey, db=None, **kwargs):
        db = db or self.db
        result = db.view(
            couch_view,
            reduce=True,
            group=True,
            startkey=startkey,
            endkey=endkey,
            **kwargs)
        return result

    def write_rows_to_sql(self, rows, extract_mapping):
        with self.writer:
            self.writer.write_table(rows, extract_mapping)

    def couch_rows_to_sql_rows(self, couch_rows, extract_mapping):
        """
        Convert the list of rows from CouchDB into rows for insertion into SQL.
        """
        for crow in couch_rows:
            sql_row = {}
            row_has_value = False
            for mc in extract_mapping.columns:
                if mc.matches(crow['key'], crow['value']):
                    sql_row[mc.name] = mc.get_value(crow['key'], crow['value'])
                    row_has_value = row_has_value or not mc.is_key_column
            if row_has_value:
                yield sql_row

    def get_fluff_grains(self, diff):
        """
        Get the list of grains that have changed as a result of this Fluff diff.
        """
        groups = diff['group_values']
        for ind in diff['indicator_changes']:
            key_prefix = [diff['doc_type']] + groups + [ind['calculator'], ind['emitter']]
            if ind['emitter_type'] == 'null':
                yield key_prefix + [None]
            elif ind['emitter_type'] == 'date':
                for value in ind['values']:
                    yield key_prefix + [value.strftime("%Y-%m-%dT%H:%M:%SZ")]
            else:
                for value in ind['values']:
                    yield key_prefix + [value]

    def get_extract_mapping(self, diff):
        """
        Get the SqlExtractMapping for the Fluff diff. This assumes the Fluff
        view is emitting data as follows:

            [doc_type, group1... groupN, calc_name, emitter_name, emitter_value] = 1
        """
        mapping = SqlExtractMapping(name=diff['doc_type'], couch_view=fluff_view)
        columns = []
        for i, group in enumerate(diff['group_names']):
            columns.append(ColumnDef(name=group,
                                     data_type='string',
                                     value_source='key',
                                     value_index=1 + i))

        num_groups = len(diff['group_names'])
        columns.append(ColumnDef(name='emitter_value',
                                 data_type='date',
                                 value_source='key',
                                 value_index=3 + num_groups))

        for indicator in diff['indicator_changes']:
            calc_name = indicator['calculator']
            emitter_name = indicator['emitter']
            columns.append(ColumnDef(name='{0}_{1}'.format(calc_name, emitter_name),
                                     data_type='integer',
                                     value_source='value',
                                     match_keys=[
                                         KeyMatcher(index=1 + num_groups, value=calc_name),
                                         KeyMatcher(index=2 + num_groups, value=emitter_name)
                                     ]))
        mapping.columns = columns
        return mapping

    def recalculate_grains(self, grains, database):
        """
        Query CouchDB to get the updated value for the grains.
        """
        result = []
        for grain in grains:
            result.extend(self.get_couch_rows(fluff_view, grain, grain + [{}], db=get_db(database)))
        return result
