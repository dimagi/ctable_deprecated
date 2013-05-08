from ctable import SqlExtractMapping
from ctable.models import ColumnDef, RowMatch
from .writers import SqlTableWriter
import logging

logger = logging.getLogger("ctable")


class CtableExtractor(object):
    def __init__(self, sql_connection_or_url, couch_db):
        self.db = couch_db
        self.sql_connection_or_url = sql_connection_or_url
        self.writer = SqlTableWriter(self.sql_connection_or_url)

    def extract(self, extract_mapping, couch_view, startkey=[], endkey=[{}], **kwargs):
        result = self.db.view(
            couch_view,
            reduce=True,
            group=True,
            startkey=startkey,
            endkey=endkey,
            **kwargs)

        logger.info("Total rows: %d", result.total_rows)
        rows = self._gen_rows(result, extract_mapping)
        self._write_rows(rows, extract_mapping)

    def _write_rows(self, rows, extract_mapping):
        with self.writer:
            self.writer.write_table(rows, extract_mapping)

    def _gen_rows(self, result, extract_mapping):
        for row in result:
            yield self._transform_row(row['key'], row['value'], extract_mapping)

    def _transform_row(self, key, value, extract_mapping):
        row = {}
        for c in extract_mapping.columns:
            if c.matches(key):
                row[c.name] = c.get_value(key, value)
        return row

    def _get_grains(self, diff):
        groups = diff['group_values']
        for indicator in diff['indicator_changes']:
            if indicator['emitter_type'] == 'null':
                yield [diff['doc_type']] + groups + [indicator['calculator'], indicator['emitter'], None]
            else:
                for value in indicator['values']:
                    yield [diff['doc_type']] + groups + [indicator['calculator'], indicator['emitter'], value]

    def _get_extract_mapping(self, diff):
        mapping = SqlExtractMapping(name=diff['doc_type'])
        columns = []
        for i, group in enumerate(diff['group_names']):
            columns.append(ColumnDef(name=group,
                                     data_type='string',
                                     value_source='key',
                                     value_index=i+1))

        num_groups = len(diff['group_names'])
        columns.append(ColumnDef(name='emitted_value',
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
                                         RowMatch(index=1 + num_groups, value=calc_name),
                                         RowMatch(index=2 + num_groups, value=emitter_name)
                                     ]))
        mapping.columns = columns
        return mapping

