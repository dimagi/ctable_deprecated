from .writers import SqlTableWriter


class CtableExtractor(object):
    def __init__(self, sql_connection_or_url, couch_db, sqlextract):
        self.db = couch_db
        self.sql_connection_or_url = sql_connection_or_url
        self.sql_extract = sqlextract

    def pull(self, table_name, couch_view, startkey=[], endkey=[{}], **kwargs):
        result = self.db.view(
            couch_view,
            reduce=True,
            group=True,
            startkey=startkey,
            endkey=endkey,
            **kwargs)

        print "Total rows: %d" % result.total_rows
        rows = self._gen_rows(result)
        writer = SqlTableWriter(self.sql_connection_or_url, self.sql_extract)
        with writer:
            writer.write_table(table_name, rows)

    def _gen_rows(self, result):
        for row in result:
            yield self._transform_row(row['key'], row['value'])

    def _transform_row(self, key, value):
        row = {}
        for c in self.sql_extract.columns:
            if c.matches(key):
                row[c.name] = c.get_value(key, value)
        return row
