import logging
import six
import sqlalchemy
import alembic

logger = logging.getLogger(__name__)

BASE_TYPE_MAP = dict(string=sqlalchemy.String,
                     integer=sqlalchemy.Integer,
                     date=sqlalchemy.Date,
                     datetime=sqlalchemy.DateTime)


class ColumnTypeException(Exception):
    pass


class CtableWriter(object):
    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def write_table(self, rows, extract_mapping, status_callback=None):
        raise NotImplementedError()


class SqlTableWriter(CtableWriter):
    """
    Write rows to a database specified by URL
    """

    def __init__(self, url_or_connection):
        if isinstance(url_or_connection, six.string_types):
            self.base_connection = sqlalchemy.create_engine(url_or_connection)
        else:
            self.base_connection = url_or_connection

    def __enter__(self):
        self.connection = self.base_connection.connect()  # "forks" the SqlAlchemy connection
        return self  # TODO: A safe context manager so this can be called many times

    def __exit__(self, type, value, traceback):
        self.connection.close()

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            self._metadata = sqlalchemy.MetaData()
            self._metadata.bind = self.connection
            self._metadata.reflect()
        return self._metadata

    def table(self, table_name):
        return sqlalchemy.Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)

    def init_table(self, table_name, column_defs):
        ctx = alembic.migration.MigrationContext.configure(self.connection)
        op = alembic.operations.Operations(ctx)

        if not table_name in self.metadata.tables:
            columns = [c.sql_column for c in column_defs]
            op.create_table(table_name, *columns)
            self.metadata.reflect()
        else:
            self.make_table_compatible(table_name, column_defs)

    def make_table_compatible(self, table_name, column_defs):
        ctx = alembic.migration.MigrationContext.configure(self.connection)
        op = alembic.operations.Operations(ctx)

        if not table_name in self.metadata.tables:
            raise Exception("Table does not exist", table_name)

        for column in column_defs:
            if not column.name in [c.name for c in self.table(table_name).columns]:
                op.add_column(table_name, column.sql_column)
                self.metadata.clear()
                self.metadata.reflect()
            else:
                columns = dict([(c.name, c) for c in self.table(table_name).columns])
                current_ty = columns[column.name].type
                if not isinstance(current_ty, BASE_TYPE_MAP[column.data_type]):
                    raise ColumnTypeException("Column types don't match", column.name)

    def upsert(self, table, row_dict, key_columns):

        # For atomicity "insert, catch, update" is slightly better than "select, insert or update".
        # The latter may crash, while the former may overwrite data (which should be fine if whatever is
        # racing against this is importing from the same source... if not you are busted anyhow
        try:
            insert = table.insert().values(**row_dict)
            self.connection.execute(insert)
        except sqlalchemy.exc.IntegrityError:
            update = table.update()
            for k in key_columns:
                k_val = row_dict.pop(k)
                update = update.where(getattr(table.c, k) == k_val)
            update = update.values(**row_dict)
            self.connection.execute(update)

    def write_table(self, rows, extract_mapping, status_callback=None):
        table_name = extract_mapping.table_name
        columns = extract_mapping.columns
        key_columns = extract_mapping.key_columns

        self.init_table(table_name, columns)
        for i, row_dict in enumerate(rows):
            logger.debug(".")
            if status_callback:
                status_callback(i)
            self.upsert(self.table(table_name), row_dict, key_columns)


class TestWriter(CtableWriter):
    data = {}
    
    def write_table(self, rows, extract_mapping, status_callback=None):
        table_name = extract_mapping.table_name
        columns = [c.name for c in extract_mapping.columns]

        if not self.data:
            self.data = {'table_name': table_name, 'columns': columns, 'rows': []}

        key_columns = extract_mapping.key_columns

        rows_tmp = {}
        for row_dict in rows:
            row_key = tuple([row_dict[k] for k in key_columns])
            row_data = rows_tmp.setdefault(row_key, {})
            row_data.update(row_dict)

        for row in rows_tmp.values():
            row_arr = [row[c] if c in row else '' for c in columns]
            self.data['rows'].append(row_arr)

