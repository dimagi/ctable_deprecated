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
        self._metadata = None
        self._op = None
        return self  # TODO: A safe context manager so this can be called many times

    def __exit__(self, type, value, traceback):
        self.connection.close()

    @property
    def metadata(self):
        if not hasattr(self, '_metadata') or self._metadata is None:
            self._metadata = sqlalchemy.MetaData()
            self._metadata.bind = self.connection
            self._metadata.reflect()
        return self._metadata

    @property
    def op(self):
        if not hasattr(self, '_op') or self._op is None:
            ctx = alembic.migration.MigrationContext.configure(self.connection)
            self._op = alembic.operations.Operations(ctx)
        return self._op

    def table(self, table_name):
        return sqlalchemy.Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)

    def init_table(self, table_name, column_defs):
        if not table_name in self.metadata.tables:
            logger.info('Creating new reporting table: %s', table_name)
            columns = [c.sql_column for c in column_defs]
            self.op.create_table(table_name, *columns)
            self.metadata.reflect()
        else:
            self.make_table_compatible(table_name, column_defs)

    def make_table_compatible(self, table_name, column_defs):
        if not table_name in self.metadata.tables:
            raise Exception("Table does not exist", table_name)

        for column in column_defs:
            if not column.name in [c.name for c in self.table(table_name).columns]:
                logger.info('Adding column to reporting table: %s.%s', table_name, column.name)
                self.op.add_column(table_name, column.sql_column)
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

    def write_table(self, rows, extract_mapping):
        table_name = extract_mapping.table_name
        columns = extract_mapping.columns
        key_columns = extract_mapping.key_columns

        self.init_table(table_name, columns)
        for row_dict in rows:
            logger.debug(".")
            self.upsert(self.table(table_name), row_dict, key_columns)


class TestWriter(CtableWriter):
    data = {}
    
    def write_table(self, rows, extract_mapping):
        table_name = extract_mapping.table_name
        columns = [c.name for c in extract_mapping.columns]

        if not self.data:
            self.data = {'table_name': table_name, 'columns': columns, 'rows': []}

        for row_dict in rows:
            row_arr = [row_dict[c] if c in row_dict else '' for c in columns]
            self.data['rows'].append(row_arr)
