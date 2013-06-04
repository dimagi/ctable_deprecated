import logging
import six
import sqlalchemy
import alembic
from itertools import combinations
from sqlalchemy.sql.operators import is_, isnot
from sqlalchemy.sql.expression import and_

logger = logging.getLogger(__name__)

BASE_TYPE_MAP = dict(string=sqlalchemy.String,
                     integer=sqlalchemy.Integer,
                     date=sqlalchemy.Date,
                     datetime=sqlalchemy.DateTime)


class ColumnTypeException(Exception):
    pass


class SqlTableWriter(object):
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
            columns = [sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)]
            columns += [c.sql_column for c in column_defs]
            unique_col_names = [c.name for c in column_defs if c.is_key_column]
            if unique_col_names:
                columns.append(sqlalchemy.UniqueConstraint(*unique_col_names))
            op.create_table(table_name, *columns)
            nullable_unique_columns = [c.name for c in column_defs if c.is_key_column and c.nullable]
            if nullable_unique_columns:
                self.create_unique_indexes(table_name, nullable_unique_columns, unique_col_names)
            self.metadata.reflect()
        else:
            self.make_table_compatible(table_name, column_defs)

    def create_unique_indexes(self, table_name, nullable_unique_columns, unique_col_names):
        """
        In order to enforce uniqueness on the 'group' columns indexes must be created. However null values are not
        considered equal so multiple partial indexes need to be created for columns that can be null.

        This function creates one index for each combination of columns.
        e.g. columns = a,b,c,d (d is not nullable)
        indexes:
            unique(a,c,d) where b is null
            unique(b,c,d) where a is null
            unique(a,b,d) where c is null
            unique(a,d) where b,c is null
            unique(b,d) where a,c is null
            unique(c,d) where a,b is null

        """
        unique_columns = [c for c in self.table(table_name).columns if c.name in unique_col_names]
        num_unique = len(nullable_unique_columns)
        while num_unique > 0:
            combos = combinations(nullable_unique_columns, num_unique)
            for combo in combos:
                index_cols = filter(lambda x: x.name not in combo, unique_columns)
                if index_cols:
                    null_cols = filter(lambda x: x.name in combo, unique_columns)
                    index = sqlalchemy.Index('%s_unique_%s_null' % (table_name, '_'.join(combo)), *index_cols, unique=True,
                                             postgresql_where=and_(*[is_(col, None) for col in null_cols]))
                    index.create(self.connection)
            num_unique -= 1

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

    def write_table(self, rows, extract_mapping):
        table_name = extract_mapping.table_name
        columns = extract_mapping.columns
        key_columns = extract_mapping.key_columns

        self.init_table(table_name, columns)

        for row_dict in rows:
            logger.debug(".")
            self.upsert(self.table(table_name), row_dict, key_columns)
