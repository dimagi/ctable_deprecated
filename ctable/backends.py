import logging
import six
import sqlalchemy
import alembic
from django.utils.translation import ugettext as _
from django.conf import settings

logger = logging.getLogger(__name__)

BASE_TYPE_MAP = dict(string=sqlalchemy.String,
                     integer=sqlalchemy.Integer,
                     date=sqlalchemy.Date,
                     datetime=sqlalchemy.DateTime)


class CompatibilityException(Exception):
    pass


class ColumnTypeException(CompatibilityException):
    pass


class CtableBackend(object):

    def write_rows(self, rows, extract_mapping):
        raise NotImplementedError()

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def check_mapping(self, mapping):
        pass

    def clear_all_data(self, mapping):
        pass


class SqlBackend(CtableBackend):
    """
    Write rows to a database specified by URL
    """
    def __init__(self, url_or_connection=None):
        if not url_or_connection:
            url_or_connection = settings.SQL_REPORTING_DATABASE_URL

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

    def table(self, table_name):
        return sqlalchemy.Table(table_name, self.metadata, autoload=True, autoload_with=self.connection)

    @property
    def op(self):
        if not hasattr(self, '_op') or self._op is None:
            ctx = alembic.migration.MigrationContext.configure(self.connection)
            self._op = alembic.operations.Operations(ctx)
        return self._op

    def init_table(self, table_name, column_defs):
        if not table_name in self.metadata.tables:
            logger.info('Creating new reporting table: %s', table_name)
            columns = [c.sql_column for c in column_defs]
            self.op.create_table(table_name, *columns)
            owner = getattr(settings, 'SQL_REPORTING_OBJECT_OWNER', None)
            if owner:
                self.op.execute('ALTER TABLE "%s" OWNER TO %s' % (table_name, owner))
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

    def clear_all_data(self, mapping):
        table_name = mapping.table_name
        if table_name in self.metadata.tables:
            self.op.drop_table(table_name)
            self.metadata.clear()
            self.metadata.reflect()

    def check_mapping(self, mapping):
        errors = []
        warnings = []
        if mapping.table_name in self.metadata.tables:
            table = self.table(mapping.table_name)
            table_columns = dict([(c.name, c) for c in table.columns])
            mapping_columns = dict([(c.name, c) for c in mapping.columns])
            for name, column in table_columns.items():
                if name not in mapping_columns:
                    if column.primary_key:
                        errors.append(_('Key column exists in table but not in mapping: %s' % name))
                    else:
                        warnings.append(_('Column exists in table but not in mapping: %(column)s') % {'column': name})

            for col in mapping.key_columns:
                if col not in table_columns:
                    errors.append(_('Key column exists in mapping but not in table: %(column)s') % {'column': col})

            for name, column in mapping_columns.items():
                if name in table_columns:
                    current_ty = table_columns[name].type
                    if not isinstance(current_ty, BASE_TYPE_MAP[column.data_type]):
                        errors.append(_('Column types do not match: %(column)s') % {'column': name})

        return {'errors': errors, 'warnings': warnings}

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

    def write_rows(self, rows, extract_mapping):
        table_name = extract_mapping.table_name
        columns = extract_mapping.columns
        key_columns = extract_mapping.key_columns

        self.init_table(table_name, columns)
        for row_dict in rows:
            logger.debug(".")
            self.upsert(self.table(table_name), row_dict, key_columns)


class InMemoryBackend(CtableBackend):
    data = {}

    def write_rows(self, rows, extract_mapping):
        table_name = extract_mapping.table_name
        columns = [c.name for c in extract_mapping.columns]

        if not self.data:
            self.data = {'table_name': table_name, 'columns': columns, 'rows': []}

        for row_dict in rows:
            row_arr = [row_dict[c] if c in row_dict else '' for c in columns]
            self.data['rows'].append(row_arr)
