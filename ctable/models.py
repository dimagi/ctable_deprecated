from couchdbkit import BadValueError
from couchdbkit.ext.django.schema import (Document, StringProperty, IntegerProperty, StringListProperty, Property,
                                          DocumentSchema, SchemaListProperty, ListProperty, BooleanProperty)
from django.conf import settings
from datetime import datetime, date
import sqlalchemy
import re


def validate_name(value, search=re.compile(r'[^a-zA-Z0-9_]').search):
    if not value or bool(search(value)):
        raise BadValueError("Only a-z, 0-9 and '_' characters allowed")


class RowMatcher(object):
    def matches(self, row_key, row_value):
        raise NotImplementedError()


class KeyMatcher(DocumentSchema, RowMatcher):
    index = IntegerProperty()
    value = StringProperty()

    def matches(self, row_key, row_value):
        return row_key[self.index] == self.value if len(row_key) > self.index else False


class ColumnDef(DocumentSchema):
    name = StringProperty(required=True)
    data_type = StringProperty(required=True, choices=["string", "integer", "date", "datetime"])
    null_value_placeholder = StringProperty()
    date_format = StringProperty()
    """Format string for date columns"""
    max_length = IntegerProperty()
    """Max length for string columns"""
    value_source = StringProperty(required=True, choices=["key", "value"])
    value_attribute = StringProperty()
    """Attribute accessor for value e.g. value["sum"]"""
    value_index = IntegerProperty()
    """Index accessor for value e.g. key[1]"""
    match_keys = SchemaListProperty(KeyMatcher)
    """List of KeyMatcher objects used to determine when this columns is relevant
    e.g. rows where key[1] = 'indicator_a' and key[2] = 'count'"""

    def matches(self, key, value):
        if not self.match_keys:
            return True
        else:
            return all([match_key.matches(key, value) for match_key in self.match_keys])

    def get_value(self, key, value):
        val = self._get_raw_value(key, value)
        return self.convert_type(val)

    def _get_raw_value(self, key, value):
        use_index = self.value_index is not None
        use_attr = self.value_attribute is not None

        if self.value_source == "key" and use_index:
            return key[self.value_index]
        elif self.value_source == "value" and (use_index or use_attr):
            return value[self.value_index if use_index else self.value_attribute]
        else:
            return value

    def convert_type(self, value):
        if value is None:
            if not self.is_key_column:
                return value
            elif self.null_value_placeholder:
                value = self.null_value_placeholder
            else:
                return self.default_null_value_placeholder

        try:
            if self.data_type == "date" or self.data_type == "datetime":
                converted = datetime.strptime(value, self.date_format or "%Y-%m-%dT%H:%M:%SZ")
                return converted.date() if self.data_type == "date" else converted
            elif self.data_type == "integer":
                return int(value)
            else:
                return value
        except ValueError:
            return self.null_value_placeholder or self.default_null_value_placeholder

    @property
    def sql_type(self):
        if self.data_type == "string":
            return sqlalchemy.Unicode(self.max_length or 255)
        elif self.data_type == "integer":
            return sqlalchemy.Integer
        elif self.data_type == "date":
            return sqlalchemy.Date
        elif self.data_type == 'datetime':
            return sqlalchemy.DateTime
        else:
            raise Exception("Unexpected type", self.data_type)

    @property
    def default_null_value_placeholder(self):
        if self.data_type == "string":
            return '__none__'
        elif self.data_type == "integer":
            return 1618033988  # see http://en.wikipedia.org/wiki/Golden_ratio
        elif self.data_type == "date":
            return date.min
        elif self.data_type == 'datetime':
            return datetime.min
        else:
            raise Exception("Unexpected type", self.data_type)

    @property
    def sql_column(self):
        return sqlalchemy.Column(self.name, self.sql_type,
                                 nullable=(not self.is_key_column),
                                 primary_key=self.is_key_column)

    @property
    def is_key_column(self):
        return not self.match_keys

    def validate(self, required=True):
        super(ColumnDef, self).validate(required)

        if self.value_source == 'key' and self.value_index is None:
            raise BadValueError('Key columns must specify a value_index')


SCHEDULE_VIEW = "ctable/schedule"


class SqlExtractMapping(Document):
    backend = StringProperty()
    """See CTABLE_BACKENDS in settings"""
    database = StringProperty(required=False)
    """CouchDB Database name where raw data is stored"""
    domains = StringListProperty(required=True)
    name = StringProperty(required=True, validators=validate_name)
    columns = SchemaListProperty(ColumnDef, required=True)
    active = BooleanProperty(default=False)
    auto_generated = BooleanProperty(default=False)

    schedule_type = StringProperty(choices=['daily', 'weekly', 'monthly'], default='daily')
    schedule_hour = IntegerProperty(default=8)
    schedule_day = IntegerProperty(default=-1)
    """Day of week for weekly, day of month for monthly, -1 for daily"""

    couch_view = StringProperty(required=True)
    couch_key_prefix = ListProperty(default=[])
    couch_date_range = IntegerProperty(default=-1)
    """Number of days in the past to query data for. This assumes that the last
    element in the view key is a date."""
    couch_date_format = StringProperty(default='%Y-%m-%dT%H:%M:%S.%fZ')
    """Used when appending the date to the key (in cases where couch_date_range > 0)"""

    @property
    def table_name(self):
        return "{0}_{1}".format('_'.join(self.domains), self.name)

    @property
    def key_columns(self):
        return [c.name for c in self.columns if c.is_key_column]

    @classmethod
    def all(cls):
        return cls.view('ctable/by_name',
                        startkey=[None],
                        endkey=[None, {}],
                        reduce=False,
                        include_docs=True).all()

    @classmethod
    def by_domain(cls, domain):
        return cls.view('ctable/by_name',
                        startkey=[domain],
                        endkey=[domain, {}],
                        reduce=False,
                        include_docs=True).all()

    @classmethod
    def by_name(cls, domain, name):
        key = [domain, name] if name else [domain]
        return cls.view('ctable/by_name',
                        startkey=key,
                        endkey=key + [{}],
                        reduce=False,
                        include_docs=True).one()

    @classmethod
    def daily_schedule(cls, extract_date, active=True):
        key = [cls._status(active), 'daily', -1, extract_date.hour]
        return SqlExtractMapping.view(SCHEDULE_VIEW, key=key).all()

    @classmethod
    def weekly_schedule(cls, extract_date, active=True):
        key = [cls._status(active), 'weekly', extract_date.weekday(), extract_date.hour]
        return SqlExtractMapping.view(SCHEDULE_VIEW, key=key).all()

    @classmethod
    def monthly_schedule(cls, extract_date, active=True):
        key = [cls._status(active), 'monthly', extract_date.day, extract_date.hour]
        return SqlExtractMapping.view(SCHEDULE_VIEW, key=key).all()

    @classmethod
    def schedule(cls, extract_date=None, active=True):
        extract_date = extract_date or datetime.utcnow()
        exps = cls.daily_schedule(extract_date, active)
        exps.extend(cls.weekly_schedule(extract_date, active))
        exps.extend(cls.monthly_schedule(extract_date, active))
        return exps

    @classmethod
    def _status(cls, active):
        return 'active' if active else 'inactive'


from . import signals