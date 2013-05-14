from couchdbkit import BadValueError
from couchdbkit.ext.django.schema import (Document, StringProperty, IntegerProperty,
                                          DocumentSchema, SchemaListProperty, ListProperty)
from django.conf import settings
import sqlalchemy
import datetime
import re


def validate_name(value, search=re.compile(r'[^a-zA-Z0-9_]').search):
    if not value or bool(search(value)):
        raise BadValueError("Only a-z, 0-9 and '_' characters allowed")


class UnsupportedScheduledExtractError(Exception):
    pass


class RowMatcher(object):
    def matches(self, row_key, row_value):
        raise NotImplementedError()


class KeyMatcher(DocumentSchema, RowMatcher):
    index = IntegerProperty()
    value = StringProperty()

    def matches(self, row_key, row_value):
        return row_key[self.index] == self.value


class ColumnDef(DocumentSchema):
    name = StringProperty(required=True)
    data_type = StringProperty(required=True, choices=["string", "integer", "date", "datetime"])
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
            return value

        if self.data_type == "date" or self.data_type == "datetime":
            converted = datetime.datetime.strptime(value, self.date_format or "%Y-%m-%dT%H:%M:%SZ")
            return converted.date() if self.data_type == "date" else converted
        elif self.data_type == "integer":
            return int(value)
        else:
            return value

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
    def sql_column(self):
        return sqlalchemy.Column(self.name, self.sql_type, nullable=True)

    @property
    def is_key_column(self):
        return not self.match_keys


class SqlExtractMapping(Document):
    domain = StringProperty()
    name = StringProperty(required=True, validators=validate_name)
    columns = SchemaListProperty(ColumnDef, required=True)

    schedule_type = StringProperty(choices=['daily', 'weekly', 'monthly'], default='daily')
    hour = IntegerProperty(default=8)
    day_of_week_month = IntegerProperty(default=-1)
    """Day of week for weekly, day of month for monthly, -1 for daily"""

    couch_view = StringProperty(required=True)
    couch_startkey = ListProperty(default=[])
    couch_endkey = ListProperty(default=[{}])

    @property
    def table_name(self):
        return "{0}_{1}".format(self.domain, self.name) if self.domain else self.name

    @classmethod
    def by_domain(cls, domain):
        key = [domain, cls.__name__]
        return cls.view('domain/docs',
                        startkey=key,
                        endkey=key + [{}],
                        reduce=False,
                        include_docs=True,
                        stale=settings.COUCH_STALE_QUERY).all()

    @property
    def key_columns(self):
        return [c.name for c in self.columns if c.is_key_column]

import signals