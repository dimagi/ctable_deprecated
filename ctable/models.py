from couchdbkit.ext.django.schema import (Document, StringProperty, IntegerProperty,
                                          DocumentSchema, DictProperty, SchemaListProperty)
from django.conf import settings
import sqlalchemy
import datetime


class ColumnDef(DocumentSchema):
    name = StringProperty(required=True)
    data_type = StringProperty(required=True, choices=["string", "integer", "date", "datetime"])
    data_format = StringProperty()
    max_length = IntegerProperty()  # only applies to string columns
    value_source = StringProperty(required=True, choices=["key", "value"])
    value_attribute = StringProperty()  # attribute accessor for value e.g. value["sum"]
    value_index = IntegerProperty()  # index accessor for value e.g. key[1]
    match_key = DictProperty()  # used to determine when this column is relevant e.g. rows where key[1] = 'indicator_a'

    def matches(self, key):
        if not self.match_key:
            return True
        else:
            return key[self.match_key["index"]] == self.match_key.get("value", self.name)

    def get_value(self, key, value):
        val = self._get_raw_value(key, value)
        return self._convert_type(val)

    def _get_raw_value(self, key, value):
        use_index = self.value_index is not None
        use_attr = self.value_attribute is not None

        if self.value_source == "key" and use_index:
            return key[self.value_index]
        elif self.value_source == "value" and use_index or use_attr:
            return value[self.value_index if use_index else self.value_attribute]
        else:
            return value

    def _convert_type(self, value):
        if self.data_type == "date" or self.data_type == "datetime":
            converted = datetime.datetime.strptime(value, self.data_format or "%Y-%m-%d")
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
        else:
            raise Exception("Unexpected type", self.data_type)

    @property
    def sql_column(self):
        opts = {}
        if self.is_key_column:
            opts = {"primary_key": True, "nullable": False, "autoincrement": False}
        return sqlalchemy.Column(self.name, self.sql_type, **opts)

    @property
    def is_key_column(self):
        return not self.match_key


class SqlExtract(Document):
    domain = StringProperty()
    columns = SchemaListProperty(ColumnDef)

    @classmethod
    def by_domain(cls, domain):
        key = [domain, cls.__name__]
        return cls.view('domain/docs',
                        startkey=key,
                        endkey=key + [{}],
                        reduce=False,
                        include_docs=True,
                        stale=settings.COUCH_STALE_QUERY
                        ).all()

    @property
    def key_columns(self):
        return [c.name for c in self.columns if c.is_key_column]
