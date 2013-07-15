from couchdbkit import ResourceNotFound
from ctable.backends import InMemoryBackend, SqlBackend
from dimagi.utils.chunked import chunked
from django.conf import settings
from ctable.base import CtableExtractor
from ctable.models import SqlExtractMapping
from dimagi.utils.couch.database import get_db
from dimagi.utils.decorators.memoized import memoized
from dimagi.utils.modules import to_function


@memoized
def get_extractor(backend_name):
    return CtableExtractor(SqlExtractMapping.get_db(), get_backend(backend_name))


def get_test_extractor():
    return CtableExtractor(SqlExtractMapping.get_db(), InMemoryBackend())


def backends():
    return getattr(settings, 'CTABLE_BACKENDS', {'SQL': 'ctable.backends.SqlBackend'})


@memoized
def get_backend(backend_name):
    if not backend_name:
        backend = SqlBackend()
    else:
        backend_class_name = backends()[backend_name]
        backend_class = to_function(backend_class_name, failhard=True)
        backend = backend_class()
    return backend


def combine_rows(rows, extract_mapping, chunksize=250):
        """
        Split the rows into chunks and combine them based on their key columns.
        Aim: reduce the number of times we have to hit the database
        """
        key_columns = extract_mapping.key_columns

        for chunk in chunked(rows, chunksize):
            rows_tmp = {}
            for row_dict in chunk:
                row_key = tuple([row_dict[k] for k in key_columns])
                row_data = rows_tmp.setdefault(row_key, {})
                row_data.update(row_dict)

            for row in rows_tmp.values():
                yield row


@memoized
def get_enabled_fluff_pillows():
    hardcoded = getattr(settings, 'FLUFF_PILLOW_TYPES_TO_SQL', {})
    try:
        dynamic = get_db().get('FLUFF_PILLOW_TYPES_TO_SQL').get('enabled_pillows', {})
    except ResourceNotFound:
        dynamic = []

    hardcoded.update(dynamic)
    return hardcoded


def get_backend_name_for_fluff_pillow(pillow_name):
    return get_enabled_fluff_pillows().get(pillow_name, None)
