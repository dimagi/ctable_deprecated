from couchdbkit import ResourceNotFound
from ctable.writer import InMemoryWriter
from dimagi.utils.chunked import chunked
from django.conf import settings
from ctable.base import CtableExtractor
from ctable.models import SqlExtractMapping
from dimagi.utils.couch.database import get_db
from dimagi.utils.decorators.memoized import memoized


@memoized
def get_extractor():
    return CtableExtractor(settings.SQL_REPORTING_DATABASE_URL, SqlExtractMapping.get_db())


@memoized
def get_test_extractor():
    return CtableExtractor(settings.SQL_REPORTING_DATABASE_URL, SqlExtractMapping.get_db(), writer_class=InMemoryWriter)


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


def get_enabled_fluff_pillows():
    hardcoded = getattr(settings, 'FLUFF_PILLOW_TYPES_TO_SQL', [])
    try:
        dynamic = get_db().get('FLUFF_PILLOW_TYPES_TO_SQL').get('enabled_pillows', [])
    except ResourceNotFound:
        dynamic = []

    hardcoded.extend(dynamic)
    return hardcoded


def is_pillow_enabled_for_sql(pillow_name):
    return pillow_name in get_enabled_fluff_pillows()