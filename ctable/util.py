from couchdbkit import ResourceNotFound
from django.conf import settings
from dimagi.utils.couch.database import get_db


def get_enabled_fluff_pillows():
    hardcoded = getattr(settings, 'FLUFF_PILLOWS_TO_SQL', {})
    try:
        dynamic = get_db().get('FLUFF_PILLOWS_TO_SQL').get('enabled_pillows', [])
    except ResourceNotFound:
        dynamic = {}

    hardcoded.extend(dynamic)
    return hardcoded


def is_pillow_enabled_for_sql(pillow_name):
    return pillow_name in get_enabled_fluff_pillows()