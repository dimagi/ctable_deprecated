from fluff.signals import indicator_document_updated
from django.conf import settings
from .models import SqlExtractMapping
from .base import CtableExtractor

ctable = CtableExtractor(settings.SQL_REPORTING_DATABASE_URL, SqlExtractMapping.get_db())

def process_fluff_diff(sender, diff=None, **kwargs):
    if diff:
        ctable.process_fluff_diff(diff)

indicator_document_updated.connect(process_fluff_diff)