from ctable.util import get_extractor
from fluff.signals import indicator_document_updated
from .util import is_pillow_enabled_for_sql


def process_fluff_diff(sender, diff=None, **kwargs):
    if diff and is_pillow_enabled_for_sql(diff['doc_type']):
        get_extractor().process_fluff_diff(diff)

indicator_document_updated.connect(process_fluff_diff)
