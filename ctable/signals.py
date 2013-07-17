from ctable.util import get_extractor
from fluff.signals import indicator_document_updated
from .util import get_backend_name_for_fluff_pillow


def process_fluff_diff(sender, diff=None, **kwargs):
    backend_name = get_backend_name_for_fluff_pillow(diff['doc_type'])
    if diff and backend_name:
        get_extractor(backend_name).process_fluff_diff(diff, backend_name)

indicator_document_updated.connect(process_fluff_diff)
