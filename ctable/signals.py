import inspect
from django.db.models import signals
from django.utils.importlib import import_module
from ctable.fixtures import CtableMappingFixture
from fluff.signals import indicator_document_updated


def process_fluff_diff(sender, diff=None, **kwargs):
    from ctable.util import get_extractor
    from .util import get_backend_name_for_fluff_pillow
    backend_name = get_backend_name_for_fluff_pillow(diff['doc_type'])
    if diff and backend_name:
        get_extractor(backend_name).process_fluff_diff(diff, backend_name)

indicator_document_updated.connect(process_fluff_diff)


def catch_signal(app, **kwargs):
    app_name = app.__name__.rsplit('.', 1)[0]
    try:
        mod = import_module('.ctable_mappings', app_name)
        print "Creating CTable mappings for %s" % app_name

        clsmembers = inspect.getmembers(mod, inspect.isclass)
        mappings = [cls[1] for cls in clsmembers
                    if not cls[1] == CtableMappingFixture and issubclass(cls[1], CtableMappingFixture)]
        for mapping in mappings:
            mapping().create()
    except ImportError:
        pass


signals.post_syncdb.connect(catch_signal)

