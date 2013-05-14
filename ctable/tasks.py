from datetime import datetime
from celery.schedules import crontab
from celery.task import periodic_task, task
from .base import CtableExtractor
from .models import SqlExtractMapping, UnsupportedScheduledExtractError
from django.conf import settings
from fluff.signals import indicator_document_updated


ctable = CtableExtractor(settings.SQL_REPORTING_DATABASE_URL, SqlExtractMapping.get_db())


def process_fluff_diff(sender, diff=None, **kwargs):
    if diff:
        ctable.process_fluff_diff(diff)

indicator_document_updated.connect(process_fluff_diff)


@task
def process_extract(extract_id):
    extract = SqlExtractMapping.get(extract_id)
    try:
        ctable.extract(extract)
    except UnsupportedScheduledExtractError:
        pass


@periodic_task(run_every=crontab(hour="*", minute="1", day_of_week="*"))
def ctable_extract_schedule():
    logger = ctable_extract_schedule.get_logger()
    now = datetime.utcnow()
    view = "ctable/schedule"
    hour = now.hour
    exps = SqlExtractMapping.view(view,
                                  key=['daily', -1, hour]).all()

    exps.extend(SqlExtractMapping.view(view,
                                       key=['weekly', now.weekday(), hour]).all())

    exps.extend(SqlExtractMapping.view(view,
                                       key=['monthly', now.day, hour]).all())

    logger.info("ctable_extract_schedule: processing %s extracts" % len(exps))

    for exp in exps:
        process_extract.delay(exp['id'])
