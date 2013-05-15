from celery.schedules import crontab
from celery.task import periodic_task, task
from .base import CtableExtractor
from .models import SqlExtractMapping, UnsupportedScheduledExtractError
from django.conf import settings

ctable = CtableExtractor(settings.SQL_REPORTING_DATABASE_URL, SqlExtractMapping.get_db())


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
    exps = SqlExtractMapping.schedule()

    logger.info("ctable_extract_schedule: processing %s extracts" % len(exps))

    for exp in exps:
        process_extract.delay(exp['id'])
