from datetime import datetime
from celery.schedules import crontab
from celery.task import periodic_task, task
from celery.utils.log import get_task_logger
from ctable import CtableExtractor
from ctable.models import SqlExtractMapping, UnsupportedScheduledExtractError
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
def daily_reports():
    now = datetime.utcnow()
    view = "ctable/schedule"
    hour = now.hour
    exps = SqlExtractMapping.view(view,
                                  key=['daily', -1, hour]).all()

    exps.extend(SqlExtractMapping.view(view,
                                       key=['weekly', now.weekday(), hour]).all())

    exps.extend(SqlExtractMapping.view(view,
                                       key=['monthly', now.day, hour]).all())

    for exp in exps:
        process_extract.delay(exp['id'])
