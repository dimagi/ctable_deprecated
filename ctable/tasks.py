from celery.schedules import crontab
from celery.task import periodic_task, task
from celery import current_task
from ctable.util import get_extractor
from .models import SqlExtractMapping, UnsupportedScheduledExtractError


@task
def process_extract(extract_id, limit=None, date_range=None):
    def update_status(total):
        def update_current(current):
            meta = {'current': current, 'total': total}
            current_task.update_state(state='PROGRESS', meta=meta)
        return update_current

    extract = SqlExtractMapping.get(extract_id)
    try:
        get_extractor().extract(extract, limit=limit, date_range=date_range, status_callback=update_status)
    except UnsupportedScheduledExtractError:
        pass


@periodic_task(run_every=crontab(hour="*", minute="1", day_of_week="*"))
def ctable_extract_schedule():
    logger = ctable_extract_schedule.get_logger()
    exps = SqlExtractMapping.schedule()

    logger.info("ctable_extract_schedule: processing %s extracts" % len(exps))

    for exp in exps:
        process_extract.delay(exp['id'])
