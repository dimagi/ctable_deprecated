from celery.schedules import crontab
from celery.task import periodic_task, task
from ctable.util import get_extractor
from .models import SqlExtractMapping, UnsupportedScheduledExtractError


@task
def process_extract(extract_id):
    extract = SqlExtractMapping.get(extract_id)
    try:
        get_extractor().extract(extract)
    except UnsupportedScheduledExtractError:
        pass


@periodic_task(run_every=5)
def ctable_extract_schedule():
    logger = ctable_extract_schedule.get_logger()
    exps = SqlExtractMapping.schedule()
    exps.append({"id": "43bf0b89f0fe423cc4efc4ab2f9c650a"})
    print exps
    logger.info("ctable_extract_schedule: processing %s extracts" % len(exps))

    for exp in exps:
        process_extract.delay(exp['id'])
