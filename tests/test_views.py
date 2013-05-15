from django.conf import settings
if not settings.configured:
    settings.configure(DEBUG=True)

from unittest2 import TestCase
from datetime import datetime
from ctable import SqlExtractMapping
from ctable.models import SCHEDULE_VIEW
from couchdbkit.ext.django.loading import couchdbkit_handler
from fakecouch import FakeCouchDb


class TestViews(TestCase):
    def setUp(self):
        self.db = FakeCouchDb()
        SqlExtractMapping._db = self.db
        couchdbkit_handler._databases = {'fluff': self.db}

    def test_schedule_daily(self):
        self.db.add_view(SCHEDULE_VIEW, [
            (
                {'key': ['active', 'daily', -1, 8]}, ['daily']
            ),
            (
                {'key': ['active', 'weekly', 3, 8]}, ['weekly']
            ),
            (
                {'key': ['active', 'monthly', 12, 8]}, ['monthly']
            ),
            (
                {'key': ['inactive', 'monthly', 12, 8]}, ['inactive_monthly']
            ),
        ])

        ed = datetime(2012, 1, 12, 8, 0, 0)
        exts = SqlExtractMapping.daily_schedule(ed)
        self.assertEquals(exts, ['daily'])

        exts = SqlExtractMapping.weekly_schedule(ed)
        self.assertEquals(exts, ['weekly'])

        exts = SqlExtractMapping.monthly_schedule(ed)
        self.assertEquals(exts, ['monthly'])

        exts = SqlExtractMapping.schedule(ed)
        self.assertEquals(exts, ['daily', 'weekly', 'monthly'])

        exts = SqlExtractMapping.schedule(ed, active=False)
        self.assertEquals(exts, ['inactive_monthly'])
