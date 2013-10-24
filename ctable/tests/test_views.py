from datetime import datetime
from fakecouch import FakeCouchDb
from ctable.models import SqlExtractMapping
from ctable.tests import TestBase


class TestViews(TestBase):
    def setUp(self):
        self.db = FakeCouchDb()
        SqlExtractMapping._db = self.db

    def test_schedule_daily(self):
        from ctable.models import SCHEDULE_VIEW
        self.db.add_view(SCHEDULE_VIEW, [
            (
                {'key': ['active', 'daily', -1, 8], 'wrap_doc': True}, [{'value': 'daily'}]
            ),
            (
                {'key': ['active', 'weekly', 3, 8], 'wrap_doc': True}, [{'value': 'weekly'}]
            ),
            (
                {'key': ['active', 'monthly', 12, 8], 'wrap_doc': True}, [{'value': 'monthly'}]
            ),
            (
                {'key': ['inactive', 'monthly', 12, 8], 'wrap_doc': True}, [{'value': 'inactive_monthly'}]
            ),
        ])

        ed = datetime(2012, 1, 12, 8, 0, 0)
        exts = SqlExtractMapping.daily_schedule(ed)
        self.assertEquals(exts[0], {'value': 'daily'})

        exts = SqlExtractMapping.weekly_schedule(ed)
        self.assertEquals(exts[0], {'value': 'weekly'})

        exts = SqlExtractMapping.monthly_schedule(ed)
        self.assertEquals(exts[0], {'value': 'monthly'})

        exts = SqlExtractMapping.schedule(ed)
        self.assertEquals(exts, [{'value': 'daily'}, {'value': 'weekly'}, {'value': 'monthly'}])

        exts = SqlExtractMapping.schedule(ed, active=False)
        self.assertEquals(exts[0], {'value': 'inactive_monthly'})
