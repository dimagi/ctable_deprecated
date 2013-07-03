import sqlalchemy
import logging
from unittest2 import TestCase
from fakecouch import FakeCouchDb
from mock import patch
from django.conf import settings

logging.basicConfig()

TEST_DB_URL = 'postgresql://postgres:@localhost/ctable_test'
engine = sqlalchemy.create_engine(TEST_DB_URL)


class TestBase(TestCase):
    @classmethod
    def setUpClass(cls):
        if not settings.configured:
            settings.configure(DEBUG=True,
                               SQL_REPORTING_DATABASE_URL=TEST_DB_URL,
                               SQL_REPORTING_OBJECT_OWNER=None,
                               COUCH_STALE_QUERY=False)

        cls.db = FakeCouchDb()
        cls.p1 = patch('couchdbkit.ext.django.schema.get_db', return_value=cls.db)
        cls.p1.start()
        from ctable.models import KeyMatcher
        from ctable.base import CtableExtractor, SqlExtractMapping, ColumnDef, fluff_view
        from ctable import util
        cls.CtableExtractor = CtableExtractor
        cls.SqlExtractMapping = SqlExtractMapping
        cls.ColumnDef = ColumnDef
        cls.fluff_view = fluff_view
        cls.KeyMatcher = KeyMatcher
        cls.util = util

    @classmethod
    def tearDownClass(cls):
        cls.p1.stop()