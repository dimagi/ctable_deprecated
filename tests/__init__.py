import sqlalchemy
import logging
from unittest2 import TestCase
from fakecouch import FakeCouchDb
from mock import patch

logging.basicConfig()

TEST_DB_URL = 'postgresql://postgres:@localhost/ctable_test'
engine = sqlalchemy.create_engine(TEST_DB_URL)


class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        from django.conf import settings
        if not settings.configured:
            settings.configure(DEBUG=True, SQL_REPORTING_DATABASE_URL=TEST_DB_URL)

        cls.p1 = patch('couchdbkit.ext.django.schema.get_db', return_value=FakeCouchDb())
        cls.p1.start()
        from ctable.models import KeyMatcher
        from ctable.base import CtableExtractor, SqlExtractMapping, ColumnDef, fluff_view
        cls.CtableExtractor = CtableExtractor
        cls.SqlExtractMapping = SqlExtractMapping
        cls.ColumnDef = ColumnDef
        cls.fluff_view = fluff_view
        cls.KeyMatcher = KeyMatcher

    @classmethod
    def tearDownClass(cls):
        cls.p1.stop()