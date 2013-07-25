from __future__ import absolute_import
from django.test import TestCase

import sqlalchemy
import logging
from fakecouch import FakeCouchDb
from mock import patch
from django.conf import settings

logging.basicConfig()


class TestBase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = sqlalchemy.create_engine(settings.SQL_REPORTING_DATABASE_URL)
        cls.db = FakeCouchDb()
        cls.p1 = patch('couchdbkit.ext.django.schema.get_db', return_value=cls.db)
        cls.p1.start()

    @classmethod
    def tearDownClass(cls):
        cls.p1.stop()

try:
    from ctable.tests.test_backends import *
    from ctable.tests.test_extract import *
    from ctable.tests.test_models import *
    from ctable.tests.test_signals import *
    from ctable.tests.test_util import *
    from ctable.tests.test_views import *
except ImportError, e:
    # for some reason the test harness squashes these so log them here for clarity
    # otherwise debugging is a pain
    logging.exception(e)
    raise
