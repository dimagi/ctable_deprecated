from settings import *

SQL_REPORTING_DATABASE_URL = "postgresql://postgres::@localhost/ctable_test"

# other django settings
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    },
}

INSTALLED_APPS = (
    'couchdbkit.ext.django',
    'ctable',
)

SECRET_KEY = '123'

COUCH_DATABASE = "http://localhost:5984/ctable"
COUCHDB_DATABASES = [
    ('ctable', COUCH_DATABASE),
]