# The database connection
SQL_REPORTING_DATABASE_URL = "postgresql://user:****:@localhost/ctable"

# This is a dict of Fluff document types (class names) mapped to CTable backends.
# When ctable receives a fluff diff for one of these types it will process the diff using the
# mapped backend.
# See CTABLE_BACKENDS for list of available backends.
FLUFF_PILLOW_TYPES_TO_CTABLE = {'MyAwesomeFluff': 'SQL'}

CTABLE_BACKENDS = {'SQL': 'ctable.backends.SqlBackend'}