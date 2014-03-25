# The database connection
SQL_REPORTING_DATABASE_URL = "postgresql://user:****:@localhost/ctable"

# Optional user or group name to make owner of created tables.
# If you're connecting with multiple different DB users then you should add them all to a group
# and specify the group here. This will give them all access to the tables created by any of them.
SQL_REPORTING_OBJECT_OWNER = None

# This is a dict of Fluff document types (class names) mapped to CTable backends.
# When ctable receives a fluff diff for one of these types it will process the diff using the
# mapped backend.
# See CTABLE_BACKENDS for list of available backends.
FLUFF_PILLOW_TYPES_TO_SQL = {'MyAwesomeFluff': 'SQL'}

CTABLE_BACKENDS = {'SQL': 'ctable.backends.SqlBackend'}

CTABLE_PREFIX = 'ctable'