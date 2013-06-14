# The database connection
SQL_REPORTING_DATABASE_URL = "postgresql://user:****:@localhost/ctable"

# This is a list of Fluff document types (class names).
# When ctable receives a fluff diff for one of these types it will process the diff
# and update the appropriate database table.
FLUFF_PILLOW_TYPES_TO_SQL = ['MyAwesomeFluff']