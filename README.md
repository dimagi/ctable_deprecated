# ctable

[![Build Status](https://travis-ci.org/dimagi/ctable.png)](https://travis-ci.org/dimagi/ctable)
[![Test coverage](https://coveralls.io/repos/dimagi/ctable/badge.png?branch=master)](https://coveralls.io/r/dimagi/ctable)

Basic ETL tool for extracting data from a CouchDB view to an SQL table.

Assume you have a data emitted fron a CouchDB view as follows:

```
{"rows":[
    {"key":["user1","indicator_a","2013-03-05T12:00:00.000Z"],"value":{"sum":3,"count":3,"min":1,"max":1,"sumsqr":3}},
    {"key":["user1","indicator_b","2013-03-05T12:00:00.000Z"],"value":{"sum":2,"count":2,"min":1,"max":1,"sumsqr":2}}
    {"key":["user2","indicator_a","2013-03-06T12:00:00.000Z"],"value":{"sum":5,"count":2,"min":1,"max":4,"sumsqr":2}}
]}
```

This can be extracted into an SQL table of the following form:

| user  |    date    | indicator_a | indicator_b |
|-------|------------|-------------|-------------|
| user1 | 2013-03-05 |      3      |      2      |
| user2 | 2013-03-06 |      5      |      -      |

To do this we need to create an extract configuration which defines each column in the SQL table and where the
data for that table comes from.

## Usage
```python
# create extract configuration
extract = SqlExtractMapping(
    domains=['test'], 
    name='demo', 
    couch_view='my/view',
    columns=[
        ColumnDef(
            name="user",
            data_type="string",
            max_length=50,
            value_source="key",
            value_index=0)
        ColumnDef(
            name="date",
            data_type="date",
            date_format="%Y-%m-%dT%H:%M:%S.%fZ",
            value_source="key",
            value_index=2),
        ColumnDef(
            name="rename_indicator_a",
            data_type="integer",
            value_source="value",
            value_attribute="sum",
            match_keys=[KeyMatcher(index=1, value="indicator_a")]),
        ColumnDef(
            name="indicator_b",
            data_type="integer",
            value_source="value",
            value_attribute="sum",
            match_keys=[KeyMatcher(index=1, value="indicator_b")]),
    ])

# perform extraction
ex = CtableExtractor(couchdb, SqlBackend())
ex.extract(extract)
```

## Extract mapping
Fields:

* backend
  * Which backend to use to save the data.
  * Currently only 'SQL' is supported.
* database
  * Optional to override defatul CouchDB Database
* domains
  * List of domains that this mapping belongs to.
  * Used for grouping in the views and preventing duplicate names.
* name
 * Mapping name
 * Allowed characters: a-z, A-Z, 0-9, '_'
* columns
  * List of mapping columns
* active
  * Boolean value determining if the schedule for this mapping is active
* auto_generated
  * True if created by code
  * Prevents editing via the UI
* schedule_type
  * options - 'daily', 'weekly', 'monthly'
  * Defaults to 'daily'
* schedule_hour
  * The hour of the day when the schedule should execute.
* schedule_day
  * Only applicable to 'weekly' and 'monthly' schedules.
  * Day of week for weekly
  * Day of month for monthly
  * -1 for daily
* couch_view
  * The name of the CouchDB view to query
* couch_key_prefix
  * List of strings to prefix the CouchDB query key with
* couch_group_level
  * Group level parameter for CouchDB query. Leave blank for 'exact' grouping.
* couch_date_range
  * Number of days in the past to query data for.
  * This requires that the first element in the view key after the key prefix is a date.
* couch_date_format
  * Date format to use when couch_date_range > 0
  * Default = '%Y-%m-%dT%H:%M:%S.%fZ'

## Columns
Fields:
* name
* data_type
  * options = string, integer, date, datetime
* null_value_placeholder
  * Value to use when column value is null.
  * Only applicable to columns with no match_keys.
  * Defaults:
    * `'__none__'` for string columns
    * [`161803398875`](http://en.wikipedia.org/wiki/Golden_ratio) for integer columns
    * `date.min` for date columns
    * `datetime.min` for datetime columns
* value_source
  * options = 'key', 'value'
* value_index
  * numeric index used to extract the value from the value_source e.g. key[1]
  * compulsory for columns where `value_source = 'key'`
* value_attribute
  * attribute key used to extract the value from the value_source e.g. value["sum"]
  * only available when `value_source = 'value'`
* max_length
  * only for string columns
  * defaults to 255
* date_format
  * only for date / datetime columns
* match_keys
  * List of KeyMatcher objects
  * used to determine when this column is relevant e.g. rows where key[1] = 'indicator_a'
