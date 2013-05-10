# ctable [![Build Status](https://travis-ci.org/dimagi/ctable.png)](https://travis-ci.org/dimagi/ctable)

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

## Columns
Fields:
* name
* data_type (string, integer, date, datetime)
* value_source (key, value)
* value_index
  * numeric index used to extract the value from the value_source e.g. key[1]
* value_attribute
  * attribute key used to extract the value from the value_source e.g. value["sum"]
* max_length (only for string columns)
* data_format (only for date / datetime columns)
* match_key
  * used to determine when this column is relevant e.g. rows where key[1] = 'indicator_a'
  * must contain and _index_ key
  * may contain a _value_ key
  * if no _value_ key is supplied it is assumed that the value = column.name
  * key[index] = value or column.name

```python
ColumnDef(
    name="user",
    data_type="string",
    max_length=50,
    value_source="key",
    value_index=0)
ColumnDef(
    name="date",
    data_type="date",
    data_format="%Y-%m-%dT%H:%M:%S.%fZ",
    value_source="key",
    value_index=2),
ColumnDef(
    name="rename_indicator_a",
    data_type="integer",
    value_source="value",
    value_attribute="sum",
    match_key={"index": 1, "value": "indicator_a"}),
ColumnDef(
    name="indicator_b",
    data_type="integer",
    value_source="value",
    value_attribute="sum",
    match_key={"index": 1}),
```

## Usage
```python
# create extract configuration
extract = SqlExtractMapping(columns=[...])

# perform extraction
ex = CtableExtractor(sql_connection_or_url, couchdb, extract)
ex.extract("my_table", "my_couch_db/view")
```
