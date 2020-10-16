# data-raven

## Description
A toolbox of flexible database connectors and test methods used to measure data integrity of datasets and database 
tables.
* Build data quality tests which can be inserted into an existing Python script or run as a stand-alone script. 
* Send outcome notifications to messaging and logging applications. 
* Halt pipelines and raise exceptions when needed.

## Prerequisites
Python 3.6+

sqlalchemy>=1.3.19

psycopg2

pymysql

## Installing
`pip install data-raven`

## Build a simple data quality test script
In this example we build a script to test the integrity of three columns in a Postgres table.
```buildoutcfg

```

# Documentation
## Database Support
* Postgres
* MySQL

## Data Quality Tests
Data quality tests are used to measure the integrity of specified columns within a table or document. Every data 
quality test will return `'test_pass'` or `'test_fail'` depending on the given measure and threshold.

### Prebuilt Data Quality Operators
`SQLNullCheckOperator` - Performs a test for each column contained in `columns` which counts the number of 
`NULL` values found. Each test will return `'test_fail'` if the proportion of null values exceeds the specified
threshold. 

`SQLDuplicateCheckOperator` - Performs a test for each column contained in `columns` which counts the number
of duplicate values found in that column. This is equivalent to counting the number of rows returned from a 
`SELECT DISTINCT` on the target column and comparing to the total number of rows. Test will return `'test_fail'`
if the proportion of duplicates exceeds the specified threshold for that column.

`SQLSetDuplicateCheckOperator` - Performs one test which counts the number of duplicates which occur in 
selecting all columns in `columns` simultaneously. This is equivalent to counting the number of rows returned from
a `SELECT DISTINCT` on all columns simultaneously and comparing to the total number of rows. Test will return 
`'test_fail'` if the proportion of duplicates exceedes the specified threshold. Note that for this test, the 
`threshold` parameter should be a literal and not a dictionary, since only one test is being performed.

`CustomSQLDQOperator` -

`CSVNullCheckOperator` - 

`CSVDuplicateCheckOperator` -

`CSVSetDuplicateCheckOperator` - 

### Operator Input Parameter Definitions
* `conn` -
* `dialect` - 
* `from_` - 
* `threshold` - 
* `columns` - 
* `logger` - 
* `where` - 
* `hard_fail` - 
* `use_ansi` - 
* `delimiter` - 
* `fieldnames` -
* `reducer_kwargs` - 
* `custom_test` -
* `description` - 
* `test_desc_kwargs` - 
