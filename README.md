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

## Build an example data quality test script
In this example we build a script to test the integrity of three columns in a Postgres table.
```buildoutcfg
import os

from dataraven.connections import PostgresConnector
from dataraven.data_quality_operators import SQLNullCheckOperator, SQLDuplicateCheckOperator, CustomSQLDQOperator,\
    SQLSetDuplicateCheckOperator


def main():
    # initialize logging
    logger = lambda msg: print(msg)

    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]

    # postgres database connector
    conn = PostgresConnector(user, password, host, dbname, port, logger=logger)
    dialect = "postgres"

    # test thresholds
    threshold0 = 0
    threshold1 = 0.01
    threshold5 = 0.05
    threshold10 = 0.1

    ##### TEST ORDERS TABLE #####
    orders_from_clause = "test_schema.Orders"
    orders_where_clause = ["date(order_ts) = '2020-09-08'"]

    # test for duplicates
    orders_duplicates_test_column = "id"
    SQLDuplicateCheckOperator(conn, dialect, orders_from_clause, threshold0, orders_duplicates_test_column,
                              where=orders_where_clause, logger=logger)

    # test multiple columns using one threshold
    orders_null_test_columns = ("name", "product_id", "price")
    SQLNullCheckOperator(conn, dialect, orders_from_clause, threshold0, *orders_null_test_columns,
                         where=orders_where_clause, logger=logger)

    ##### TEST CONTACTS TABLE #####
    contacts_from_clause = "test_schema.Contacts"

    # test first_name-last_name for duplicates
    contacts_duplicats_test_columns = ("first_name", "last_name")
    SQLSetDuplicateCheckOperator(conn, dialect, contacts_from_clause, threshold0, *contacts_duplicats_test_columns,
                                 logger=logger)


    ##### TEST EARTHQUAKES TABLE #####
    # test columns for blank values
    earthquakes_columns = ("state", "epicenter", "date", "magnitude")
    earthquake_null_thresholds = {"state": threshold0, "epicenter": threshold5, "date": threshold1,
                                  "magnitude": threshold0}
    earthquake_col_not_blank_description = "{column} in table test_schema.Earthquakes should have fewer than {threshold} BLANK values."
    earthquake_col_not_blank_query = """
    select      
    case
        when measure is NULL then 'test_fail'
        when measure > {threshold} then 'test_fail'
        else 'test_pass'
    end as result,
    measure,
    {threshold} as threshold
    from
    (select 
    case when rows_ > 0 then cast(blank_cnt as float) / rows_ end as measure
    from
    (select 
    count(1) as rows_,
    sum(case when cast({column} as varchar) = '' then 1 else 0 end) as blank_cnt
    from test_schema.Earthquakes)t)tt
    """
    CustomSQLDQOperator(
        conn,
        earthquake_col_not_blank_query,
        earthquake_col_not_blank_description,
        *earthquakes_columns,
        threshold=earthquake_null_thresholds,
        logger=logger
    )


if __name__ == "__main__":
    main()
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
