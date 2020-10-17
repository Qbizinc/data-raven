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

## A simple data quality test script
In this example we build a script to test the `name`, `price` and `product_id` columns from the Postgres table `Orders`.
This table has the following DDL:
```buildoutcfg
create table Orders (
id int,
name varchar(50),
order_ts varchar(26),
product_id int,
price float
);
```

Here's the test script.
```buildoutcfg
import os

from dataraven.connections import PostgresConnector
from dataraven.data_quality_operators import SQLNullCheckOperator


def main():
    # initialize logging
    lazy_logger = lambda msg: print(msg)

    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]

    # postgres database connector
    conn = PostgresConnector(user, password, host, dbname, port, logger=lazy_logger)
    dialect = "postgres"

    # test thresholds
    threshold0 = 0
    threshold1 = 0.01
    threshold5 = 0.05

    ##### TEST ORDERS TABLE #####
    # Table to be tested
    from_clause = "test_schema.Orders"

    # Conditional logic to be applied to input data
    date = "2020-09-08"
    where_clause = [f"date(order_ts) = '{date}'"]

    # Columns to be tested in target table
    columns = ("name", "product_id", "price")

    # Threshold value to be applied to each column
    threhold = {"name": threshold1, "product_id": threshold0, "price": threshold5}

    # Hard fail condition set on specific columns
    hard_fail = {"product_id": True}

    # Execute the null check test on each column in the above table
    SQLNullCheckOperator(conn, dialect, from_clause, threhold, *columns, where=where_clause, logger=lazy_logger,
                         hard_fail=hard_fail)


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

### Data Quality Operators
Each operator will log the test results using the function passed in the `logger` parameter. If no logger is found then
these log messages will be swallowed. 

Each operator has a `test_results` attribute which exposes the results from the underlying test. `test_results` is a 
`dict` object with the following structure.
```buildoutcfg
{
    "test_outcomes": {
        COLUMN NAME: {
            "result": 'test_pass' or 'test_fail',
            "measure": THE MEASURED VALUE OF COLUMN NAME 
            "threshold": THE THRESHOLD VALUE SPECIFIED FOR TEST
        
    },
    "result_messages": {
        COLUMN NAME: {
            "result_msg": TEST RESULT MESSAGE,        
            "outcome": 'test_pass' or 'test_fail'
        }
    }
}   



```

`test_results` is a 
`dict` with the following keys:
`test_outcomes` - a `dict` object which maps a column name to it's test outcomes. The test outcomes are a `dict` with 
the following keys:
* `result` - The test result. This is `'test_pass'` or `'test_fail'`.
* `measure` - The calculated measure on the given column.
* `threshold` - The threshold value applied to test.

 
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

`CustomSQLDQOperator` - Executes the test passed by the `custom_test` parameter on each column contained in `columns`. 

`CSVNullCheckOperator` - Tests each column contained in `columns` for `NULL` values. 

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
