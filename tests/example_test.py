import os
import logging

from dataraven.connections import PostgresConnector
from dataraven.data_quality_operators import SQLNullCheckOperator, SQLDuplicateCheckOperator, CustomSQLDQOperator,\
    SQLSetDuplicateCheckOperator


logging.basicConfig(filename="test.log", level=logging.DEBUG,
                    format="%(asctime)s | %(name)s | %(levelname)s | \n%(message)s\n")


def main():
    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]


    # postgres database connector
    error_logger = logging.error
    conn = PostgresConnector(user, password, host, dbname, port, logger=error_logger)
    dialect = "postgres"

    # logging application
    logger = logging.info

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
    SQLDuplicateCheckOperator(conn, dialect, orders_from_clause, logger, threshold0, orders_duplicates_test_column,
                              where=orders_where_clause)

    # test multiple columns using one threshold
    orders_null_test_columns = ("name", "product_id", "price")
    SQLNullCheckOperator(conn, dialect, orders_from_clause, logger, threshold0, *orders_null_test_columns,
                         where=orders_where_clause)

    ##### TEST CONTACTS TABLE #####
    contacts_from_clause = "test_schema.Contacts"

    # test first_name-last_name for duplicates
    contacts_duplicats_test_columns = ("first_name", "last_name")
    SQLSetDuplicateCheckOperator(conn, dialect, contacts_from_clause, logger, threshold0,
                                 *contacts_duplicats_test_columns)

    # test email, state for null values
    contacts_null_columns = ("email", "country")
    contacts_null_threshold = {"email": threshold10, "country": 0.5}
    SQLNullCheckOperator(conn, dialect, contacts_from_clause, logger, contacts_null_threshold, *contacts_null_columns)


    ##### TEST EARTHQUAKES TABLE #####
    # test magnitude is bounded above at 10
    magnitude_bounds_test_description = "Earthquakes.magnitude should be less than 10"
    magnitude_bounds_test_query = """
        select
            case
                when measure > 0 then 'test_fail'
                else 'test_pass'
            end as result,
            measure,
            0 as threshold
        from
        (select count(1) as measure
        from test_schema.Earthquakes
        where magnitude > 10)t
        """
    CustomSQLDQOperator(conn, magnitude_bounds_test_query, magnitude_bounds_test_description, logger)

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
        logger,
        *earthquakes_columns,
        threshold=earthquake_null_thresholds
    )






if __name__ == "__main__":
    main()
