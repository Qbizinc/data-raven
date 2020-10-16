import os

from dataraven.connections import PostgresConnector
from dataraven.data_quality_operators import SQLNullCheckOperator, SQLDuplicateCheckOperator


def main():
    # database connection credentials
    user = os.environ["user"]
    password = os.environ["password"]
    host = os.environ["host"]
    dbname = os.environ["dbname"]
    port = os.environ["port"]


    # postgres database connector
    conn = PostgresConnector(user, password, host, dbname, port)
    dialect = "postgres"

    # logging application
    logger = lambda msg: print(msg)

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
    '''contacts_from_clause = "test_schema.Contacts"

    # test first_name-last_name for duplicates
    contacts_duplicats_test_columns = ("first_name", "last_name")
    multi_proportion_duplicate_sql_test(conn, contacts_from_clause, threshold0, *contacts_duplicats_test_columns,
                                               logger=logger)

    # test email, state for null values
    contacts_null_test_columns = ("email", "country")
    proportion_null_sql_test(conn, contacts_from_clause, threshold10, *contacts_null_test_columns, logger=logger)


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
    custom_sql_test(conn, magnitude_bounds_test_description, magnitude_bounds_test_query, logger=logger)

    earthquakes_from_clause = "test_schema.Earthquakes"

    # test state, epicenter, date, magnitude columns for null values
    earthquakes_test_columns = ("state", "epicenter", "date", "magnitude")

    earthquake_null_test_thresholds= {"state": threshold0, "epicenter": threshold5, "date": threshold1,
                                      "magnitude": threshold0}
    proportion_null_sql_test(conn, earthquakes_from_clause, earthquake_null_test_thresholds, *earthquakes_test_columns,
                             logger=logger, hard_fail=True)'''


if __name__ == "__main__":
    main()
