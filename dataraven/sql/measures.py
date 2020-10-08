import sqlalchemy.sql.expression as sql
from sqlalchemy.sql import func, distinct
from sqlalchemy import text, Float

from .core import build_select_query, build_aggregate_query
from .helpers import compile_to_dialect


def measure_proportion_each_column(from_clause, aggregate_func, *columns, where_clause=None):
    """
    :param from_clause: used as query from clause; expected sqlalchemy TextClause, Select, Alias or Join object
    :param aggregate_func: the sql aggregation function
    :param columns: column names to be selected; expected str, TextClause or ColumnClause
    :param where_clause: predicate to apply to where clause in query; expected str, TextClause, BinaryExpression
    :return: sqlalchemy.sql.selectable.Select object
    """
    count_rows_aggregate = {"1": func.count}
    column_aggregates = {column: aggregate_func for column in columns}
    aggregates = {**count_rows_aggregate, **column_aggregates}

    aggregate_query = build_aggregate_query(from_clause, where_clause=where_clause, **aggregates)
    aggregate_query = aggregate_query.alias('t')
    aggregate_query_columns = list(aggregate_query.columns)
    rows_column = aggregate_query_columns[0]
    null_cnt_columns = aggregate_query_columns[1:]

    case_clauses = []
    for i in range(len(columns)):
        column = null_cnt_columns[i]
        column_label = columns[i]
        case_statement = sql.case(
            [
                (
                    rows_column > text("0"),
                    text("1") - sql.cast(column, Float()) / rows_column
                )
            ],
            else_=None
        ).label(column_label)
        case_clauses.append(case_statement)

    measure_query = sql.select(case_clauses).select_from(aggregate_query)
    return measure_query


def measure_set_duplication(from_clause, *columns, where_clause=None):
    """
    :param from_clause: used as query from clause; expected sqlalchemy TextClause, Select, Alias or Join object
    :param columns: column names to be selected; expected str, TextClause or ColumnClause
    :param where_clause: predicate to apply to where clause in query; expected str, TextClause, BinaryExpression
    :return: sqlalchemy.sql.selectable.Select object
    """
    count_rows_aggregate = {"1": func.count}

    rows_query = build_aggregate_query(from_clause, where_clause=where_clause, **count_rows_aggregate)
    rows_query = rows_query.alias('r')
    rows_column = list(rows_query.columns)[0]

    distinct_query = build_select_query(from_clause, *columns, where_clause=where_clause, select_distinct=True)
    distinct_query = distinct_query.alias('t')

    uniques_query = build_aggregate_query(distinct_query, **count_rows_aggregate)
    uniques_query = uniques_query.alias('u')
    uniques_column = list(uniques_query.columns)[0]

    join_clause = sql.join(rows_query, uniques_query, onclause=rows_column != None)

    measure_query = sql.select(
        [
            sql.case(
                [
                    (
                        rows_column > text("0"),
                        text("1") - sql.cast(uniques_column, Float()) / rows_column
                    )
                ],
                else_=None
            ).label("measure")
        ]
    ).select_from(join_clause)
    return measure_query


class BaseSQLMeasure(object):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None,
            use_ansi=True
    ):
        self.dialect = dialect
        self.from_clause = from_clause
        self.columns = columns
        self.where_clause = where_clause
        self.query = None

        measure_query = self.build_measure_query()
        dialect_query = self.compile_dialect(measure_query, use_ansi)
        self.set_query(dialect_query)

    def set_query(self, query):
        self.query = query

    def compile_dialect(self, query, use_ansi=True):
        query_ = compile_to_dialect(query, self.dialect, use_ansi=use_ansi)
        return query_

    def build_measure_query(self):
        pass


class NullMeasure(BaseSQLMeasure):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None
    ):
        super().__init__(dialect, from_clause, *columns, where_clause=where_clause)

    def build_measure_query(self):
        aggregate_func = func.count
        measure_query = measure_proportion_each_column(
            self.from_clause,
            aggregate_func,
            *self.columns,
            where_clause=self.where_clause
        )
        return measure_query


class DuplicateMeasure(BaseSQLMeasure):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None
    ):
        super().__init__(dialect, from_clause, *columns, where_clause=where_clause)

    def build_measure_query(self):
        def aggregate_func(column):
            return func.count(distinct(column))

        measure_query = measure_proportion_each_column(
            self.from_clause,
            aggregate_func,
            *self.columns,
            where_clause=self.where_clause
        )
        return measure_query


class SetDuplicateMeasure(BaseSQLMeasure):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None
    ):
        super().__init__(dialect, from_clause, *columns, where_clause=where_clause)

    def build_measure_query(self):
        measure_query = measure_set_duplication(self.from_clause, *self.columns, where_clause=self.where_clause)
        return measure_query
