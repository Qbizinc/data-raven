from sqlalchemy.sql import func, distinct

from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication


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
