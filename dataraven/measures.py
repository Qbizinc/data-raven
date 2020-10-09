import abc

from sqlalchemy.sql import func, distinct
from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication


class BaseMeasure(object):
    def __init__(self, from_, *columns, where=None, dialect=None, use_ansi=None):
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.dialect = dialect
        self.use_ansi = use_ansi

        self.measure_func = None
        self.query = None


class BaseSQLMeasure(BaseMeasure):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None,
            use_ansi=True
    ):
        super().__init__(from_clause, *columns, where=where_clause, dialect=dialect, use_ansi=use_ansi)

        measure_query = self.build_measure_query()
        dialect_query = self.compile_dialect(measure_query)
        self.set_query(dialect_query)

    def set_query(self, query):
        self.query = query

    def compile_dialect(self, query):
        query_ = compile_to_dialect(query, self.dialect, use_ansi=self.use_ansi)
        return query_

    @abc.abstractmethod
    def build_measure_query(self):
        pass


class SQLNullMeasure(BaseSQLMeasure):
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
            self.from_,
            aggregate_func,
            *self.columns,
            where_clause=self.where
        )
        return measure_query


class SQLDuplicateMeasure(BaseSQLMeasure):
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
            self.from_,
            aggregate_func,
            *self.columns,
            where_clause=self.where
        )
        return measure_query


class SQLSetDuplicateMeasure(BaseSQLMeasure):
    def __init__(
            self,
            dialect,
            from_clause,
            *columns,
            where_clause=None
    ):
        super().__init__(dialect, from_clause, *columns, where_clause=where_clause)

    def build_measure_query(self):
        measure_query = measure_set_duplication(self.from_, *self.columns, where_clause=self.where)
        return measure_query
