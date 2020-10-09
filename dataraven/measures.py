import abc

from sqlalchemy.sql import func, distinct
from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication


class SQLMeasure(object):
    def __init__(self, query, from_, dialect, *columns):
        self.query = query
        self.from_ = from_
        self.columns = columns
        self.dialect = dialect


class SQLMeasureFactory(object):
    def __init__(
            self,
            dialect,
            from_,
            *columns,
            where=None,
            use_ansi=True
    ):
        self.dialect = dialect
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.use_ansi = use_ansi

    def compile_dialect(self, query):
        query_ = compile_to_dialect(query, self.dialect, use_ansi=self.use_ansi)
        return query_

    @abc.abstractmethod
    def build_measure_query(self):
        pass

    def factory(self):
        query = self.build_measure_query()
        dialect_query = self.compile_dialect(query)
        return SQLMeasure(dialect_query, self.from_, self.dialect, *self.columns)


class SQLNullMeasureFactory(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        aggregate_func = func.count
        measure_query = measure_proportion_each_column(
            self.from_,
            aggregate_func,
            *self.columns,
            where_clause=self.where
        )
        return measure_query


class SQLDuplicateMeasureFactory(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

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


class SQLSetDuplicateMeasureFactory(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        measure_query = measure_set_duplication(self.from_, *self.columns, where_clause=self.where)
        return measure_query
