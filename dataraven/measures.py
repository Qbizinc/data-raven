import abc

from sqlalchemy.sql import func, distinct
from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication


class SQLMeasure(object):
    def __init__(self, *columns, query=None, from_=None, dialect=None):
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
            #dialect=None,
            #from_=None,
            where=None,
            use_ansi=True
    ):
        self.dialect = dialect
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.use_ansi = use_ansi

    @staticmethod
    def compile_dialect(query, dialect, use_ansi):
        query_ = compile_to_dialect(query, dialect, use_ansi=use_ansi)
        return query_

    @abc.abstractmethod
    def build_measure_query(self):
        pass

    def factory(self):
        query = self.build_measure_query()
        query_ = self.compile_dialect(query, self.dialect, self.use_ansi)
        return SQLMeasure(*self.columns, query=query_, from_=self.from_, dialect=self.dialect)


class SQLNullMeasureFactory(SQLMeasureFactory):
    def __init__(self, dialect, from_, *columns, where=None, use_ansi=True):
        #super().__init__(*columns, dialect=dialect, from_=from_, where=where, use_ansi=use_ansi)
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
        #super().__init__(*columns, dialect=dialect, from_=from_, where=where, use_ansi=use_ansi)
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
        #super().__init__(*columns, dialect=dialect, from_=from_, where=where, use_ansi=use_ansi)
        super().__init__(dialect, from_, *columns, where=where, use_ansi=use_ansi)

    def build_measure_query(self):
        measure_query = measure_set_duplication(self.from_, *self.columns, where_clause=self.where)
        return measure_query

