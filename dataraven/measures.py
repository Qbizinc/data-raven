import abc

from sqlalchemy.sql import func, distinct

from .sql.helpers import compile_to_dialect
from .sql.measure_logic import measure_proportion_each_column, measure_set_duplication

from .csv.reducers import measure_null


class Measure(object):
    pass


class SQLMeasure(Measure):
    def __init__(self, *columns, query, from_, dialect):
        self.columns = columns
        self.query = query
        self.from_ = from_
        self.dialect = dialect


class CSVMeasure(Measure):
    def __init__(self, *columns, path, reducer, filter, delimiter):
        self.columns = columns
        self.path = path
        self.reducer = reducer
        self.filter = filter
        self.delimiter = delimiter


class MeasureFactory(object):
    @abc.abstractmethod
    def factory(self): pass


class SQLMeasureFactory(MeasureFactory):
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


class CSVMeasureFactory(MeasureFactory):
    def __init__(self, path, *columns, filter=None, delimiter=','):
        self.path = path
        self.columns = columns
        self.filter = filter
        self.delimiter = delimiter

    @abc.abstractmethod
    def build_reducer(self):
        pass

    def factory(self):
        reducer = self.build_reducer()
        return CSVMeasure(*self.columns, self.path, reducer, self.filter, self.delimiter)


class CSVNullMeasure(CSVMeasureFactory):
    def __init__(self, path, *columns, filter=None, delimiter=','):
        super().__init__(path, *columns, filter=filter, delimiter=delimiter)

    def build_reducer(self):
        return measure_null
