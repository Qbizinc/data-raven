import abc

from .measures import SQLNullMeasure, CSVNullMeasure
from .test_logic import test_predicate_gt


class Test(object):
    pass


class SQLTest(Test):
    def __init__(self, description, measure, predicate, threshold, hard_fail=False):
        self.description = description
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail


class CustomSQLTest(Test):
    def __init__(self, description, test, *columns, threshold=None, hard_fail=False):
        self.description = description
        self.test = test
        self.columns = columns
        self.threshold = threshold
        self.hard_fail = hard_fail


class CSVTest(Test):
    def __init__(self, description, measure, predicate, threshold, hard_fail=False):
        self.description = description
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail


class TestFactory(object):
    @abc.abstractmethod
    def build_measure(self): pass

    @abc.abstractmethod
    def factory(self): pass


class CustomTestFactory(TestFactory):
    def __init__( self, description, test, *columns, threshold=None, hard_fail=None):
        self.description = description
        self.test = test
        self.columns = columns
        self.threshold = threshold
        self.hard_fail = hard_fail

    def build_measure(self): pass

    def factory(self):
        return CustomSQLTest(self.description, self.test, *self.columns, threshold=self.threshold,
                             hard_fail=self.hard_fail)


class SQLTestFactory(TestFactory):
    def __init__(self, description, dialect, from_, predicate, threshold, *columns, where=None, hard_fail=False,
                              use_ansi=True):
        self.description = description
        self.threshold = threshold
        self.predicate = predicate
        self.dialect = dialect
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi

    @abc.abstractmethod
    def build_measure(self): pass

    def factory(self):
        measure = self.build_measure()
        return SQLTest(self.description, measure, self.predicate, self.threshold, hard_fail=self.hard_fail)


class SQLNullTest(SQLTestFactory):
    def __init__(self, dialect, from_, threshold, *columns, where=None, hard_fail=False, use_ansi=True):
        description = "{column} in table {from_} should have fewer than {threshold} null values."
        predicate = test_predicate_gt
        super().__init__(description, dialect, from_, predicate, threshold, *columns, where=where, hard_fail=hard_fail,
                              use_ansi=use_ansi)

    def build_measure(self):
        measure = SQLNullMeasure(self.dialect, self.from_, *self.columns, where=self.where, use_ansi=self.use_ansi) \
            .factory()
        return measure


class CSVTestFactory(TestFactory):
    def __init__(self, description, from_, predicate, threshold, *columns, delimiter=',', hard_fail=False):

        self.description = description
        self.threshold = threshold
        self.predicate = predicate
        self.from_ = from_
        self.columns = columns
        self.delimiter = delimiter
        self.hard_fail = hard_fail

    @abc.abstractmethod
    def build_measure(self): pass

    def factory(self):
        measure = self.build_measure()
        return CSVTest(self.description, measure, self.predicate, self.threshold, hard_fail=self.hard_fail)


class CSVNullTest(CSVTestFactory):
    def __init__(self, from_, threshold, *columns, delimiter=',', hard_fail=False):
        description = "{column} in document {from_} should have fewer than {threshold} null values."
        predicate = test_predicate_gt
        super().__init__(description, from_, predicate, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail)

    def build_measure(self):
        measure = CSVNullMeasure(self.from_, *self.columns, delimiter=self.delimiter).factory()
        return measure
