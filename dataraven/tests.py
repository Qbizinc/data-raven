import abc

from .measures import SQLNullMeasure, CSVNullMeasure
from .test_logic import test_predicate_gt


class Test(object):
    def __init__(self, description=None, measure=None, threshold=None, predicate=None, hard_fail=False,
                 custom_test=None, custom_test_columns=None):
        self.description = description
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail
        self.custom_test = custom_test
        self.custom_test_columns = custom_test_columns


class SQLTest(Test):
    def __init__(self, description, measure, threshold, predicate, hard_fail=False):
        super().__init__(description=description, measure=measure, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)


class CustomSQLTest(Test):
    def __init__(self, description, custom_test, *columns, threshold=None, hard_fail=False):
        super().__init__(
            description=description,
            custom_test=custom_test,
            custom_test_columns=columns,
            threshold=threshold,
            hard_fail=hard_fail
        )


class CSVTest(Test):
    def __init__(self, description, measure, threshold, predicate, hard_fail=False):
        super().__init__(description=description, measure=measure, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)


class TestFactory(object):
    def __init__(
            self,
            *columns,
            dialect=None,
            from_=None,
            where=None,
            description=None,
            threshold=None,
            predicate=None,
            hard_fail=False,
            use_ansi=True,
            custom_test=None,
            delimiter=','
    ):
        self.dialect = dialect
        self.from_ = from_
        self.where = where
        self.description = description
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi
        self.custom_test = custom_test
        self.columns = columns
        self.delimiter = delimiter

    @abc.abstractmethod
    def build_measure(self): pass

    @abc.abstractmethod
    def factory(self): pass


class CustomTestFactory(TestFactory):
    def __init__( self, description, custom_test, *columns, threshold=None, hard_fail=None):
        super().__init__(*columns, description=description, custom_test=custom_test, threshold=threshold,
                         hard_fail=hard_fail)

    def build_measure(self): pass

    def factory(self):
        return CustomSQLTest(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                             hard_fail=self.hard_fail)


class SQLTestFactory(TestFactory):
    def __init__(self, description, threshold, predicate, dialect, from_, *columns, where=None, hard_fail=False,
                 use_ansi=True):
        super().__init__(*columns, description=description, threshold=threshold, predicate=predicate, dialect=dialect,
                         from_=from_, where=where, hard_fail=hard_fail, use_ansi=use_ansi)

    @abc.abstractmethod
    def build_measure(self): pass

    def factory(self):
        measure = self.build_measure()
        return SQLTest(self.description, measure, self.threshold, self.predicate, hard_fail=self.hard_fail)


class SQLNullTest(SQLTestFactory):
    def __init__(self, threshold, dialect, from_, *columns, where=None, hard_fail=False, use_ansi=True):
        description = "{column} in table {from_} should have fewer than {threshold} null values."
        predicate = test_predicate_gt
        super().__init__(description, threshold, predicate, dialect, from_, *columns, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_measure(self):
        measure = SQLNullMeasure(self.dialect, self.from_, *self.columns, where=self.where, use_ansi=self.use_ansi) \
            .factory()
        return measure


class CSVTestFactory(TestFactory):
    def __init__(self, description, threshold, predicate, from_, *columns, delimiter=',', hard_fail=False):
        super().__init__(*columns, description=description, threshold=threshold, predicate=predicate, from_=from_,
                         delimiter=delimiter, hard_fail=hard_fail)

    @abc.abstractmethod
    def build_measure(self): pass

    def factory(self):
        measure = self.build_measure()
        return CSVTest(self.description, measure, self.threshold, self.predicate, hard_fail=self.hard_fail)


class CSVNullTest(CSVTestFactory):
    def __init__(self, threshold, from_, *columns, delimiter=',', hard_fail=False):
        description = "{column} in document {from_} should have fewer than {threshold} null values."
        predicate = test_predicate_gt
        super().__init__(description, threshold, predicate, from_, *columns, delimiter=delimiter, hard_fail=hard_fail)

    def build_measure(self):
        measure = CSVNullMeasure(self.from_, *self.columns, delimiter=self.delimiter).factory()
        return measure
