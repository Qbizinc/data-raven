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
            description=None,
            measure=None,
            threshold=None,
            predicate=None,
            hard_fail=False,
            custom_test=None
    ):
        self.description = description
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail
        self.custom_test = custom_test
        self.columns = columns

    @abc.abstractmethod
    def factory(self): pass


class CustomTestFactory(TestFactory):
    def __init__( self, description, custom_test, *columns, threshold=None, hard_fail=None):
        super().__init__(*columns, description=description, custom_test=custom_test, threshold=threshold,
                         hard_fail=hard_fail)

    def factory(self):
        return CustomSQLTest(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                             hard_fail=self.hard_fail)


class SQLTestFactory(TestFactory):
    def __init__(self, measure, description, threshold, predicate, hard_fail=False):
        super().__init__(measure=measure, description=description, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)

    def factory(self):
        return SQLTest(self.description, self.measure, self.threshold, self.predicate, hard_fail=self.hard_fail)


class CSVTestFactory(TestFactory):
    def __init__(self, measure, description, threshold, predicate, hard_fail=False):
        super().__init__(measure=measure, description=description, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)

    def factory(self):
        return CSVTest(self.description, self.measure, self.threshold, self.predicate, hard_fail=self.hard_fail)


def create_sql_null_test(dialect, from_, threshold, *columns, where=None, hard_fail=False, use_ansi=True):
    description = "{column} in table {from_} should have fewer than {threshold} null values."
    measure = SQLNullMeasure(dialect, from_, *columns, where=where, use_ansi=use_ansi).factory()
    predicate = test_predicate_gt
    test = SQLTestFactory(measure, description, threshold, predicate, hard_fail=hard_fail).factory()
    return test


def create_csv_null_test(path, threshold, *columns, delimiter=',', hard_fail=False):
    description = "{column} in document {from_} should have fewer than {threshold} null values."
    measure = CSVNullMeasure(path, *columns, delimiter=delimiter).factory()
    predicate = test_predicate_gt
    test = CSVTestFactory(measure, description, threshold, predicate, hard_fail=hard_fail).factory()
    return test


def create_custom_sql_test(description, test_query, *columns, threshold=None, hard_fail=None):
    test = CustomTestFactory(description, test_query, *columns, threshold=threshold, hard_fail=hard_fail).factory()
    return test