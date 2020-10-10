import abc

from .measures import SQLNullMeasureFactory
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


class CustomTest(Test):
    def __init__(self, description, custom_test, *columns, threshold=None, hard_fail=False):
        super().__init__(
            description=description,
            custom_test=custom_test,
            custom_test_columns=columns,
            threshold=threshold,
            hard_fail=hard_fail
        )


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

    def factory(self):
        if self.custom_test is not None:
            test = CustomTest(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                              hard_fail=self.hard_fail)
        else:
            test = Test(description=self.description, measure=self.measure, threshold=self.threshold,
                        predicate=self.predicate, hard_fail=self.hard_fail)
        return test


class SQLNullTestFactory(TestFactory):
    def __init__(
            self,
            dialect,
            from_,
            threshold,
            *columns,
            where=None,
            hard_fail=False,
            use_ansi=True
    ):
        description = "{column} in table {from_} should have fewer than {threshold} null values."
        measure = SQLNullMeasureFactory(dialect, from_, *columns, where=where, use_ansi=use_ansi).factory()
        predicate = test_predicate_gt
        super().__init__(
            description=description,
            measure=measure,
            threshold=threshold,
            predicate=predicate,
            hard_fail=hard_fail
        )


class CustomTestFactory(TestFactory):
    def __init__( self, description, custom_test, *columns, threshold=None, hard_fail=False):
        super().__init__(*columns, description=description, custom_test=custom_test, threshold=threshold,
                         hard_fail=hard_fail)
