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


'''def test_factory(
        type_,
        *columns,
        description,
        measure=None,
        threshold=None,
        predicate=None,
        hard_fail=False,
        custom_test=None
):
    expected_types = ("SQL", "CSV", "CUSTOM_SQL")
    test_type = type_.upper()

    if test_type == "SQL":
        test = SQLTest(description, measure, threshold, predicate, hard_fail=hard_fail)
    elif test_type == "CSV":
        test = CSVTest(description, measure, threshold, predicate, hard_fail=hard_fail)
    elif test_type == "CUSTOM_SQL":
        test = CustomSQLTest(description, custom_test, *columns, threshold=threshold, hard_fail=hard_fail)
    else:
        exp_types = ','.join(expected_types)
        raise ValueError(f"type_ parameter expected to be in {exp_types} but found {test_type}")
    return test'''


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
    def __init__( self, description, custom_test, *columns, threshold=None, hard_fail=False):
        super().__init__(*columns, description=description, custom_test=custom_test, threshold=threshold,
                         hard_fail=hard_fail)

    def factory(self):
        return CustomSQLTest(self.description, self.custom_test, *self.columns, self.threshold, self.hard_fail)


class SQLTestFactory(TestFactory):
    def __init__(self, measure, description, threshold, predicate, hard_fail):
        super().__init__(measure=measure, description=description, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)

    def factory(self):
        return SQLTest(self.description, self.measure, self.threshold, self.predicate, self.hard_fail)


class CSVTestFactory(TestFactory):
    def __init__(self, measure, description, threshold, predicate, hard_fail):
        super().__init__(measure=measure, description=description, threshold=threshold, predicate=predicate,
                         hard_fail=hard_fail)

    def factory(self):
        return CSVTest(self.description, self.measure, self.threshold, self.predicate, self.hard_fail)


def create_sql_null_test(dialect, from_, threshold, *columns, where=None, hard_fail=False, use_ansi=True):
    description = "{column} in table {from_} should have fewer than {threshold} null values."
    measure = SQLNullMeasureFactory(dialect, from_, *columns, where=where, use_ansi=use_ansi).factory()
    predicate = test_predicate_gt
    test = SQLTestFactory(measure, description, threshold, predicate, hard_fail=hard_fail).factory()
    return test


'''class SQLNullTest(TestFactory):
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
        )'''


