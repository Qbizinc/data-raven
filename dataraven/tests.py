import abc

from .measures import SQLNullMeasureFactory
from .test_logic import test_predicate_gt


class Test(object):
    def __init__(self, descriptions, measure, threshold, predicate, hard_fail=False):
        self.descriptions = descriptions
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail


class TestFactory(object):
    def __init__(
            self,
            dialect,
            from_,
            threshold,
            *columns,
            where=None,
            predicate=None,
            hard_fail=False,
            use_ansi=None
    ):
        self.dialect = dialect
        self.from_ = from_
        self.threshold = threshold
        self.columns = columns
        self.where = where
        self.predicate = predicate
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi

    def set_test_predicate(self, predicate):
        self.predicate = predicate

    @abc.abstractmethod
    def build_measure(self):
        pass

    @staticmethod
    def format_description(self, description_template, column):
        if isinstance(self.threshold, dict):
            threshold_ = self.threshold[column]
        else:
            threshold_ = self.threshold

        description = description_template.format(column=column, from_clause=self.from_, threshold=threshold_)
        return description

    @abc.abstractmethod
    def build_test_descriptions(self):
        pass

    def factory(self):
        descriptions = self.build_test_descriptions()
        measure = self.build_measure()
        predicate = test_predicate_gt
        return Test(descriptions, measure, self.threshold, predicate, hard_fail=self.hard_fail)


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
        super().__init__(dialect, from_, threshold, *columns, where=where, hard_fail=hard_fail, use_ansi=use_ansi)

    def build_measure(self):
        measure = SQLNullMeasureFactory(self.dialect, self.from_, *self.columns, where=self.where,
                                        use_ansi=self.use_ansi).factory()
        return measure

    def build_test_descriptions(self):
        test_descriptions = {}
        description_template = "{column} in table {from_clause} should have fewer than {threshold} null values."
        for column in self.columns:
            description = self.format_description(description_template, column)
            test_descriptions[column] = description
        return test_descriptions
