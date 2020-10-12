import abc

from .tests import CustomTestFactory, SQLNullTest, CSVNullTest
from .operations import SQLOperator, CSVOperator, CustomOperator


class DQOperator(object):
    @abc.abstractmethod
    def build_test(self): pass


class SQLDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            threshold,
            logger,
            *columns,
            where=None,
            hard_fail=None,
            use_ansi=True,
            **test_desc_kwargs
    ):
        self.conn = conn
        self.threshold = threshold
        self.dialect = dialect
        self.from_ = from_
        self.logger = logger
        self.columns = columns
        self.where = where
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi
        self.test_desc_kwargs = test_desc_kwargs

        self.result_msgs = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = SQLOperator(test, self.logger, self.conn, **self.test_desc_kwargs)
        result_msgs = operator.execute()
        return result_msgs


class SQLNullCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            threshold,
            dialect,
            from_,
            logger,
            *columns,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(conn, dialect, from_, threshold, logger, *columns, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLNullTest(self.threshold, self.dialect, self.from_, *self.columns, where=self.where,
                           hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class CSVDQOperator(DQOperator):
    def __init__(
            self,
            from_,
            threshold,
            logger,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            **test_desc_kwargs
    ):
        self.from_ = from_
        self.threshold = threshold
        self.logger = logger
        self.columns = columns
        self.delimiter = delimiter
        self.hard_fail = hard_fail
        self.fieldnames = fieldnames
        self.test_desc_kwargs = test_desc_kwargs

        self.result_msgs = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = CSVOperator(test, self.logger, self.fieldnames, **self.test_desc_kwargs)
        result_msgs = operator.execute()
        return result_msgs


class CSVNullCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None
    ):
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail)

    def build_test(self):
        test = CSVNullTest(self.threshold, self.from_, *self.columns, delimiter=self.delimiter,
                           hard_fail=self.hard_fail).factory()
        return test


class CustomDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            description,
            custom_test,
            logger,
            *columns,
            threshold=None,
            hard_fail=None,
            **test_desc_kwargs
    ):
        self.conn = conn
        self.description = description
        self.custom_test = custom_test
        self.logger = logger
        self.columns = columns
        self.threshold = threshold
        self.hard_fail = hard_fail
        self.test_desc_kwargs = test_desc_kwargs

        self.result_msgs = self.execute()

    def build_test(self):
        test = CustomTestFactory(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                                 hard_fail=self.hard_fail).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = CustomOperator(test, self.logger, self.conn, **self.test_desc_kwargs)
        results_msgs = operator.execute()
        return results_msgs

