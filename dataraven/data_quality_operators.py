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
            logger,
            threshold,
            *columns,
            where=None,
            hard_fail=None,
            use_ansi=True
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

        self.test_results = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = SQLOperator(self.conn, self.logger, test)
        test_results = operator.execute()
        return test_results


class SQLNullCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            logger,
            threshold,
            *columns,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(conn, dialect, from_, logger, threshold, *columns, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLNullTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                           hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class CSVDQOperator(DQOperator):
    def __init__(
            self,
            from_,
            logger,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            **reducer_kwargs
    ):
        self.from_ = from_
        self.threshold = threshold
        self.logger = logger
        self.columns = columns
        self.delimiter = delimiter
        self.hard_fail = hard_fail
        self.fieldnames = fieldnames
        self.reducer_kwargs = reducer_kwargs

        self.test_results = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = CSVOperator(self.logger, test , fieldnames=self.fieldnames, **self.reducer_kwargs)
        test_results = operator.execute()
        return test_results


class CSVNullCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            logger,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None
    ):
        super().__init__(from_, logger, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail,
                         fieldnames=fieldnames)

    def build_test(self):
        test = CSVNullTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                           hard_fail=self.hard_fail).factory()
        return test


class CustomDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            custom_test,
            description,
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

        self.test_results = self.execute()

    def build_test(self):
        test = CustomTestFactory(self.description, self.custom_test, *self.columns, threshold=self.threshold,
                                 hard_fail=self.hard_fail).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = CustomOperator(self.conn, self.logger, test, **self.test_desc_kwargs)
        test_results = operator.execute()
        return test_results
