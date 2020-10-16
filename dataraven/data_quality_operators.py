import abc

from .log import get_null_logger
from .tests import CustomTestFactory, SQLNullTest, SQLDuplicateTest, SQLSetDuplicateTest, CSVNullTest, \
    CSVDuplicateTest, CSVSetDuplicateTest

from .operations import SQLOperations, SQLSetOperations, CSVOperations, CSVSetOperations, CustomSQLOperations


class DQOperator(object):
    def __init__(self, logger=None):
        self.logger = logger if logger is not None else get_null_logger().info

    @abc.abstractmethod
    def build_test(self): pass


class SQLDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(logger=logger)
        self.conn = conn
        self.threshold = threshold
        self.dialect = dialect
        self.from_ = from_
        self.columns = columns
        self.where = where
        self.hard_fail = hard_fail
        self.use_ansi = use_ansi

        self.test_results = self.execute()

    @abc.abstractmethod
    def build_test(self): pass

    def execute(self):
        test = self.build_test()
        operator = SQLOperations(self.conn, self.logger, test)
        test_results = operator.execute()
        return test_results


class SQLNullCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(conn, dialect, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLNullTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                           hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class SQLDuplicateCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(conn, dialect, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLDuplicateTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                                hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test


class SQLSetDuplicateCheckOperator(SQLDQOperator):
    def __init__(
            self,
            conn,
            dialect,
            from_,
            threshold,
            *columns,
            logger=None,
            where=None,
            hard_fail=None,
            use_ansi=True
    ):
        super().__init__(conn, dialect, from_, threshold, *columns, logger=logger, where=where, hard_fail=hard_fail,
                         use_ansi=use_ansi)

    def build_test(self):
        test = SQLSetDuplicateTest(self.dialect, self.from_, self.threshold, *self.columns, where=self.where,
                                   hard_fail=self.hard_fail, use_ansi=self.use_ansi).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = SQLSetOperations(self.conn, self.logger, test)
        test_results = operator.execute()
        return test_results


class CSVDQOperator(DQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None,
            **reducer_kwargs
    ):
        super().__init__(logger=logger)
        self.from_ = from_
        self.threshold = threshold
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
        operator = CSVOperations(self.logger, test, fieldnames=self.fieldnames, **self.reducer_kwargs)
        test_results = operator.execute()
        return test_results


class CSVNullCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None

    ):
        super().__init__(from_, logger, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail,
                         fieldnames=fieldnames, logger=logger)

    def build_test(self):
        test = CSVNullTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                           hard_fail=self.hard_fail).factory()
        return test


class CSVDuplicateCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None
    ):
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail, fieldnames=fieldnames,
                         logger=logger)

    def build_test(self):
        test = CSVDuplicateTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                                hard_fail=self.hard_fail).factory()
        return test


class CSVSetDuplicateCheckOperator(CSVDQOperator):
    def __init__(
            self,
            from_,
            threshold,
            *columns,
            delimiter=',',
            hard_fail=None,
            fieldnames=None,
            logger=None
    ):
        super().__init__(from_, threshold, *columns, delimiter=delimiter, hard_fail=hard_fail, fieldnames=fieldnames,
                         logger=logger)

    def build_test(self):
        test = CSVSetDuplicateTest(self.from_, self.threshold, *self.columns, delimiter=self.delimiter,
                                   hard_fail=self.hard_fail).factory()
        return test

    def execute(self):
        test = self.build_test()
        operator = CSVSetOperations(self.logger, test, fieldnames=self.fieldnames, **self.reducer_kwargs)
        test_results = operator.execute()
        return test_results


class CustomSQLDQOperator(DQOperator):
    def __init__(
            self,
            conn,
            custom_test,
            description,
            *columns,
            threshold=None,
            hard_fail=None,
            logger=None,
            **test_desc_kwargs
    ):
        super().__init__(logger=logger)
        self.conn = conn
        self.description = description
        self.custom_test = custom_test
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
        operator = CustomSQLOperations(self.conn, self.logger, test, **self.test_desc_kwargs)
        test_results = operator.execute()
        return test_results
