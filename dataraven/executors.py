from .query_handling import QueryHandler
from .exception_handling import TestFailure


class BaseExecutor(object):
    def __init__(self, source, test, logger):
        self.source = source
        self.test = test
        self.logger = logger

    def fetch_measure_data(self):
        pass

    def calculate_test_outcome(self, measure_results):
        test_outcomes = []
        threshold = self.test.threshold
        predicate = self.test.predicate

        for column_name in measure_results:
            if isinstance(threshold, dict):
                threshold_ = threshold[column_name]
            else:
                threshold_ = threshold
            measure = measure_results[column_name]
            test_result = predicate(measure, threshold_)
            test_outcome = {"column": column_name, "result": test_result, "measure": measure, "threshold": threshold_}
            test_outcomes.append(test_outcome)
        return test_outcomes

    def parse_test_results(self, test_outcomes):
        """ return the formatted test description, test_result_msg"""
        test_result_msgs = []
        test_description = self.test.description
        measure = self.test.measure
        data_source = measure.from_clause  # this is intended to be general so needs to be abstracted from from_clause

        for test_outcome in test_outcomes:
            column_name = test_outcome["column"]
            result = test_outcome["result"]
            measure = test_outcome["measure"]
            threshold = test_outcome["threshold"]
            # need to add documentation around expected test description formatting
            test_description_ = test_description.format(data_source=data_source, column=column_name,
                                                        threshold=threshold)
            test_result_msg = f"""
                    Test description: {test_description_}
                    Test outcome: {result}
                    Test measure: {measure}
                    Test threshold: {threshold}
                    """
            test_result_msg = {"result_msg": test_result_msg, "test_outcome": result}
            test_result_msgs.append(test_result_msg)
        return test_result_msgs

    def log_test_results(self, test_result_msgs):
        for test_result_msg in test_result_msgs:
            result_msg = test_result_msg["result_msg"]
            self.logger(result_msg)
        return True

    def raise_execpetion_if_fail(self, test_result_msgs):
        hard_fail = self.test.hard_fail
        if hard_fail:
            for test_result_msg in test_result_msgs:
                result = test_result_msg["result"]
                if result == "test_fail":
                    result_msg = test_result_msg["result_msg"]
                    fail_msg = f"""
                    Hard fail test condition met. 
                    {result_msg}
                    """
                    raise TestFailure(fail_msg)
        return True


class SQLExecutor(object):
    def __init__(self, db_connection, test, logger):
        self.conn = db_connection
        self.test = test
        self.logger = logger

    def fetch_measure_data(self):
        measure = self.test.measure
        measure_query = measure.query
        handler = QueryHandler(self.conn, measure_query)
        measure_results = handler.get_results()
        return measure_results





