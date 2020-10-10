from .exception_handling import TestFailure


class FetchQueryResults(object):
    def __init__(self, conn, query):
        self.conn = conn
        self.query = query

        response = self.execute_query()
        self.results = self.fetch_results(response)

    def execute_query(self):
        resposne = self.conn.execute(self.query)
        return resposne

    def fetch_results(self, response):
        result = self.conn.fetch(response)[0]
        result_columns = response.keys()
        query_results = dict(zip(result_columns, result))
        return query_results

    def get_results(self):
        return self.results


class BuildTestOutcomes(object):
    def __init__(self, measure_results, test):
        outcomes = self.build_test_outcomes(measure_results, test)
        self.results_msgs = self.format_test_result_msg(outcomes, test)

    @staticmethod
    def build_test_outcomes(measure_results, test):
        test_outcomes = []
        threshold = test.threshold
        predicate = test.predicate

        for column_name in measure_results:
            if isinstance(threshold, dict):
                threshold_ = threshold[column_name]
            else:
                threshold_ = threshold
            measure_value = measure_results[column_name]
            test_result = predicate(measure_value, threshold_)
            test_outcome = {"column": column_name, "result": test_result, "measure": measure_value,
                            "threshold": threshold_}
            test_outcomes.append(test_outcome)
        return test_outcomes

    @staticmethod
    def format_test_result_msg(test_outcomes, test):
        test_result_msgs = []
        descriptions = test.descriptions

        for test_outcome in test_outcomes:
            column = test_outcome["column"]
            description = descriptions[column]
            result = test_outcome["result"]
            measure = test_outcome["measure"]
            threshold = test_outcome["threshold"]

            test_result_msg = f"""
                           Test description: {description}
                           Test outcome: {result}
                           Test measure: {measure}
                           Test threshold: {threshold}
                           """
            test_result_msg = {"result_msg": test_result_msg, "outcome": result, "column": column}
            test_result_msgs.append(test_result_msg)
        return test_result_msgs

    def get_result_msgs(self):
        return self.results_msgs


def log_test_results(test_results, logger):
    for test_result in test_results:
        result_msg = test_result["result_msg"]
        logger(result_msg)
    return True


def raise_execpetion_if_fail_(test_results, test):
    hard_fail = test.hard_fail  # modify to allow for dict hard fail params
    if hard_fail is True:
        for test_result in test_results:
            outcome = test_result["outcome"]
            if outcome == "test_fail":
                result_msg = test_result["result_msg"]
                error_msg = f""" 
                Hard fail condition met.
                {result_msg}
                """
                raise TestFailure(error_msg)
    return True


def raise_execpetion_if_fail(test_results, test):
    hard_fail = test.hard_fail
    if isinstance(hard_fail, dict):
        for test_result in test_results:
            column = test_result["column"]
            hard_fail_ = hard_fail[column]
            if hard_fail_ is True:
                outcome = test_result["outcome"]
                if outcome == "test_fail":
                    result_msg = test_result["result_msg"]
                    error_msg = f""" 
                    Hard fail condition met.
                    {result_msg}
                    """
                    raise TestFailure(error_msg)
    return True


def execute_sql_test(test, conn, logger):
    measure = test.measure
    query = measure.query
    measure_results = FetchQueryResults(conn, query).get_results()
    result_msgs = BuildTestOutcomes(measure_results, test).get_result_msgs()
    log_test_results(result_msgs, logger)
    raise_execpetion_if_fail(result_msgs, test)
    return True
