

class BaseTest(object):
    def __init__(self, description, measure, threshold, predicate, hard_fail=False):
        self.description = description
        self.measure = measure
        self.threshold = threshold
        self.predicate = predicate
        self.hard_fail = hard_fail


class SQLTest(BaseTest):
    def __init__(self, description, measure, threshold, predicate, hard_fail=False):
        super().__init__(description, measure, threshold, predicate, hard_fail)

