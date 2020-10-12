

def measure_null(row, *columns, null_values=None):
    """
    :param row:
    :param column_indicies:
    :param null_values:
    :return:
    """
    null_values_ = {"NULL"}
    if null_values is None:
        null_values_ = null_values_.union({})
    else:
        null_values_ = null_values_.union(null_values)

    result = dict(zip(columns, [0] * len(columns)))
    for column in columns:
        value = row[column]
        if value in null_values_:
            result[column] += 1
    output = {"result": result}
    return output


def measure_duplicates(row, *columns, collection=None):
    """
    :param row:
    :param column_indicies:
    :param collection:
    :return:
    """
    if collection is None:
        collection = {}

    indexed_values = []
    for column in columns:
        value = row[column]
        indexed_values.append((column, value))

    key = tuple(indexed_values)
    if key not in collection:
        collection[key] = None
        result = {key: 0}
    else:
        result = {key: 1}

    output = {"result": result, "collection": collection}
    return output
