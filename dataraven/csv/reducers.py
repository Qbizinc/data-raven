

def measure_null(row, *column_indicies, null_values=None):
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

    result = dict(zip(column_indicies, [0] * len(column_indicies)))
    for column_index in column_indicies:
        value = row[column_index]
        if value in null_values_:
            result[column_index] += 1
    output = {"result": result}
    return output


def measure_duplicates(row, *column_indicies, collection=None):
    """
    :param row:
    :param column_indicies:
    :param collection:
    :return:
    """
    if collection is None:
        collection = {}

    indexed_values = []
    for column_index in column_indicies:
        value = row[column_index]
        indexed_values.append((column_index, value))

    key = tuple(indexed_values)
    if key not in collection:
        collection[key] = None
        result = {key: 0}
    else:
        result = {key: 1}

    output = {"result": result, "collection": collection}
    return output
