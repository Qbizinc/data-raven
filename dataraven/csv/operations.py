import csv


def get_csv_document(path, delimiter=',', fieldnames=None):
    """
    :param path: path to csv file
    :param delimiter: separator used in csv file
    :return: list of tuples where each tuple is a row in csv file
    """
    with open(path, 'r') as infile:
        csvreader = csv.DictReader(infile, delimiter=delimiter, fieldnames=fieldnames)
        dataset = list(csvreader)
    infile.close()
    return dataset


def apply_reducer(dataset, reducer, *columns, **kwargs):
    """
    :param dataset:
    :param measure:
    :param accum:
    :param args:
    :param is_header_included:
    :param kwargs:
    :return:
    """
    rowcnt = 0
    headers = None
    accum = dict(zip(columns, [0] * len(columns)))
    for row in dataset:
        rowcnt += 1
        if rowcnt == 1:
            headers = row
        else:
            output = reducer(row, *columns, **kwargs)

            result = output["result"]
            collection = output.get("collection")
            if collection is not None:
                kwargs["collection"] = collection

            for column in result:
                accum[column] = accum.get(column, 0) + result[column]

    rowcnt -= 1  # offset for headers row
    results = {"rowcnt": rowcnt, "accum": accum, "headers": headers}
    return results
