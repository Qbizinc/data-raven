from .operations import apply_reducer


def build_measure_proportion_values(results):
    measure_values = {}
    rowcnt = results["rowcnt"]
    if rowcnt == 0:
        raise ValueError(f"rowcnt parameter returned from {apply_reducer.__name__} must be greater than 0.")

    accum = results["accum"]
    for column in accum:
        result = accum[column]
        measure_value = result / rowcnt
        measure_values[column] = measure_value

    return measure_values