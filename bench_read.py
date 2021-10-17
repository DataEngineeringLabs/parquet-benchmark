import timeit
import io
import os
import json

import pyarrow.parquet


def _bench_single(
    log2_size: int,
    column: str,
    use_dictionary: bool,
    multiple_pages: bool,
    compression: bool,
) -> float:
    base_path = f"fixtures/pyarrow/v1"

    if use_dictionary:
        base_path = f"{base_path}/dict"

    if multiple_pages:
        base_path = f"{base_path}/multi"

    if compression:
        base_path = f"{base_path}/snappy"

    if compression:
        compression = "snappy"
    else:
        compression = None

    path = f"fixtures/pyarrow/v1/benches_{2**log2_size}.parquet"

    with open(path, "rb") as f:
        data = f.read()
    data = io.BytesIO(data)

    def f():
        pyarrow.parquet.read_table(data, columns=[column])

    seconds = timeit.Timer(f).timeit(number=100) / 100
    ns = seconds * 1000 * 1000 * 1000
    return ns


def _report(name: str, result: float):
    path = f"benchmarks/runs/{name}/new"
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/estimates.json", "w") as f:
        json.dump({"mean": {"point_estimate": result}}, f)


CASES = {
    "i64": ("int64", False, False, False),
    "i64 snappy": ("int64", False, False, True),
    "bool": ("bool", False, False, False),
    "bool snappy": ("bool", False, False, False),
    "utf8": ("string", False, False, False),
    "utf8 snappy": ("string", False, False, True),
    "utf8 dict": ("string", True, False, False),
}


def _bench(size, case):
    column, use_dict, multiple_pages, is_compressed = CASES[case]

    result = _bench_single(size, column, use_dict, multiple_pages, is_compressed)
    print(result)
    _report(f"read/{case}/{size}", result)


for size in range(10, 22, 2):
    for ty in CASES:
        print(size, ty)
        _bench(size, ty)
