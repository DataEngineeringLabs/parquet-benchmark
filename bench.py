import timeit
import io
import os
import json

import pyarrow.parquet


def _bench_read_single(
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


def _bench_write_single(log2_size: int, column: str, compression: bool) -> float:

    if column == "int64":
        data = [0, 1, None, 3, 4, 5, 6, 7]
        field = pyarrow.field("int64", pyarrow.int64())
    elif column == "string":
        data = [
            "aaaa",
            "aaab",
            None,
            "aaac",
            "aaad",
            "aaae",
            "aaaf",
            "aaag",
        ]
        field = pyarrow.field("utf8", pyarrow.utf8())
    elif column == "bool":
        data = [True, False, None, True, False, True, True, True]
        field = pyarrow.field("bool", pyarrow.bool_())

    data = data * 2 ** (log2_size - 3)  # 3 because data already has 8 elements

    t = pyarrow.table([data], schema=pyarrow.schema([field]))

    def f():
        pyarrow.parquet.write_table(
            t,
            io.BytesIO(),
            use_dictionary=False,
            compression="snappy" if compression else None,
            write_statistics=True,
            data_page_size=2 ** 40,  # i.e. a large number to ensure a single page
            data_page_version="1.0",
        )

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


def _bench_read(size, case):
    column, use_dict, multiple_pages, is_compressed = CASES[case]

    result = _bench_read_single(size, column, use_dict, multiple_pages, is_compressed)
    print(result)
    _report(f"read/{case}/{size}", result)


def _bench_write(size, case):
    column, use_dict, multiple_pages, is_compressed = CASES[case]
    if use_dict or multiple_pages:
        return

    result = _bench_write_single(size, column, is_compressed)
    print(result)
    _report(f"write/{case}/{size}", result)


for size in range(10, 22, 2):
    for ty in CASES:
        print(size, ty)
        _bench_read(size, ty)
        _bench_write(size, ty)
