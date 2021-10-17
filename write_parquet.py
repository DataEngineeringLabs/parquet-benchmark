import pyarrow as pa
import pyarrow.parquet
import os
from decimal import Decimal

PYARROW_PATH = "fixtures/pyarrow"


def data_nullable(size=1):
    int64 = [0, 1, None, 3, None, 5, 6, 7, None, 9]
    float64 = [0.0, 1.0, None, 3.0, None, 5.0, 6.0, 7.0, None, 9.0]
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]
    string_large = [
        "ABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCD😃🌚🕳👊"
    ] * 10
    decimal = [Decimal(e) if e is not None else None for e in int64]

    fields = [
        pa.field("int64", pa.int64()),
        pa.field("float64", pa.float64()),
        pa.field("string", pa.utf8()),
        pa.field("bool", pa.bool_()),
        pa.field("date", pa.timestamp("ms")),
        pa.field("uint32", pa.uint32()),
        pa.field("string_large", pa.utf8()),
        # decimal testing
        pa.field("decimal_9", pa.decimal128(9, 0)),
        pa.field("decimal_18", pa.decimal128(18, 0)),
        pa.field("decimal_26", pa.decimal128(26, 0)),
    ]
    schema = pa.schema(fields)

    return (
        {
            "int64": int64 * size,
            "float64": float64 * size,
            "string": string * size,
            "bool": boolean * size,
            "date": int64 * size,
            "uint32": int64 * size,
            "string_large": string_large * size,
            "decimal_9": decimal * size,
            "decimal_18": decimal * size,
            "decimal_26": decimal * size,
        },
        schema,
    )


def case_benches(size):
    assert size % 8 == 0
    data, schema = data_nullable(1)
    for k in data:
        data[k] = data[k][:8] * (size // 8)
    return data, schema, f"benches_{size}.parquet"


def write_pyarrow(
    case,
    size: int,
    page_version: int,
    use_dictionary: bool,
    multiple_pages: bool,
    compression: bool,
):
    data, schema, path = case(size)

    base_path = f"{PYARROW_PATH}/v{page_version}"
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

    if multiple_pages:
        data_page_size = 2 ** 10  # i.e. a small number to ensure multiple pages
    else:
        data_page_size = 2 ** 40  # i.e. a large number to ensure a single page

    t = pa.table(data, schema=schema)
    os.makedirs(base_path, exist_ok=True)
    pa.parquet.write_table(
        t,
        f"{base_path}/{path}",
        row_group_size=2 ** 40,
        use_dictionary=use_dictionary,
        compression=compression,
        write_statistics=True,
        data_page_size=data_page_size,
        data_page_version=f"{page_version}.0",
    )


# for read benchmarks
for i in range(10, 22, 2):
    # two pages (dict)
    write_pyarrow(case_benches, 2 ** i, 1, True, False, False)
    # single page
    write_pyarrow(case_benches, 2 ** i, 1, False, False, False)
    # multiple pages
    write_pyarrow(case_benches, 2 ** i, 1, False, True, False)
    # multiple compressed pages
    write_pyarrow(case_benches, 2 ** i, 1, False, True, True)
    # single compressed page
    write_pyarrow(case_benches, 2 ** i, 1, False, False, True)
