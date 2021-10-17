import json
import os
import csv
import sys


def _read_reports(engine: str):
    root = {
        "arrow2": "target/criterion/arrow2/",
        "pyarrow": "benchmarks/runs",
        "arrow": "target/criterion/arrow/",
    }[engine]

    result = []
    for original_path, dirs, files in os.walk(root):
        path = original_path.split(os.sep)
        if path[-1] != "new":
            continue
        path = path[-4:-1]
        task = path[0]
        keys = path[1].split()
        size = int(path[2])

        with open(os.path.join(original_path, "estimates.json")) as f:
            data = json.load(f)

        ms = data["mean"]["point_estimate"] / 1000
        result.append(
            {
                "engine": engine,
                "task": task,
                "type": keys[0],
                "is_dict": "dict" in keys,
                "multi_page": "multi" in keys,
                "is_compressed": "snappy" in keys,
                "size": size,
                "time": ms,
            }
        )
    return result


def print_csv_report():
    writer = csv.writer(sys.stdout)

    writer.writerow(
        [
            "Engine",
            "Task",
            "Type",
            "Is dictionary",
            "Is multi page",
            "Is compressed",
            "Size",
            "Time",
        ]
    )
    for engine in ["arrow2", "pyarrow", "arrow"]:
        result = _read_reports(engine)

        for row in result:
            writer.writerow(row.values())


print_csv_report()
