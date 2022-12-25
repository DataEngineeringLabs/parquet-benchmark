import json
import os
import csv
import sys
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def _read_reports(engine: str):
    root = {
        "arrow2": "target/criterion/arrow2/",
        "pyarrow": "benchmarks/runs",
        "pa": "target/criterion/pa/",
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
    for engine in ["arrow2", "pyarrow", "pa"]:
        result = _read_reports(engine)

        for row in result:
            writer.writerow(row.values())


def print_csv_report():

    with open('target/summarize.csv', 'w', newline='') as f:
        writer = csv.writer(f)

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
        for engine in ["arrow2", "pyarrow", "pa"]:
            result = _read_reports(engine)

            for row in result:
                writer.writerow(row.values())


def print_graph():
    data = pd.read_csv("target/summarize.csv")

    grouped = data.groupby(
        by=[data["Type"], data["Task"], data["Is dictionary"], data["Is multi page"], data["Is compressed"]])

    for key, value in grouped:
        df = pd.DataFrame(value)
        sns.pointplot(x="Size", y="Time",  hue="Engine", data=df, label="test")
        plt.title("Type = {}, Task = {}, Is compressed = {}, ".format(
            key[0], key[1],  key[4]))
        # plt.show()
        f = plt.gcf()
        f.savefig(
            r'target/{}'.format("{}-{}-{}, ".format(key[0], key[1],  key[4])))
        f.clear()


print_csv_report()
print_graph()
