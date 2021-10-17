import subprocess
import shutil


def _run_arrow2():
    args = ["cargo", "bench", "--bench", "read_parquet_arrow2"]
    subprocess.call(args)
    shutil.move("target/criterion/read", "target/criterion/arrow2/read")


def _run_arrow():
    args = ["cargo", "bench", "--bench", "read_parquet_arrow"]
    subprocess.call(args)
    shutil.move("target/criterion/read", "target/criterion/arrow/read")


# run pyarrow
subprocess.call(["python", "bench_read.py"])
_run_arrow()
_run_arrow2()
