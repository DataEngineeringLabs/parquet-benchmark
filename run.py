import subprocess
import shutil


def _run_read_arrow2():
    args = ["cargo", "bench", "--bench", "read_parquet_arrow2"]
    subprocess.call(args)
    shutil.move("target/criterion/read", "target/criterion/arrow2/read")


def _run_write_arrow2():
    args = ["cargo", "bench", "--bench", "write_parquet_arrow2"]
    subprocess.call(args)
    shutil.move("target/criterion/write", "target/criterion/arrow2/write")


def _run_read_arrow():
    args = ["cargo", "bench", "--bench", "read_parquet_arrow"]
    subprocess.call(args)
    shutil.move("target/criterion/read", "target/criterion/arrow/read")


def _run_write_arrow():
    args = ["cargo", "bench", "--bench", "write_parquet_arrow"]
    subprocess.call(args)
    shutil.move("target/criterion/write", "target/criterion/arrow/write")


# run pyarrow
# subprocess.call(["python", "bench.py"])
# _run_arrow()
# _run_read_arrow2()
_run_write_arrow()
