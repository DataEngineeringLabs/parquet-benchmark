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


def _run_read_pa():
    args = ["cargo", "bench", "--bench", "read_pa"]
    subprocess.call(args)
    shutil.move("target/criterion/read", "target/criterion/pa/read")


def _run_write_pa():
    args = ["cargo", "bench", "--bench", "write_pa"]
    subprocess.call(args)
    shutil.move("target/criterion/write", "target/criterion/pa/write")


# run pyarrow
subprocess.call(["python", "bench.py"])
_run_read_arrow2()
_run_read_pa()
_run_write_arrow2()
_run_write_pa()
