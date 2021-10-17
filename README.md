# Parquet benchmarks

This repository contains a set of benchmarks of different implementations of 
Parquet (storage format) <-> Arrow (in-memory format).

The results on Azure's Standard D4s v3 (4 vcpus, 16 GiB memory) are summarized [here](https://docs.google.com/spreadsheets/d/19mHMZHH2YLtvGBqcJqdGRQxxLh-DUzGZ6xe8F9o00MU/edit?usp=sharing).

To reproduce, use 

```bash
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install pyarrow

# create files
venv/bin/python write_parquet.py

# run benchmarks
venv/bin/python run.py

# print results to stdout as csv
venv/bin/python summarize.py
```

## Details

The benchmark reads a single column from a file pre-loaded into memory,
decompresses and deserializes the column to an arrow array.

The benchmark includes different configurations:

* dictionary-encoded vs plain encoding
* single page vs multiple pages
* compressed vs uncompressed
* different types:
    * `i64`
    * `bool`
    * `utf8`
