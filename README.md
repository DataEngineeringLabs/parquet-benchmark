# Parquet benchmarks

This repository contains a set of benchmarks of different implementations of 
Parquet (storage format) <-> Arrow (in-memory format).

The results on Azure's Standard D4s v3 (4 vcpus, 16 GiB memory) are available [here](https://docs.google.com/spreadsheets/d/19mHMZHH2YLtvGBqcJqdGRQxxLh-DUzGZ6xe8F9o00MU/edit?usp=sharing).

### Uncompressed

![read uncompressed i64](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=1265154504&format=image)

![read uncompressed bool](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=1959960703&format=image)

![read uncompressed utf8](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=1538007177&format=image)

(Note: neither `pyarrow` nor `arrow` validate `utf8`, which can result in undefined behavior.)

![read uncompressed dict utf8](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=480263317&format=image)

(Note: neither `pyarrow` nor `arrow` validate `utf8`, which can result in undefined behavior.)


### Compressed (snappy)

![read compressed i64](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=1241916784&format=image)

![read compressed bool](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=104361337&format=image)

![read compressed utf8](https://docs.google.com/spreadsheets/d/e/2PACX-1vTjeBAL6xNnsKG5JO0v5XSH_s8bX95qYvYgWUXWOHijCE1TYYuhXGTGxDo0MHJD_LrAhgQbmMmYEFoY/pubchart?oid=1524487308&format=image)

(Note: neither `pyarrow` nor `arrow` validate `utf8`, which can result in undefined behavior.)

## Run benchmarks

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
