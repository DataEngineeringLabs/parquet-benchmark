use std::fs::File;
use std::io::Read;
use std::{fs, io::Cursor, path::PathBuf};

use arrow2::datatypes::Schema;
use arrow2::io::parquet::write::FileMetaData;
use criterion::*;

use arrow2::error::Result;
use arrow2::io::parquet::read;

fn to_path(size: usize, dict: bool, multi_page: bool, compressed: bool) -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed = if compressed { "snappy/" } else { "" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.parquet",
        dict, multi_page, compressed, size
    ));

    path
}

fn read_batch(path: &PathBuf, size: usize, column: usize) -> Result<()> {
    let mut reader = Box::new(File::open(path)?);

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;

    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|_index, _field| true);

    // say we found that we only need to read the first two row groups, "0" and "1"
    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == 0 || *index == 1)
        .map(|(_, row_group)| row_group)
        .collect();

    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(reader, row_groups, schema, Some(size), None, None);

    for maybe_chunk in chunks {
        let chunk = maybe_chunk?;
        assert!(!chunk.is_empty());
    }

    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    for log2_size in (10..=20).step_by(2) {
        let size = 2usize.pow(log2_size);
        let path = to_path(size, false, false, false);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("i64", log2_size), &path, |b, path| {
            b.iter(|| read_batch(&path, size, 0).unwrap())
        });

        group.bench_with_input(BenchmarkId::new("utf8", log2_size), &path, |b, path| {
            b.iter(|| read_batch(&path, size, 2).unwrap())
        });

        group.bench_with_input(BenchmarkId::new("bool", log2_size), &path, |b, path| {
            b.iter(|| read_batch(&path, size, 3).unwrap())
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
