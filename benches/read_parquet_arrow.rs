use std::io::Read;
use std::sync::Arc;
use std::{fs, path::PathBuf};

use criterion::*;

use parquet::arrow::*;
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::SliceableCursor;

fn to_buffer(size: usize, dict: bool, multi_page: bool, compressed: bool) -> Arc<Vec<u8>> {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed = if compressed { "snappy/" } else { "" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.parquet",
        dict, multi_page, compressed, size
    ));

    let metadata = fs::metadata(&path).expect("unable to read_arrow metadata");
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    file.read_exact(&mut buffer).expect("buffer overflow");
    Arc::new(buffer)
}

fn read_batch(buffer: Arc<Vec<u8>>, size: usize, column: usize) {
    let file = SliceableCursor::new(buffer);

    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let record_batch_reader = arrow_reader
        .get_record_reader_by_columns(vec![column], size)
        .unwrap();

    for maybe_batch in record_batch_reader {
        let batch = maybe_batch.unwrap();
        assert_eq!(batch.num_rows(), size);
    }
}

fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    for log2_size in (10..=20).step_by(2) {
        let size = 2usize.pow(log2_size);
        let buffer = to_buffer(size, false, false, false);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("i64", log2_size), &buffer, |b, buffer| {
            b.iter(|| read_batch(buffer.clone(), size, 0))
        });

        group.bench_with_input(BenchmarkId::new("utf8", log2_size), &buffer, |b, buffer| {
            b.iter(|| read_batch(buffer.clone(), size, 2))
        });

        group.bench_with_input(BenchmarkId::new("bool", log2_size), &buffer, |b, buffer| {
            b.iter(|| read_batch(buffer.clone(), size, 3))
        });

        let buffer = to_buffer(size, true, false, false);
        group.bench_with_input(
            BenchmarkId::new("utf8 dict", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 2)),
        );

        let buffer = to_buffer(size, false, false, true);
        group.bench_with_input(
            BenchmarkId::new("i64 snappy", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 0)),
        );

        group.bench_with_input(
            BenchmarkId::new("bool snappy", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 3)),
        );

        group.bench_with_input(
            BenchmarkId::new("utf8 snappy", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 2)),
        );

        let buffer = to_buffer(size, false, true, false);
        group.bench_with_input(
            BenchmarkId::new("utf8 multi", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 2)),
        );

        let buffer = to_buffer(size, false, true, true);
        group.bench_with_input(
            BenchmarkId::new("utf8 multi snappy", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 2)),
        );

        let buffer = to_buffer(size, false, true, true);
        group.bench_with_input(
            BenchmarkId::new("i64 multi snappy", log2_size),
            &buffer,
            |b, buffer| b.iter(|| read_batch(buffer.clone(), size, 0)),
        );
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
