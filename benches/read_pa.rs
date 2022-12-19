use std::fs::File;
use std::io::{BufReader, Read, Seek, Write};
use std::time::Instant;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use criterion::BenchmarkId;
use criterion::Throughput;
use criterion::*;
use pa::read::deserialize;
use pa::read::reader::{infer_schema, read_meta, PaReader};
use pa::{read, write, ColumnMeta};
use std::path::PathBuf;

fn to_path(size: usize, dict: bool, multi_page: bool, compressed: bool) -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed = if compressed { "snappy/" } else { "" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.parquet",
        dict, multi_page, compressed, size
    ));

    let pa_path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.pa",
        dict, multi_page, compressed, size
    ));
    // write pa file to test
    write_pa(&path, &pa_path);
    pa_path
}

fn write_pa(parquet_path: &PathBuf, pa_path: &PathBuf) -> Result<()> {
    let mut reader = File::open(parquet_path).unwrap();

    // we can read its metadata:
    let metadata = arrow2::io::parquet::read::read_metadata(&mut reader).unwrap();
    // and infer a [`Schema`] from the `metadata`.
    let schema = arrow2::io::parquet::read::infer_schema(&metadata).unwrap();
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
    let chunks = arrow2::io::parquet::read::FileReader::new(
        reader,
        row_groups,
        schema.clone(),
        Some(usize::MAX),
        None,
        None,
    );
    let file = File::create(pa_path)?;
    let options = write::WriteOptions {
        compression: Some(write::Compression::LZ4),
        max_page_size: Some(8192),
    };
    let mut writer = write::PaWriter::new(file, schema, options);
    writer.start().unwrap();
    for maybe_chunk in chunks {
        let chunk = maybe_chunk.unwrap();
        writer.write(&chunk);
    }
    writer.finish().unwrap();
    Ok(())
}

fn read_batch(path: &PathBuf, size: usize, column: usize) -> Result<()> {
    let mut reader = File::open(path).unwrap();
    // we can read its metadata:
    // and infer a [`Schema`] from the `metadata`.
    let schema = infer_schema(&mut reader).unwrap();

    let metas: Vec<ColumnMeta> = read_meta(&mut reader)?;

    let mut readers = vec![];
    for (meta, field) in metas.iter().zip(schema.fields.iter()) {
        let mut reader = File::open(path).unwrap();
        reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
        let reader = reader.take(meta.length);

        let buffer_size = meta.length.min(8192) as usize;
        let reader = BufReader::with_capacity(buffer_size, reader);
        let scratch = Vec::with_capacity(8 * 1024);

        let pa_reader = PaReader::new(
            reader,
            field.data_type().clone(),
            true,
            Some(read::Compression::LZ4),
            meta.num_values as usize,
            scratch,
        );
        readers.push(pa_reader);
    }

    'FOR: loop {
        let mut chunks = Vec::new();
        for reader in readers.iter_mut() {
            if !reader.has_next() {
                break 'FOR;
            }
            chunks.push(reader.next_array().unwrap());
        }

        let chunk = Chunk::new(chunks);
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
