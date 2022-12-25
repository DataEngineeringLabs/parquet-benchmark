use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::PathBuf;
use std::vec;

use arrow2::chunk::Chunk;
use arrow2::error::Result;
use criterion::BenchmarkId;
use criterion::Throughput;
use criterion::*;
use pa::read::reader::{infer_schema, read_meta, PaReader};
use pa::{write, ColumnMeta, Compression};

fn to_path(size: usize, dict: bool, multi_page: bool, compressed: bool) -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");

    let dict = if dict { "dict/" } else { "" };
    let multi_page = if multi_page { "multi/" } else { "" };
    let compressed_str = if compressed { "snappy/" } else { "" };

    let path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.parquet",
        dict, multi_page, compressed_str, size
    ));

    let pa_path = PathBuf::from(dir).join(format!(
        "fixtures/pyarrow/v1/{}{}{}benches_{}.pa",
        dict, multi_page, compressed_str, size
    ));
    // write pa file to test
    write_pa(&path, &pa_path, compressed);
    pa_path
}

fn write_pa(parquet_path: &PathBuf, pa_path: &PathBuf, compressed: bool) -> Result<()> {
    let mut reader = File::open(parquet_path).unwrap();
    let metadata = arrow2::io::parquet::read::read_metadata(&mut reader).unwrap();
    let schema = arrow2::io::parquet::read::infer_schema(&metadata).unwrap();
    let schema = schema.filter(|_index, _field| true);
    let row_groups = metadata.row_groups;
    let chunks = arrow2::io::parquet::read::FileReader::new(
        reader,
        row_groups,
        schema.clone(),
        Some(usize::MAX),
        None,
        None,
    );
    let file = File::create(pa_path)?;
    let compression = if compressed {
        Compression::SNAPPY
    } else {
        Compression::None
    };
    let options = write::WriteOptions {
        compression: compression,
        max_page_size: Some(8 * 1024),
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
    let schema = infer_schema(&mut reader).unwrap();
    let schema = schema.filter(|index, _field| index == column);
    let metas: Vec<ColumnMeta> = read_meta(&mut reader)
        .expect("read error")
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == column)
        .map(|(_, meta)| meta)
        .collect();
    let mut readers = vec![];
    for (meta, field) in metas.iter().zip(schema.fields.iter()) {
        let mut reader = File::open(path).unwrap();
        reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();

        let buffer_size = meta.total_len().min(8192) as usize;
        let reader = BufReader::with_capacity(buffer_size, reader);
        let scratch = Vec::with_capacity(8 * 1024);

        let pa_reader = PaReader::new(
            reader,
            field.data_type().clone(),
            meta.pages.clone(),
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

        let _ = Chunk::new(chunks);
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

        let path = to_path(size, false, false, true);

        group.bench_with_input(
            BenchmarkId::new("i64 snappy", log2_size),
            &path,
            |b, path| b.iter(|| read_batch(&path, size, 0).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("utf8 snappy", log2_size),
            &path,
            |b, path| b.iter(|| read_batch(&path, size, 2).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("bool snappy", log2_size),
            &path,
            |b, path| b.iter(|| read_batch(&path, size, 3).unwrap()),
        );
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
