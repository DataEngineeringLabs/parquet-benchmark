use std::sync::Arc;

use criterion::*;

use arrow::array::*;
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::{arrow::ArrowWriter, errors::Result, file::writer::InMemoryWriteableCursor};

fn create_batch(size: usize, ty: &str) -> RecordBatch {
    let i64 = [
        Some(0),
        Some(1),
        None,
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
    ];

    let utf8 = [
        Some("aaaa"),
        Some("aaab"),
        None,
        Some("aaac"),
        Some("aaad"),
        Some("aaae"),
        Some("aaaf"),
        Some("aaag"),
    ];

    let bool = [
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        Some(true),
        Some(true),
        Some(true),
    ];

    let array = match ty {
        "i64" => Arc::new(i64.iter().cycle().take(size).collect::<Int64Array>()) as Arc<dyn Array>,
        "utf8" => Arc::new(
            utf8.iter()
                .cloned()
                .cycle()
                .take(size)
                .collect::<StringArray>(),
        ) as Arc<dyn Array>,
        "bool" => {
            Arc::new(bool.iter().cycle().take(size).collect::<BooleanArray>()) as Arc<dyn Array>
        }
        _ => todo!(),
    };
    assert_eq!(array.len(), size);
    RecordBatch::try_from_iter([("test", array)]).unwrap()
}

fn write(batch: RecordBatch, is_compressed: bool) -> Result<()> {
    let cursor = InMemoryWriteableCursor::default();

    let compression = if is_compressed {
        Compression::SNAPPY
    } else {
        Compression::UNCOMPRESSED
    };

    let options = WriterProperties::builder()
        .set_write_batch_size(batch.num_rows())
        .set_compression(compression)
        .build();

    let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(options))?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    for log2_size in (10..=20).step_by(2) {
        let size = 2usize.pow(log2_size);
        group.throughput(Throughput::Elements(size as u64));
        for ty in ["i64", "utf8", "bool"] {
            let batch = create_batch(size, ty);

            for is_compressed in [true, false] {
                let id = if is_compressed {
                    format!("{} snappy", ty)
                } else {
                    ty.to_string()
                };

                group.bench_with_input(BenchmarkId::new(id, log2_size), &batch, |b, batch| {
                    b.iter(|| write(batch.clone(), is_compressed).unwrap())
                });
            }
        }
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
