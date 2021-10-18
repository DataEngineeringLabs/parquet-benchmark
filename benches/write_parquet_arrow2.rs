use std::io::Cursor;
use std::sync::Arc;

use criterion::*;

use arrow2::array::*;
use arrow2::error::Result;
use arrow2::io::parquet::write::*;
use arrow2::record_batch::RecordBatch;

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
                .collect::<Utf8Array<i32>>(),
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
    let schema = batch.schema().clone();

    let compression = if is_compressed {
        Compression::Snappy
    } else {
        Compression::Uncompressed
    };

    let options = WriteOptions {
        write_statistics: true,
        compression,
        version: Version::V1,
    };

    let parquet_schema = to_parquet_schema(&schema)?;

    let iter = vec![Ok(batch)];

    let row_groups =
        RowGroupIterator::try_new(iter.into_iter(), &schema, options, vec![Encoding::Plain])?;

    let mut writer = Cursor::new(vec![]);
    write_file(
        &mut writer,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;
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
