use criterion::*;
use format_study::{encode_parquet_meta, encoded_ipc_schema};
use parquet::{
    format::FileMetaData,
    thrift::{TCompactSliceInputProtocol, TSerializable},
};

fn criterion_benchmark(c: &mut Criterion) {
    let columns = [10, 100, 1_000, 10_000, 100_000];
    let mut parquet_group = c.benchmark_group("parquet_column_size");
    for num_column in columns.iter() {
        let buf = black_box(encode_parquet_meta(*num_column));
        println!("Parquet metadata len {}", buf.len());
        parquet_group.bench_with_input(
            BenchmarkId::from_parameter(num_column),
            num_column,
            |b, _num_column| {
                b.iter(|| {
                    let mut input = TCompactSliceInputProtocol::new(&buf);
                    FileMetaData::read_from_in_protocol(&mut input).unwrap();
                });
            },
        );
    }
    parquet_group.finish();

    let mut arrow_group = c.benchmark_group("arrow_ipc_column_size");
    for num_column in columns.iter() {
        let buf = black_box(encoded_ipc_schema(*num_column));
        println!("Arrow IPC schema len {}", buf.len());
        arrow_group.bench_with_input(
            BenchmarkId::from_parameter(num_column),
            num_column,
            |b, _num_column| {
                b.iter(|| arrow::ipc::root_as_message(&buf).unwrap());
            },
        );
    }
    arrow_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
