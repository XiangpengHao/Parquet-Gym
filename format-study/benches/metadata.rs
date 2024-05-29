use std::{path::Path, time::Duration};

use format_study::encode_parquet_meta;
use parquet::{
    format::FileMetaData,
    thrift::{TCompactSliceInputProtocol, TSerializable},
};
use serde::Serialize;

const REPEAT: usize = 5;

#[derive(Clone, Debug, Serialize)]
struct Config {
    num_columns: usize,
}

#[derive(Debug, Serialize)]
struct Measurements {
    elapse: Duration,
    meta_data_size: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkResult {
    config: Config,
    measurements: Measurements,
}

fn benchmark() -> Vec<BenchmarkResult> {
    let columns = [10, 100, 1_000, 10_000, 100_000];
    let mut results = vec![];

    for num_column in columns.iter() {
        let c = Config {
            num_columns: *num_column,
        };
        let result = benchmark_one(&c);
        results.extend(result);
    }
    results
}

fn benchmark_one(c: &Config) -> Vec<BenchmarkResult> {
    let mut results = vec![];
    let buf = encode_parquet_meta(c.num_columns);
    let meta_size = buf.len();

    for _ in 0..REPEAT {
        let start = std::time::Instant::now();
        let mut input = TCompactSliceInputProtocol::new(&buf);
        FileMetaData::read_from_in_protocol(&mut input).unwrap();
        let elapse = start.elapsed();
        results.push(BenchmarkResult {
            config: c.clone(),
            measurements: Measurements {
                elapse,
                meta_data_size: meta_size,
            },
        });
    }
    results
}

fn save_result_to_json(dst: impl AsRef<Path>, results: &Vec<BenchmarkResult>) {
    let path = dst.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("Unable to create directories");
    }
    let file = std::fs::File::create(dst).unwrap();
    serde_json::to_writer_pretty(file, results).unwrap();
}

fn main() {
    let results = benchmark();
    let dst_file = "target/benchmark/metadata_bench.json";
    save_result_to_json(dst_file, &results);
    println!("Benchmark result saved to {}", dst_file);
}
