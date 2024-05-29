use std::{path::Path, time::Duration};

use chrono::Local;
use format_study::encode_parquet_meta;
use parquet::{
    format::FileMetaData,
    thrift::{TCompactSliceInputProtocol, TSerializable},
};
use serde::Serialize;

const REPEAT: usize = 5;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone, Debug, Serialize)]
struct Config {
    num_columns: usize,
    mimalloc: bool,
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

fn benchmark(column_size: Option<usize>) -> Vec<BenchmarkResult> {
    let columns = match column_size {
        Some(size) => vec![size],
        None => vec![10, 100, 1_000, 10_000, 100_000],
    };
    let mut results = vec![];

    for num_column in columns.iter() {
        let c = Config {
            num_columns: *num_column,
            mimalloc: if cfg!(feature = "mimalloc") {
                true
            } else {
                false
            },
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
        std::hint::black_box(FileMetaData::read_from_in_protocol(&mut input).unwrap());
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
    // An almost too simple arg handling.
    // let args: Vec<String> = std::env::args().collect();
    // let column_size = if args.len() > 1 {
    //     Some(args[1].parse::<usize>().expect("Invalid column size"))
    // } else {
    //     None
    // };

    let current_time = Local::now();
    let formatted_time = current_time.format("%m_%d_%H_%M").to_string();
    let dst_file = format!("target/benchmark/metadata_bench_{}.json", formatted_time);
    let results = benchmark(None);
    save_result_to_json(&dst_file, &results);
    println!("Benchmark result saved to {}", dst_file);
}
