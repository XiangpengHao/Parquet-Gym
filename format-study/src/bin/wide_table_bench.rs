use chrono::Local;
use clap::Parser;
use serde::Serialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use parquet::{
    arrow::arrow_reader::ArrowReaderMetadata,
    file::{
        footer::{self, decode_footer},
        reader::ChunkReader,
        FOOTER_SIZE,
    },
};

#[derive(Debug, Clone, Serialize)]
struct Measurements {
    metadata_end_to_end_load_time_nanos: usize,
    schema_build_time_nanos: usize,
    thrift_decode_time_nanos: usize,
    file_open_time_nanos: usize,
    metadata_len: usize,
    column_cnt: usize,
    row_group_cnt: usize,
    row_cnt: usize,
    file_name: String,
}

fn get_metadata_len<R: ChunkReader>(reader: &R) -> usize {
    let file_size = reader.len();
    assert!(file_size >= FOOTER_SIZE as u64);

    let mut footer = [0_u8; 8];
    reader
        .get_read(file_size - 8)
        .unwrap()
        .read_exact(&mut footer)
        .unwrap();

    let metadata_len = decode_footer(&footer).unwrap();
    metadata_len
}

fn get_column_row_count(meta: &ArrowReaderMetadata) -> (usize, usize, usize) {
    let row_group_cnt = meta.metadata().num_row_groups();
    let row_cnt = meta.metadata().file_metadata().num_rows();
    let column_cnt = meta.schema().fields().len();
    (row_group_cnt, row_cnt as usize, column_cnt)
}

fn benchmark_one(path: impl AsRef<Path>) -> Measurements {
    let mut now = std::time::Instant::now();
    let file = std::fs::File::open(&path).unwrap();
    let file_open_time = now.elapsed();

    now = std::time::Instant::now();
    let metadata = footer::parse_metadata(&file).unwrap();
    let thrift_parse_time = now.elapsed();

    now = std::time::Instant::now();
    let end_metadata =
        ArrowReaderMetadata::try_new(Arc::new(metadata), Default::default()).unwrap();
    let schema_build_time = now.elapsed();

    let metadata_end_to_end_load_time = file_open_time + thrift_parse_time + schema_build_time;

    let metadata_len = get_metadata_len(&file);

    let (row_group_cnt, row_cnt, column_cnt) = get_column_row_count(&end_metadata);

    Measurements {
        metadata_end_to_end_load_time_nanos: metadata_end_to_end_load_time.as_nanos() as usize,
        thrift_decode_time_nanos: thrift_parse_time.as_nanos() as usize,
        file_open_time_nanos: file_open_time.as_nanos() as usize,
        schema_build_time_nanos: schema_build_time.as_nanos() as usize,
        metadata_len,
        column_cnt,
        row_group_cnt,
        row_cnt,
        file_name: path.as_ref().file_name().unwrap().to_str().unwrap().into(),
    }
}

fn benchmark(file: impl AsRef<Path>, repeat: usize) -> Vec<Measurements> {
    (0..repeat).map(|_| benchmark_one(&file)).collect()
}

fn save_to_json(out_dir: impl AsRef<Path>, data: &[Measurements]) -> PathBuf {
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir).unwrap();

    let current_time = Local::now();
    let formatted_time = current_time.format("%H_%M_%S").to_string();
    let out_file = out_dir.join(format!("wide_table_{}.json", formatted_time));
    let file = std::fs::File::create(&out_file).unwrap();
    serde_json::to_writer_pretty(file, data).unwrap();
    out_file
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Parquet file
    #[arg(long)]
    input: String,

    #[arg(long, default_value_t = 5)]
    repeat: usize,

    #[arg(long, default_value = "target")]
    output_dir: String,
}

fn main() {
    #[cfg(debug_assertions)]
    {
        println!("Running with debug assertions, are you building with --release?");
    }
    let args = Args::parse();
    let results = benchmark(&args.input, args.repeat);
    let out_file = save_to_json(args.output_dir, &results);
    println!("Benchmark result saved to {}", out_file.display());
}
