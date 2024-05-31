use chrono::Local;
use clap::Parser;
use serde::Serialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

use parquet::{
    arrow::arrow_reader::ArrowReaderMetadata,
    file::{
        footer::{decode_footer, decode_metadata},
        reader::ChunkReader,
        FOOTER_SIZE,
    },
};

#[derive(Debug, Clone, Serialize)]
struct Measurements {
    metadata_end_to_end_load_time_nanos: usize,
    metadata_decode_time_nanos: usize,
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

fn measure_decode_time<R: ChunkReader>(reader: &R) -> Duration {
    let metadata_len = get_metadata_len(reader);
    let footer_metadata_len = FOOTER_SIZE + metadata_len;

    assert!(footer_metadata_len <= reader.len() as usize);
    let start = reader.len() - footer_metadata_len as u64;
    let metadata_bytes = reader.get_bytes(start, metadata_len).unwrap();

    let now = std::time::Instant::now();
    let metadata = decode_metadata(metadata_bytes.as_ref()).unwrap();
    let elapsed = now.elapsed();
    std::hint::black_box(metadata);
    elapsed
}

fn get_column_row_count(meta: &ArrowReaderMetadata) -> (usize, usize, usize) {
    let row_group_cnt = meta.metadata().num_row_groups();
    let row_cnt = meta.metadata().file_metadata().num_rows();
    let column_cnt = meta.schema().fields().len();
    (row_group_cnt, row_cnt as usize, column_cnt)
}

fn benchmark_one(path: impl AsRef<Path>) -> Measurements {
    let now = std::time::Instant::now();
    let file = std::fs::File::open(&path).unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    let metadata_end_to_end_load_time = now.elapsed();

    let metadata_len = get_metadata_len(&file);

    let metadata_decode_time = measure_decode_time(&file);

    let (row_group_cnt, row_cnt, column_cnt) = get_column_row_count(&metadata);

    Measurements {
        metadata_end_to_end_load_time_nanos: metadata_end_to_end_load_time.as_nanos() as usize,
        metadata_decode_time_nanos: metadata_decode_time.as_nanos() as usize,
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
