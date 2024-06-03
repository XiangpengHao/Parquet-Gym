use std::{fs::File, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use clap::{Parser, ValueEnum};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};

#[derive(ValueEnum, Clone, Debug)]
enum Statistics {
    None,
    Chunk,
    Page,
}

impl From<Statistics> for EnabledStatistics {
    fn from(stats: Statistics) -> Self {
        match stats {
            Statistics::None => EnabledStatistics::None,
            Statistics::Chunk => EnabledStatistics::Chunk,
            Statistics::Page => EnabledStatistics::Page,
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of columns
    #[arg(long)]
    column: usize,

    /// Total number of values
    #[arg(long)]
    value_cnt_million: usize,

    /// Number of row groups
    #[arg(long, default_value_t = 10)]
    row_group: usize,

    #[arg(long, value_enum)]
    stats: Statistics,

    /// Output file
    #[arg(long)]
    output: String,
}

fn generate(args: Args) {
    let mut fields = Vec::with_capacity(args.column);

    for i in 0..args.column {
        fields.push(Field::new(
            &format!("column_{}", i),
            DataType::Float64,
            false,
        ));
    }

    let schema = Arc::new(Schema::new(fields));

    let row_per_group = args.value_cnt_million * 1_000_000 / args.row_group / args.column;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(args.column);
    let array = {
        let mut v = Vec::with_capacity(row_per_group);
        for _ in 0..row_per_group {
            v.push(42.0);
        }
        Arc::new(Float64Array::from(v))
    };

    for _ in 0..args.column {
        columns.push(array.clone());
    }
    let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = File::create(&args.output).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_size(1_000_000)
                .set_data_page_row_count_limit(10_000)
                .set_statistics_enabled(args.stats.into())
                .build(),
        ),
    )
    .unwrap();

    for i in 0..args.row_group {
        println!("Working on row group: {}", i);
        let write_step = 10_000;
        for offset in (0..row_per_group).step_by(write_step) {
            let length = std::cmp::min(write_step, row_per_group - offset);
            let sliced = record_batch.slice(offset, length);
            writer.write(&sliced).unwrap();
        }
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

fn main() {
    let args = Args::parse();
    generate(args);
}
