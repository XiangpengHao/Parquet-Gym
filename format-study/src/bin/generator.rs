use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::{ArrayRef, Float32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use clap::Parser;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of columns
    #[arg(short, long)]
    column: usize,

    /// Number of rows
    #[arg(short, long)]
    row: usize,

    /// Output file
    #[arg(short, long)]
    output: String,
}

fn generate(n_column: usize, n_row: usize, path: impl AsRef<Path>) {
    let mut fields = Vec::with_capacity(n_column);

    for i in 0..n_column {
        fields.push(Field::new(
            &format!("column_{}", i),
            DataType::Float32,
            false,
        ));
    }

    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_column);
    let array = {
        let mut v = Vec::with_capacity(n_row);
        for _ in 0..n_row {
            v.push(42.0);
        }
        Arc::new(Float32Array::from(v))
    };

    for _ in 0..n_column {
        columns.push(array.clone());
    }
    let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = File::create(path.as_ref()).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();

    for i in 0..10 {
        println!("Working on record batch: {}", i);
        writer.write(&record_batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

fn main() {
    let args = Args::parse();
    generate(args.column, args.row, args.output);
}
