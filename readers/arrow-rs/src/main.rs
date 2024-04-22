use clap::{Arg, ArgAction, Command};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("Parquet to Arrow")
        .version("0.1.0")
        .author("Your Name")
        .about("Converts Parquet files to Arrow format")
        .arg(
            Arg::new("input")
                .help("The input Parquet file path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("print")
                .help("Print the record batch")
                .short('p')
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    let print = matches.get_one::<bool>("print").unwrap();
    let input_path = matches.get_one::<String>("input").unwrap();
    let file = File::open(Path::new(input_path))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();

    while let Some(record_batch) = reader.next() {
        match record_batch {
            Ok(batch) => {
                if *print {
                    println!("{:?}", batch);
                }
                std::hint::black_box(batch);
            }
            Err(e) => eprintln!("Error reading batch: {}", e),
        }
    }

    Ok(())
}
