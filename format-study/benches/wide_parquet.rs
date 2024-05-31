use std::{fs::File, path::Path, sync::Arc};

use arrow::{
    array::{ArrayRef, Float32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use parquet::{arrow::ArrowWriter, basic::Encoding, file::properties::WriterProperties};

const N_COLUMN: usize = 4_000;
const N_ROW: usize = 1_000_000;

fn main() {
    let mut fields = Vec::with_capacity(N_COLUMN);

    for i in 0..N_COLUMN {
        fields.push(Field::new(
            &format!("column_{}", i),
            DataType::Float32,
            false,
        ));
    }

    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(N_COLUMN);
    let array = {
        let mut v = Vec::with_capacity(N_ROW);
        for _ in 0..N_ROW {
            v.push(42.0);
        }
        Arc::new(Float32Array::from(v))
    };

    for _ in 0..N_COLUMN {
        columns.push(array.clone());
    }
    let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = File::create(Path::new("data.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(true)
                .set_encoding(Encoding::RLE)
                .build(),
        ),
    )
    .unwrap();

    // Write the RecordBatch to 10 row groups
    for i in 0..10 {
        println!("writing record batch: {}", i);
        writer.write(&record_batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}
