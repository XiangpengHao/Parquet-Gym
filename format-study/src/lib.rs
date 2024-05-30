use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions};
use parquet::format::{
    ColumnChunk, ColumnMetaData, CompressionCodec, Encoding, FieldRepetitionType, FileMetaData,
    RowGroup, SchemaElement, Type,
};
use parquet::thrift::TSerializable;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use thrift::protocol::TCompactOutputProtocol;

#[cfg(feature = "simd")]
mod simd_thrift;

#[cfg(feature = "simd")]
pub use simd_thrift::TCompactSimdInputProtocol;

const NUM_ROW_GROUPS: usize = 10;

pub fn encode_parquet_meta(num_columns: usize) -> (Vec<u8>, FileMetaData) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut schema = Vec::with_capacity(num_columns + 1);

    schema.push(SchemaElement {
        type_: None,
        type_length: None,
        repetition_type: None,
        name: Default::default(),
        num_children: Some(num_columns as _),
        converted_type: None,
        scale: None,
        precision: None,
        field_id: None,
        logical_type: None,
    });

    for i in 0..num_columns {
        schema.push(SchemaElement {
            type_: Some(Type::FLOAT),
            type_length: None,
            repetition_type: Some(FieldRepetitionType::REQUIRED),
            name: i.to_string().into(),
            num_children: None,
            converted_type: None,
            scale: None,
            precision: None,
            field_id: None,
            logical_type: None,
        });
    }

    let row_groups = (0..NUM_ROW_GROUPS)
        .map(|i| {
            let columns = (0..num_columns)
                .map(|_| ColumnChunk {
                    file_path: None,
                    file_offset: 0,
                    meta_data: Some(ColumnMetaData {
                        type_: Type::FLOAT,
                        encodings: vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY],
                        path_in_schema: vec![],
                        codec: CompressionCodec::UNCOMPRESSED,
                        num_values: rng.gen(),
                        total_uncompressed_size: rng.gen(),
                        total_compressed_size: rng.gen(),
                        key_value_metadata: None,
                        data_page_offset: rng.gen(),
                        index_page_offset: Some(rng.gen()),
                        dictionary_page_offset: Some(rng.gen()),
                        statistics: None,
                        encoding_stats: None,
                        bloom_filter_offset: None,
                        bloom_filter_length: None,
                    }),
                    offset_index_length: Some(rng.gen()),
                    offset_index_offset: Some(rng.gen()),
                    column_index_length: Some(rng.gen()),
                    column_index_offset: Some(rng.gen()),
                    crypto_metadata: None,
                    encrypted_column_metadata: None,
                })
                .collect();
            RowGroup {
                columns,
                total_byte_size: rng.gen(),
                num_rows: rng.gen(),
                sorting_columns: None,
                file_offset: None,
                total_compressed_size: Some(rng.gen()),
                ordinal: Some(i as _),
            }
        })
        .collect();
    let file = FileMetaData {
        schema,
        row_groups,
        version: 1,
        num_rows: rng.gen(),
        key_value_metadata: None,
        created_by: Some("parquet-rs".into()),
        column_orders: None,
        encryption_algorithm: None,
        footer_signing_key_metadata: None,
    };

    let mut buf = Vec::with_capacity(1024);
    {
        let mut out = TCompactOutputProtocol::new(&mut buf);
        file.write_to_out_protocol(&mut out).unwrap();
    }
    (buf, file)
}

pub fn encoded_ipc_schema(num_columns: usize) -> Vec<u8> {
    let schema = Schema::new(Fields::from_iter(
        (0..num_columns).map(|i| Field::new(i.to_string(), DataType::Float64, true)),
    ));

    let data = IpcDataGenerator::default();
    let r = data.schema_to_bytes(&schema, &IpcWriteOptions::default());
    assert_eq!(r.arrow_data.len(), 0);
    r.ipc_message
}
