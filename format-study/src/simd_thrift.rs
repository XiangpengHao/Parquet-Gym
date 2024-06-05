use parquet::thrift::TInputProtocolRef;
use thrift::protocol::{
    TFieldIdentifier, TInputProtocol, TListIdentifier, TMapIdentifier, TMessageIdentifier,
    TSetIdentifier, TStructIdentifier, TType,
};
use varint_simd::SignedVarIntTarget;

/// A more performant implementation of [`TCompactInputProtocol`] that reads a slice
///
/// [`TCompactInputProtocol`]: thrift::protocol::TCompactInputProtocol
pub struct TCompactSimdInputProtocol<'a> {
    buf: &'a [u8],
    // Identifier of the last field deserialized for a struct.
    last_read_field_id: i16,
    // Stack of the last read field ids (a new entry is added each time a nested struct is read).
    read_field_id_stack: Vec<i16>,
    // Boolean value for a field.
    // Saved because boolean fields and their value are encoded in a single byte,
    // and reading the field only occurs after the field id is read.
    pending_read_bool_value: Option<bool>,
}

impl<'a> TCompactSimdInputProtocol<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            last_read_field_id: 0,
            read_field_id_stack: Vec::with_capacity(16),
            pending_read_bool_value: None,
        }
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.buf
    }

    fn read_vlq(&mut self) -> thrift::Result<u64> {
        let (val, shift) = varint_simd::decode(&self.buf).unwrap();
        self.buf = &self.buf[shift..];
        Ok(val)
    }

    fn read_zig_zag<T: SignedVarIntTarget>(&mut self) -> thrift::Result<T> {
        let (val, shift): (T, usize) = varint_simd::decode_zigzag(&self.buf).unwrap();
        self.buf = &self.buf[shift..];
        Ok(val)
    }

    fn read_list_set_begin(&mut self) -> thrift::Result<(TType, i32)> {
        let header = self.read_byte()?;
        let element_type = collection_u8_to_type(header & 0x0F)?;

        let possible_element_count = (header & 0xF0) >> 4;
        let element_count = if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            possible_element_count as i32
        } else {
            self.read_vlq()? as _
        };

        Ok((element_type, element_count))
    }
}

impl<'a> TInputProtocolRef<'a> for TCompactSimdInputProtocol<'a> {
    fn read_buf(&mut self) -> thrift::Result<std::borrow::Cow<'a, [u8]>> {
        Ok(self.read_bytes()?.into())
    }

    fn read_str(&mut self) -> thrift::Result<std::borrow::Cow<'a, str>> {
        Ok(self.read_string()?.into())
    }
}

impl<'a> TInputProtocol for TCompactSimdInputProtocol<'a> {
    fn read_message_begin(&mut self) -> thrift::Result<TMessageIdentifier> {
        unimplemented!()
    }

    fn read_message_end(&mut self) -> thrift::Result<()> {
        unimplemented!()
    }

    fn read_struct_begin(&mut self) -> thrift::Result<Option<TStructIdentifier>> {
        self.read_field_id_stack.push(self.last_read_field_id);
        self.last_read_field_id = 0;
        Ok(None)
    }

    fn read_struct_end(&mut self) -> thrift::Result<()> {
        self.last_read_field_id = self
            .read_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(())
    }

    fn read_field_begin(&mut self) -> thrift::Result<TFieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xF0) >> 4;
        let field_type = match field_type & 0x0F {
            0x01 => {
                self.pending_read_bool_value = Some(true);
                Ok(TType::Bool)
            }
            0x02 => {
                self.pending_read_bool_value = Some(false);
                Ok(TType::Bool)
            }
            ttu8 => u8_to_type(ttu8),
        }?;

        match field_type {
            TType::Stop => Ok(
                TFieldIdentifier::new::<Option<String>, String, Option<i16>>(
                    None,
                    TType::Stop,
                    None,
                ),
            ),
            _ => {
                if field_delta != 0 {
                    self.last_read_field_id += field_delta as i16;
                } else {
                    self.last_read_field_id = self.read_i16()?;
                };

                Ok(TFieldIdentifier {
                    name: None,
                    field_type,
                    id: Some(self.last_read_field_id),
                })
            }
        }
    }

    fn read_field_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }

    fn read_bool(&mut self) -> thrift::Result<bool> {
        match self.pending_read_bool_value.take() {
            Some(b) => Ok(b),
            None => {
                let b = self.read_byte()?;
                match b {
                    0x01 => Ok(true),
                    0x02 => Ok(false),
                    unkn => Err(thrift::Error::Protocol(thrift::ProtocolError {
                        kind: thrift::ProtocolErrorKind::InvalidData,
                        message: format!("cannot convert {} into bool", unkn),
                    })),
                }
            }
        }
    }

    fn read_bytes(&mut self) -> thrift::Result<Vec<u8>> {
        let len = self.read_vlq()? as usize;
        let ret = self.buf.get(..len).ok_or_else(eof_error)?.to_vec();
        self.buf = &self.buf[len..];
        Ok(ret)
    }

    fn read_i8(&mut self) -> thrift::Result<i8> {
        Ok(self.read_byte()? as _)
    }

    fn read_i16(&mut self) -> thrift::Result<i16> {
        Ok(self.read_zig_zag()?)
    }

    fn read_i32(&mut self) -> thrift::Result<i32> {
        Ok(self.read_zig_zag()?)
    }

    fn read_i64(&mut self) -> thrift::Result<i64> {
        self.read_zig_zag()
    }

    fn read_double(&mut self) -> thrift::Result<f64> {
        let slice = (self.buf[..8]).try_into().unwrap();
        self.buf = &self.buf[8..];
        Ok(f64::from_le_bytes(slice))
    }

    fn read_string(&mut self) -> thrift::Result<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(From::from)
    }

    fn read_list_begin(&mut self) -> thrift::Result<TListIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(TListIdentifier::new(element_type, element_count))
    }

    fn read_list_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }

    fn read_set_begin(&mut self) -> thrift::Result<TSetIdentifier> {
        unimplemented!()
    }

    fn read_set_end(&mut self) -> thrift::Result<()> {
        unimplemented!()
    }

    fn read_map_begin(&mut self) -> thrift::Result<TMapIdentifier> {
        unimplemented!()
    }

    fn read_map_end(&mut self) -> thrift::Result<()> {
        Ok(())
    }

    #[inline]
    fn read_byte(&mut self) -> thrift::Result<u8> {
        let ret = *self.buf.first().ok_or_else(eof_error)?;
        self.buf = &self.buf[1..];
        Ok(ret)
    }
}

fn collection_u8_to_type(b: u8) -> thrift::Result<TType> {
    match b {
        0x01 => Ok(TType::Bool),
        o => u8_to_type(o),
    }
}

fn u8_to_type(b: u8) -> thrift::Result<TType> {
    match b {
        0x00 => Ok(TType::Stop),
        0x03 => Ok(TType::I08), // equivalent to TType::Byte
        0x04 => Ok(TType::I16),
        0x05 => Ok(TType::I32),
        0x06 => Ok(TType::I64),
        0x07 => Ok(TType::Double),
        0x08 => Ok(TType::String),
        0x09 => Ok(TType::List),
        0x0A => Ok(TType::Set),
        0x0B => Ok(TType::Map),
        0x0C => Ok(TType::Struct),
        unkn => Err(thrift::Error::Protocol(thrift::ProtocolError {
            kind: thrift::ProtocolErrorKind::InvalidData,
            message: format!("cannot convert {} into TType", unkn),
        })),
    }
}

fn eof_error() -> thrift::Error {
    thrift::Error::Transport(thrift::TransportError {
        kind: thrift::TransportErrorKind::EndOfFile,
        message: "Unexpected EOF".to_string(),
    })
}
