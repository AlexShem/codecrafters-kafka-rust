use crate::api_keys::ApiKey;
use anyhow::{bail, Result};
use bytes::{Buf, Bytes, BytesMut};
use std::io::{BufReader, Read};
use std::net::TcpStream;

#[allow(unused)]
pub struct Request {
    pub message_size: i32,
    pub request_header: RequestHeader,
}

pub struct RequestHeader {
    #[allow(unused)]
    pub request_api_key: ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl Request {
    pub fn parse_request(reader: &mut BufReader<TcpStream>) -> Result<Self> {
        // 1. Read message_size (4 bytes, big-endian): 00 00 00 23 | (35)
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let message_size = i32::from_be_bytes(len_buf);
        if message_size < 8 {
            // Need at least api_key(2) + api_version(2) + correlation_id(4)
            bail!("message_size too small: {}", message_size);
        }

        let mut message_buf = BytesMut::with_capacity(message_size as usize);
        message_buf.resize(message_size as usize, 0);
        reader.read_exact(&mut message_buf)?;
        let mut message = message_buf.freeze();

        // 2. Read the request_header
        let request_header = RequestHeader::parse_request_header(&mut message);

        Ok(Self {
            message_size,
            request_header,
        })
    }
}

impl RequestHeader {
    fn parse_request_header(message: &mut Bytes) -> Self {
        // 1. Read request_api_key (2 bytes, big-endian): 00 12 | (18)
        let request_api_key = message.get_i16();
        // 2. Read request_api_version (2 bytes, big-endian): 00 04 | (4)
        let request_api_version = message.get_i16();
        // 3. Read correlation_id (4 bytes, big-endian): 6f 7f c6 61 | (1870644833)
        let correlation_id = message.get_i32();

        // 4. Read client_id as a compact string
        // let client_id = read_compact_string(message);

        Self {
            request_api_key: ApiKey::from_int(request_api_key),
            request_api_version,
            correlation_id,
        }
    }
}
