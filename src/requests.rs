use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use std::io::{BufReader, Read};
use std::net::TcpStream;

#[allow(unused)]
pub struct Request {
    pub message_size: i32,
    pub request_api_key: i16,
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

        let mut body = BytesMut::with_capacity(message_size as usize);
        body.resize(message_size as usize, 0);
        reader.read_exact(&mut body)?;
        let mut header = body.freeze();

        //2. Read request_api_key (2 bytes, big-endian): 00 12 | (18)
        let request_api_key = header.get_i16();
        // 3. Read request_api_version (2 bytes, big-endian): 00 04 | (4)
        let request_api_version = header.get_i16();
        // 4. Read correlation_id (4 bytes, big-endian): 6f 7f c6 61 | (1870644833)
        let correlation_id = header.get_i32();

        Ok(Self {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
        })
    }
}
