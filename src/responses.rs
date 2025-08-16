use crate::api_keys::ApiKey;
use crate::requests::Request;
use bytes::{BufMut, BytesMut};

#[derive(Debug)]
pub struct Response {
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
}

impl Response {
    pub fn new(request: Request) -> Self {
        let version = request.request_header.request_api_version;
        let error_code = if version <= 4 && version >= 0 { 0 } else { 35 };

        Self {
            correlation_id: request.request_header.correlation_id,
            error_code,
            api_keys: vec![ApiKey::from_int(18), ApiKey::from_int(75)],
        }
    }

    pub fn to_bytes(&self) -> bytes::Bytes {
        let mut body_buf = BytesMut::new();

        // 2. Response header: correlation_id (4 bytes)
        body_buf.extend_from_slice(&self.correlation_id.to_be_bytes());

        // 3. Response body
        // 3.1 Error code (2 bytes)
        body_buf.extend_from_slice(&self.error_code.to_be_bytes());

        // 3.2 ApiVersion Compact Array
        // One byte to indicate the `array_length + 1`
        write_uvarint(&mut body_buf, (self.api_keys.len() as u32) + 1);

        for api_key in &self.api_keys {
            match api_key {
                ApiKey::ApiVersions {
                    min_version,
                    max_version,
                } => {
                    body_buf.extend_from_slice(&api_key.to_int().to_be_bytes());
                    body_buf.extend_from_slice(&min_version.to_be_bytes());
                    body_buf.extend_from_slice(&max_version.to_be_bytes());
                    write_uvarint(&mut body_buf, 0);
                }
                ApiKey::DescribeTopicPartitions {
                    min_version,
                    max_version,
                } => {
                    body_buf.extend_from_slice(&api_key.to_int().to_be_bytes());
                    body_buf.extend_from_slice(&min_version.to_be_bytes());
                    body_buf.extend_from_slice(&max_version.to_be_bytes());
                    write_uvarint(&mut body_buf, 0);
                }
                ApiKey::Unsupported => {
                    write_uvarint(&mut body_buf, 0);
                }
            }
        }

        // 3.3 Throttle time (4 byte)
        body_buf.extend_from_slice(&0_i32.to_be_bytes()); // 0 ms

        // 3.4 Tag buffer
        write_uvarint(&mut body_buf, 0);

        // 1. Message size (4 bytes)
        let body = body_buf.freeze();
        let mut out = BytesMut::with_capacity(4 + body.len());
        dbg!(&body.len());
        dbg!(&body);
        out.extend_from_slice(&(body.len() as i32).to_be_bytes());
        out.extend_from_slice(&body);

        out.freeze()
    }
}

fn write_uvarint(buf: &mut BytesMut, mut val: u32) {
    loop {
        if val < 0x80 {
            buf.put_u8(val as u8);
            break;
        }
        buf.put_u8(((val as u8) & 0x7F) | 0x80);
        val >>= 7;
    }
}
