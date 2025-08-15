use crate::api_keys::ApiKey;
use crate::requests::Request;

pub struct Response {
    #[allow(unused)]
    pub message_size: i32,
    pub correlation_id: i32,
    pub error_code: i16,
    pub api_keys: Vec<ApiKey>,
}

impl Response {
    pub fn generate_response(request: Request) -> bytes::Bytes {
        let version = request.request_header.request_api_version;
        let error_code = if version <= 4 && version >= 0 { 0 } else { 35 };

        let response = Self {
            message_size: 0,
            correlation_id: request.request_header.correlation_id,
            error_code,
            api_keys: vec![ApiKey::from_int(18)],
        };

        response.to_bytes()
    }

    pub fn to_bytes(&self) -> bytes::Bytes {
        // let mut byte_buf = bytes::BytesMut::with_capacity(self.message_size as usize + 4);
        let mut byte_buf = bytes::BytesMut::new();

        // 2. Response header: correlation_id (4 bytes)
        byte_buf.extend_from_slice(&self.correlation_id.to_be_bytes());

        // 3. Response body
        // 3.1 Error code (2 bytes)
        byte_buf.extend_from_slice(&self.error_code.to_be_bytes());

        // 3.2 ApiVersion Compact Array
        // One byte to indicate the `array_length + 1`
        byte_buf.extend_from_slice(&(1_i8 + 1_i8).to_be_bytes());

        for api_key in &self.api_keys {
            match api_key {
                ApiKey::ApiVersions {
                    min_version,
                    max_version,
                } => {
                    byte_buf.extend_from_slice(&api_key.to_int().to_be_bytes());
                    byte_buf.extend_from_slice(&min_version.to_be_bytes());
                    byte_buf.extend_from_slice(&max_version.to_be_bytes());
                    byte_buf.extend_from_slice(&0_i8.to_be_bytes()); // 0 tag buffer
                }
                ApiKey::Unsupported => {
                    byte_buf.extend_from_slice(&0_i8.to_be_bytes()); // 0 tag buffer
                }
            }
        }

        // 3.3 Throttle time (4 byte)
        byte_buf.extend_from_slice(&0_i32.to_be_bytes()); // 0 ms

        // 3.4 Tag buffer
        byte_buf.extend_from_slice(&0_i8.to_be_bytes()); // 0 tag buffer

        // 1. Message size (4 bytes)
        let body = byte_buf.freeze();
        let message_size = body.len();
        let mut response_bytes = bytes::BytesMut::new();
        response_bytes.extend_from_slice(&(message_size as i32).to_be_bytes());
        response_bytes.extend_from_slice(&body);

        response_bytes.freeze()
    }
}
