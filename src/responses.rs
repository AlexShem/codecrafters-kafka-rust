use crate::api_keys::ApiKey;
use crate::requests::{Request, RequestBody, RequestHeader};
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response {
    pub response_header: ResponseHeader,
    pub response_body: ResponseBody,
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
    pub tag_buffer: Option<u8>,
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersions {
        error_code: i16,
        api_versions: Vec<ApiVersionDetails>,
        throttle_time: i32,
    },
    DescribeTopicPartitions {
        throttle_time: i32,
        topics: Vec<DescribeTopicPartitionsDetails>,
        next_cursor: u8,
    },
}

#[derive(Debug)]
pub struct ApiVersionDetails {
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsDetails {
    error_code: i16,
    topic_name: String,
    topic_id: u128,
    is_internal: bool,
    #[allow(unused)]
    partitions_array: Vec<String>,
    topic_authorized_operations: u32,
}

impl Response {
    pub fn new(request: Request) -> Self {
        let response_header = ResponseHeader::from_request_header(&request.request_header);

        let response_body: ResponseBody = ResponseBody::from_request_body(
            &request.request_body,
            request.request_header.api_version,
        );

        Self {
            response_header,
            response_body,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut body_buf = BytesMut::new();

        // 2. Response header: correlation_id (4 bytes)
        body_buf.extend_from_slice(&self.response_header.to_be_bytes());

        // 3. Response body
        body_buf.extend_from_slice(&self.response_body.to_be_bytes());

        // 1. Message size (4 bytes)
        let body = body_buf.freeze();
        let mut out = BytesMut::with_capacity(4 + body.len());
        out.extend_from_slice(&(body.len() as i32).to_be_bytes());
        out.extend_from_slice(&body);

        out.freeze()
    }
}

impl ResponseHeader {
    fn from_request_header(request_header: &RequestHeader) -> Self {
        let tag_buffer = match request_header.api_key {
            ApiKey::ApiVersions => None,
            ApiKey::DescribeTopicPartitions => Some(0x00),
            ApiKey::Unsupported => Some(0x00),
        };
        Self {
            correlation_id: request_header.correlation_id,
            tag_buffer,
        }
    }

    fn to_be_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32(self.correlation_id);
        if let Some(_tag) = self.tag_buffer {
            buf.put_u8(0);
        }
        buf.freeze()
    }
}

impl ResponseBody {
    fn from_request_body(request_body: &RequestBody, version: i16) -> Self {
        match request_body {
            RequestBody::ApiVersions { .. } => {
                let error_code = if version <= 4 && version >= 0 { 0 } else { 35 };
                let mut api_versions: Vec<ApiVersionDetails> = Vec::with_capacity(2);

                // Fill in supported ApiVersions
                api_versions.push(ApiVersionDetails {
                    api_key: ApiKey::ApiVersions,
                    min_version: 0,
                    max_version: 4,
                });
                api_versions.push(ApiVersionDetails {
                    api_key: ApiKey::DescribeTopicPartitions,
                    min_version: 0,
                    max_version: 0,
                });

                let throttle_time: i32 = 0;

                Self::ApiVersions {
                    error_code,
                    api_versions,
                    throttle_time,
                }
            }
            RequestBody::DescribeTopicPartitions { topics, .. } => {
                let throttle_time: i32 = 0;

                let mut requested_topics: Vec<DescribeTopicPartitionsDetails> = Vec::new();
                // Manually add the topics
                requested_topics.push(DescribeTopicPartitionsDetails {
                    error_code: 3,
                    topic_name: topics.get(0).unwrap().to_string(),
                    topic_id: 0,
                    is_internal: false,
                    partitions_array: vec![],
                    topic_authorized_operations: 0x00000df8,
                });

                let next_cursor: u8 = 0xFF;

                Self::DescribeTopicPartitions {
                    throttle_time,
                    topics: requested_topics,
                    next_cursor,
                }
            }
        }
    }

    fn to_be_bytes(&self) -> Bytes {
        let mut body_buf = BytesMut::new();
        match self {
            ResponseBody::ApiVersions {
                error_code,
                api_versions,
                throttle_time,
            } => {
                // 1. Error code (2 bytes)
                body_buf.put_i16(*error_code);

                // 2. ApiVersion Compact Array
                // One byte to indicate the `array_length + 1`
                write_uvarint(&mut body_buf, (api_versions.len() as u32) + 1);

                for api_version in api_versions {
                    body_buf.extend_from_slice(&api_version.api_key.to_int().to_be_bytes());
                    body_buf.extend_from_slice(&api_version.min_version.to_be_bytes());
                    body_buf.extend_from_slice(&api_version.max_version.to_be_bytes());
                    write_uvarint(&mut body_buf, 0);
                }

                // 3.3 Throttle time (4 byte)
                body_buf.extend_from_slice(&throttle_time.to_be_bytes());

                // 3.4 Tag buffer
                write_uvarint(&mut body_buf, 0);
            }
            ResponseBody::DescribeTopicPartitions {
                throttle_time,
                topics,
                next_cursor,
            } => {
                // 1. Throttle time
                body_buf.extend_from_slice(&throttle_time.to_be_bytes());

                // 2. Topics array
                write_uvarint(&mut body_buf, (topics.len() as u32) + 1);
                for topic in topics {
                    body_buf.extend_from_slice(&topic.error_code.to_be_bytes());

                    // Topic name as compact string
                    write_uvarint(&mut body_buf, (topic.topic_name.len() as u32) + 1);
                    body_buf.extend_from_slice(topic.topic_name.as_bytes());

                    // Topic ID: 16-byte uuid
                    body_buf.extend_from_slice(&topic.topic_id.to_be_bytes());

                    // Is Internal
                    body_buf.put_u8(topic.is_internal as u8);

                    // Partition array: compact array
                    write_uvarint(&mut body_buf, 1); // 1 means it's empty

                    // Topic authorised Operations
                    body_buf.put_u32(topic.topic_authorized_operations);

                    // Tag buffer
                    write_uvarint(&mut body_buf, 0);
                }

                // 3. Next cursor
                body_buf.put_u8(*next_cursor);

                // 4. Tag buffer
                write_uvarint(&mut body_buf, 0);
            }
        };

        body_buf.freeze()
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
