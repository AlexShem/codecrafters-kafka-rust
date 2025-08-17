use crate::api_keys::ApiKey;
use anyhow::{anyhow, bail, Result};
use bytes::{Buf, Bytes, BytesMut};
use std::io::{BufReader, Read};
use std::net::TcpStream;

pub struct Request {
    pub request_header: RequestHeader,
    pub request_body: RequestBody,
}

pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    #[allow(unused)]
    pub client_id: String,
}

pub enum RequestBody {
    #[allow(unused)]
    ApiVersions {
        client_id: String,
        client_software_version: String,
    },
    #[allow(unused)]
    DescribeTopicPartitions {
        topics: Vec<String>,
        response_partition_limit: i32,
        cursor: u8,
    },
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
        let request_header = RequestHeader::parse_request_header(&mut message)?;

        // 3. Read the request body
        let request_body = RequestBody::parse_request_body(&mut message, &request_header)?;

        Ok(Self {
            request_header,
            request_body,
        })
    }
}

impl RequestHeader {
    fn parse_request_header(message: &mut Bytes) -> Result<Self> {
        // 1. Read request_api_key (2 bytes, big-endian): 00 12 | (18)
        let request_api_key = message.get_i16();

        // 2. Read request_api_version (2 bytes, big-endian): 00 04 | (4)
        let request_api_version = message.get_i16();

        // 3. Read correlation_id (4 bytes, big-endian): 6f 7f c6 61 | (1870644833)
        let correlation_id = message.get_i32();

        // 4. Read client_id as a string
        let client_id_len = message.get_i16();
        let mut client_id = Vec::with_capacity(client_id_len as usize);
        for _ in 0..client_id_len {
            client_id.push(message.get_u8());
        }

        // 5. Consume the tag buffer
        match message.get_u8() {
            0x00 => Ok(Self {
                api_key: ApiKey::from_int(request_api_key),
                api_version: request_api_version,
                correlation_id,
                client_id: String::from_utf8_lossy(client_id.as_slice()).to_string(),
            }),
            tag_buf => Err(anyhow!(
                "Error parsing the request header: 0x00 tag buffer was expected. Got: {:x}",
                tag_buf
            )),
        }
    }
}

impl RequestBody {
    pub fn parse_request_body(message: &mut Bytes, request_header: &RequestHeader) -> Result<Self> {
        match request_header.api_key {
            ApiKey::ApiVersions => Self::parse_api_versions_body(message),
            ApiKey::DescribeTopicPartitions => Self::parse_describe_topic_partitions(message),
            ApiKey::Unsupported => Err(anyhow!("Trying to parse unsupported request")),
        }
    }

    fn parse_api_versions_body(message: &mut Bytes) -> Result<RequestBody> {
        // 1. Client ID as a compact string
        let client_id = match read_uvarint(message) {
            Ok(client_id_len) => {
                // Read the client_id
                let mut buf = Vec::with_capacity(client_id_len as usize - 1);
                for _ in 0..(client_id_len - 1) {
                    buf.push(message.get_u8());
                }
                String::from_utf8_lossy(buf.as_slice()).to_string()
            }
            Err(e) => {
                return Err(anyhow!("Failed to read client_id: {}", e));
            }
        };

        // 2. Client Software version as compact string
        let client_software_version = match read_uvarint(message) {
            Ok(client_software_version_len) => {
                // Read the client_software_version
                let mut buf = Vec::with_capacity(client_software_version_len as usize - 1);
                for _ in 0..(client_software_version_len - 1) {
                    buf.push(message.get_u8());
                }
                String::from_utf8_lossy(buf.as_slice()).to_string()
            }
            Err(e) => {
                return Err(anyhow!("Failed to read client_software_version: {}", e));
            }
        };

        // 3. Consume the tag buffer
        match message.get_u8() {
            0x00 => Ok(RequestBody::ApiVersions { client_id, client_software_version }),
            tag_buf => Err(anyhow!(
                "Error parsing the ApiVersions request body: 0x00 tag buffer was expected. Got: {:x}",
                tag_buf
            )),
        }
    }

    fn parse_describe_topic_partitions(message: &mut Bytes) -> Result<RequestBody> {
        // 1. Parse topics array as compact array
        let mut topics = Vec::new();
        match read_uvarint(message) {
            Ok(array_len) => {
                for _ in 0..(array_len - 1) {
                    topics.push(Self::parse_topic(message)?);
                }
            }
            Err(e) => {
                return Err(anyhow!("Failed to read client_id: {}", e));
            }
        };

        // 2. Read Response Partition Limit
        let response_partition_limit = message.get_i32();

        // 3. Read the cursor (assume nullable byte 0xFF)
        let cursor = message.get_u8();
        if cursor != 0xFF {
            return Err(anyhow!("Expected cursor to be 0xFF. Got: {:x}", cursor));
        }

        // 4. Consume the tag buffer
        match message.get_u8() {
            0x00 => Ok(RequestBody::DescribeTopicPartitions { topics, response_partition_limit, cursor }),
            tag_buf => Err(anyhow!(
                "Error parsing the ApiVersions request body: 0x00 tag buffer was expected. Got: {:x}",
                tag_buf
            )),
        }
    }

    fn parse_topic(message: &mut Bytes) -> Result<String> {
        // Topic is encoded as a compact string
        let topic_name: String = match read_uvarint(message) {
            Ok(topic_name_length) => {
                let mut buf = Vec::with_capacity(topic_name_length as usize - 1);
                // message.copy_to_slice(&mut buf);
                for _ in 0..(topic_name_length - 1) {
                    buf.push(message.get_u8());
                }
                String::from_utf8_lossy(buf.as_slice()).to_string()
            }
            Err(e) => {
                return Err(anyhow!("Failed to read topic_name_length: {}", e));
            }
        };

        // Consume the tag buffer
        match message.get_u8() {
            0x00 => Ok(topic_name),
            tag_buf => Err(anyhow!(
                "Error parsing the topic_name: 0x00 tag buffer was expected. Got: {:x}",
                tag_buf
            )),
        }
    }
}

fn read_uvarint(message: &mut Bytes) -> Result<u32> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;

    for i in 0..5 {
        if !message.has_remaining() {
            return Err(anyhow!("unexpected EOF while reading UnsigedVarInt"));
        }

        let byte = message.get_u8();
        let payload = (byte & 0x7F) as u32;

        result |= payload << shift;

        // If Most Significant Byte (MSB) is not set, this is the final byte
        if (byte & 0x80) == 0 {
            // Overflow guard (7 * 4 = 28 => 32-28=4 more bits are possible)
            if i == 4 && payload > 0x0F {
                return Err(anyhow!("UnsigedVarInt overflow for u32"));
            }
            return Ok(result);
        }

        shift += 7;
    }

    // More than 5 bytes were required
    Err(anyhow!("UnsigedVarInt too long (overflow)"))
}
