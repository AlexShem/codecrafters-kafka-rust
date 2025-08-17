#[derive(Debug)]
pub enum ApiKey {
    ApiVersions,
    DescribeTopicPartitions,
    Unsupported,
}

impl ApiKey {
    pub fn from_int(code: i16) -> Self {
        match code {
            18 => Self::ApiVersions,
            75 => Self::DescribeTopicPartitions,
            _ => Self::Unsupported,
        }
    }

    pub fn to_int(&self) -> i16 {
        match self {
            ApiKey::ApiVersions => 18,
            ApiKey::DescribeTopicPartitions => 75,
            ApiKey::Unsupported => 0,
        }
    }
}
