#[derive(Debug)]
pub enum ApiKey {
    ApiVersions { min_version: i16, max_version: i16 },
    DescribeTopicPartitions { min_version: i16, max_version: i16 },
    Unsupported,
}

impl ApiKey {
    pub fn from_int(code: i16) -> Self {
        match code {
            18 => Self::ApiVersions {
                min_version: 0,
                max_version: 4,
            },
            75 => Self::DescribeTopicPartitions {
                min_version: 0,
                max_version: 0,
            },
            _ => Self::Unsupported,
        }
    }

    pub fn to_int(&self) -> i16 {
        match self {
            ApiKey::ApiVersions { .. } => 18,
            ApiKey::DescribeTopicPartitions { .. } => 75,
            ApiKey::Unsupported => 0,
        }
    }
}
