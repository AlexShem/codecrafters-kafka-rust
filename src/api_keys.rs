pub enum ApiKey {
    ApiVersions { min_version: i16, max_version: i16 },
    Unsupported,
}

impl ApiKey {
    pub fn from_int(code: i32) -> Self {
        match code {
            18 => Self::ApiVersions {
                min_version: 0,
                max_version: 4,
            },
            _ => Self::Unsupported,
        }
    }

    pub fn to_int(&self) -> i16 {
        match self {
            ApiKey::ApiVersions { .. } => 18,
            ApiKey::Unsupported => 0,
        }
    }
}
