use serde::{Deserialize, Serialize};

pub use super::BucketType;
use crate::{JSInt as Int, JSMap as Map};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub status: Int,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizeAccountAllowed {
    pub capabilities: Vec<String>,
    pub bucket_id: Option<String>,
    pub bucket_name: Option<String>,
    pub name_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizeAccountResponse {
    pub account_id: String,
    pub authorization_token: String,
    pub allowed: AuthorizeAccountAllowed,
    pub api_url: String,
    pub download_url: String,
    pub recommended_part_size: Int,
    pub absolute_minimum_part_size: Int,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleRule {
    pub days_from_hiding_to_deleting: Option<Int>,
    pub days_from_uploading_to_hiding: Option<Int>,
    pub file_name_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorsRule {
    cors_rule_name: String,
    allowed_origins: Vec<String>,
    allowed_operations: Vec<String>,
    allowed_headers: Option<Vec<String>>,
    expose_headers: Option<Vec<String>>,
    max_age_seconds: Int,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub account_id: String,
    pub bucket_id: String,
    pub bucket_name: String,
    pub bucket_type: BucketType,
    pub bucket_info: Map,
    pub cors_rules: Vec<CorsRule>,
    pub lifecycle_rules: Vec<LifecycleRule>,
    pub revision: Int,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponse {
    pub buckets: Vec<Bucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub account_id: String,
    pub action: String,
    pub bucket_id: String,
    pub content_length: Int,
    pub content_sha1: Option<String>,
    pub content_type: Option<String>,
    pub file_id: Option<String>,
    pub file_info: Map,
    pub file_name: String,
    pub upload_timestamp: Int,
}

impl PartialEq for FileInfo {
    fn eq(&self, other: &FileInfo) -> bool {
        self.account_id == other.account_id
            && self.bucket_id == other.bucket_id
            && self.file_id == other.file_id
            && self.file_name == other.file_name
    }
}

impl Eq for FileInfo {}

pub type GetFileInfoResponse = FileInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileNamesResponse {
    pub files: Vec<FileInfo>,
    pub next_file_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileVersionsResponse {
    pub files: Vec<FileInfo>,
    pub next_file_name: Option<String>,
    pub next_file_id: Option<String>,
}
