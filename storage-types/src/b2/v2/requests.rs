use serde::{Deserialize, Serialize};

use super::BucketTypes;
use crate::JSInt as Int;

pub const B2_API_HOST: &str = "https://api.backblazeb2.com";
pub const B2_VERSION: &str = "v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsRequest {
    pub account_id: String,
    pub bucket_id: Option<String>,
    pub bucket_name: Option<String>,
    pub bucket_types: BucketTypes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFileInfoRequest {
    pub file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileNamesRequest {
    pub bucket_id: String,
    pub start_file_name: Option<String>,
    pub max_file_count: Option<Int>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFileVersionsRequest {
    pub bucket_id: String,
    pub start_file_name: Option<String>,
    pub start_file_id: Option<String>,
    pub max_file_count: Option<Int>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteFileVersionRequest {
    pub file_name: String,
    pub file_id: String,
}
