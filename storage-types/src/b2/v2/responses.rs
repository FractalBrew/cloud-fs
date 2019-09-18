// Copyright 2019 Dave Townsend
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use super::{BucketType, FileAction, Int, Map, UserFileInfo};

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
    pub action: FileAction,
    pub bucket_id: String,
    pub content_length: Int,
    pub content_sha1: Option<String>,
    pub content_type: Option<String>,
    pub file_id: Option<String>,
    pub file_info: UserFileInfo,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteFileVersionResponse {
    pub file_name: String,
    pub file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadUrlResponse {
    pub bucket_id: String,
    pub upload_url: String,
    pub authorization_token: String,
}

pub type UploadFileResponse = FileInfo;

pub type StartLargeFileResponse = FileInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadPartUrlResponse {
    pub file_id: String,
    pub upload_url: String,
    pub authorization_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPartResponse {
    pub file_id: String,
    pub part_number: usize,
    pub content_length: Int,
    pub content_sha1: String,
    pub upload_timestamp: Int,
}

pub type FinishLargeFileResponse = FileInfo;
