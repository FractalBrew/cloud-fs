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

use super::{BucketTypes, Int, UserFileInfo};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadUrlRequest {
    pub bucket_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartLargeFileRequest {
    pub bucket_id: String,
    pub file_name: String,
    pub content_type: String,
    pub file_info: Option<UserFileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetUploadPartUrlRequest {
    pub file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinishLargeFileRequest {
    pub file_id: String,
    pub part_sha1_array: Vec<String>,
}
