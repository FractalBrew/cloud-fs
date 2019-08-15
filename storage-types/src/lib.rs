pub mod b2;

pub use serde_json::Value;
pub type JSMap = serde_json::Map<String, Value>;
pub type JSInt = u64;
