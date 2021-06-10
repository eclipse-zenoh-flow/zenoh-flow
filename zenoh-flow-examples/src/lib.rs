use async_std::sync::Mutex;
use std::cell::RefCell;
use zenoh_flow::serde::{Deserialize, Serialize};
use zenoh_flow::zenoh_flow_macros::{ZFData, ZFState};
// We may want to provide some "built-in" types
#[derive(Debug, Clone, ZFData)]
pub struct ZFString(String);

impl From<String> for ZFString {
    fn from(s: String) -> Self {
        ZFString(s)
    }
}

impl From<&str> for ZFString {
    fn from(s: &str) -> Self {
        ZFString(s.to_owned())
    }
}

#[derive(Debug, Clone, ZFState)]
pub struct ZFEmptyState;

#[derive(Serialize, Deserialize, Debug, Clone, ZFData)]
pub struct RandomData {
    pub d: u128,
}

#[derive(Serialize, Deserialize, Debug, Clone, ZFData)]
pub struct ZFBytes {
    pub bytes: Vec<u8>,
}

#[derive(Debug, ZFData)]
pub struct ZFOpenCVBytes {
    pub bytes: Mutex<RefCell<opencv::types::VectorOfu8>>,
}
