use async_std::sync::Mutex;
use std::cell::RefCell;
use std::convert::TryInto;
use zenoh_flow::serde::{Deserialize, Serialize};
use zenoh_flow::zenoh_flow_derive::{ZFData, ZFState};
use zenoh_flow::{ZFDataTrait, ZFDeserializable, ZFError, ZFResult};
// We may want to provide some "built-in" types

#[derive(Debug, Clone, ZFData)]
pub struct ZFString(pub String);

impl ZFDataTrait for ZFString {
    fn try_serialize(&self) -> zenoh_flow::ZFResult<Vec<u8>> {
        Ok(self.0.as_bytes().to_vec())
    }
}

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

impl ZFDeserializable<&[u8]> for ZFString {
    fn try_deserialize(value: &[u8]) -> ZFResult<ZFString>
    where
        Self: Sized,
    {
        Ok(ZFString(
            String::from_utf8(value.to_vec()).map_err(|_| ZFError::DeseralizationError)?,
        ))
    }
}

#[derive(Debug, Clone, ZFData)]
pub struct ZFUsize(pub usize);

impl ZFDataTrait for ZFUsize {
    fn try_serialize(&self) -> ZFResult<Vec<u8>> {
        Ok(self.0.to_ne_bytes().to_vec())
    }
}

impl ZFDeserializable<&[u8]> for ZFUsize {
    fn try_deserialize(value: &[u8]) -> ZFResult<Self>
    where
        Self: Sized,
    {
        let value =
            usize::from_ne_bytes(value.try_into().map_err(|_| ZFError::DeseralizationError)?);
        Ok(ZFUsize(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ZFState)]
pub struct ZFEmptyState;

#[derive(Serialize, Deserialize, Debug, Clone, ZFData)]
pub struct RandomData {
    pub d: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, ZFData)]
pub struct ZFBytes {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, ZFData)]
pub struct ZFOpenCVBytes {
    #[serde(skip_serializing, skip_deserializing)]
    pub bytes: Mutex<RefCell<opencv::types::VectorOfu8>>,
}

// #[derive(Debug, ZFData)]
// pub struct OpenCVMat {
//     pub mat: Mutex<RefCell<opencv::prelude::Mat>>,
// }
