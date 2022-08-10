//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use crate::async_std::sync::Arc;
use crate::error::ZFError;
use crate::runtime::dataflow::instance::io::{AsyncCallbackReceiver, AsyncCallbackSender};
use crate::runtime::InstanceContext;
use crate::serde::{Deserialize, Serialize};
use crate::traits::ZFData;
use std::convert::From;
use std::ops::Deref;
use std::time::Duration;

/// A NodeId identifies a node inside a Zenoh Flow graph
pub type NodeId = Arc<str>;
/// A PortId identifies a port within an node.
pub type PortId = Arc<str>;
/// A RuntimeId identifies a runtime within the Zenoh Flow infrastructure.
pub type RuntimeId = Arc<str>;
/// A FlowId identifies a Zenoh Flow graph within Zenoh Flow
pub type FlowId = Arc<str>;
/// The PortType identifies the type of the data expected in a port.
pub type PortType = Arc<str>;

/// The Zenoh Flow result type.
pub type ZFResult<T> = Result<T, ZFError>;

/// Context is a structure provided by Zenoh Flow to access the execution context directly from the
/// nodes. It contains the `mode` as usize.
pub struct Context {
    instance: Arc<InstanceContext>,
    pub(crate) callback_receivers: Vec<AsyncCallbackReceiver>,
    pub(crate) callback_senders: Vec<AsyncCallbackSender>,
}

impl Context {
    pub(crate) fn new(instance_context: Arc<InstanceContext>) -> Self {
        Self {
            instance: instance_context,
            callback_receivers: vec![],
            callback_senders: vec![],
        }
    }
}

impl Deref for Context {
    type Target = InstanceContext;

    fn deref(&self) -> &Self::Target {
        &self.instance
    }
}

/// The Zenoh Flow data.
/// It is an `enum` that can contain both the serialized data (if received from
/// the network, or from nodes not written in Rust),
/// or the actual `Typed` data as [`ZFData`](`ZFData`).
/// The `Typed` data is never serialized directly when sending over Zenoh
/// or to an operator not written in Rust.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    /// Serialized data, coming either from Zenoh of from non-rust node.
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    /// Actual data as instance of 'ZFData` coming from a Rust node.
    /// This is never serialized directly.
    Typed(Arc<dyn ZFData>),
}

impl Data {
    /// Tries to return a serialized representation of the data.
    /// It does not actually change the internal representation.
    /// The serialized representation in stored inside an `Arc`
    /// to avoid copies.
    ///
    /// # Errors
    /// If it fails to serialize an error
    /// variant will be returned.
    pub fn try_as_bytes(&self) -> ZFResult<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|_| ZFError::SerializationError)?;
                Ok(Arc::new(serialized_data))
            }
        }
    }

    /// Tries to cast the data to the given type.
    /// If the data is represented as serialized, this will try to deserialize
    /// the bytes and change the internal representation of the data.
    /// If the data is already represented with as `Typed` then it will
    /// return an *immutable* reference to the internal data.
    /// This reference is *immutable* because one Output can send data to
    /// multiple Inputs, therefore to avoid copies the same `Arc` is sent
    /// to multiple operators, thus it is multiple-owned and the data inside
    /// cannot be modified.
    ///
    /// # Errors
    /// If fails to cast an error
    /// variant will be returned.
    pub fn try_get<Typed>(&mut self) -> ZFResult<&Typed>
    where
        Typed: ZFData + crate::traits::Deserializable + 'static,
    {
        *self = (match &self {
            Self::Bytes(bytes) => {
                let data: Arc<dyn ZFData> = Arc::new(
                    Typed::try_deserialize(bytes.as_slice())
                        .map_err(|_| crate::types::ZFError::DeseralizationError)?,
                );
                Ok(Self::Typed(data.clone()))
            }
            Self::Typed(typed) => Ok(Self::Typed(typed.clone())),
        } as ZFResult<Self>)?;

        match self {
            Self::Typed(typed) => Ok(typed
                .as_any()
                .downcast_ref::<Typed>()
                .ok_or_else(|| ZFError::InvalidData("Could not downcast.".to_string()))?),
            _ => Err(ZFError::InvalidData(
                "Should be deserialized first".to_string(),
            )),
        }
    }
}

/// Creates a Data from an `Arc` of  typed data.
/// The typed data has to be an instance of `ZFData`.
impl<UT> From<Arc<UT>> for Data
where
    UT: ZFData + 'static,
{
    fn from(data: Arc<UT>) -> Self {
        Self::Typed(data)
    }
}

/// Creates a Data from  typed data.
/// The typed data has to be an instance of `ZFData`.
/// The data is then stored inside an `Arc` to avoid copies.
impl<UT> From<UT> for Data
where
    UT: ZFData + 'static,
{
    fn from(data: UT) -> Self {
        Self::Typed(Arc::new(data))
    }
}

/// Creates a new `Data` from a `Arc<Vec<u8>>`.
impl From<Arc<Vec<u8>>> for Data {
    fn from(bytes: Arc<Vec<u8>>) -> Self {
        Self::Bytes(bytes)
    }
}

/// Creates a new `Data` from a `Vec<u8>`,
/// In order to avoid copies it puts the data inside an `Arc`.
impl From<Vec<u8>> for Data {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }
}

/// Creates a new `Data` from a `&[u8]`.
impl From<&[u8]> for Data {
    fn from(bytes: &[u8]) -> Self {
        Self::Bytes(Arc::new(bytes.to_vec()))
    }
}

/// The generic configuration of a graph node.
/// It is a re-export of `serde_json::Value`
pub type Configuration = serde_json::Value;

/// Merges two configurations, keeping the values of the `local` Configuration in case of duplicated
/// keys.
///
/// This function was created with the idea of merging (and overwriting) a `global` Configuration,
/// common to all nodes of a graph, with a `local` one, i.e. specific to a node.
pub(crate) fn merge_configurations(
    global: Option<Configuration>,
    local: Option<Configuration>,
) -> Option<Configuration> {
    match (global, local) {
        (None, None) => None,
        (None, Some(local)) => Some(local),
        (Some(global), None) => Some(global),
        (Some(mut global), Some(mut local)) => {
            global
                .as_object_mut()
                .unwrap()
                .append(local.as_object_mut().unwrap());
            Some(global)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::merge_configurations;
    use serde_json::json;

    #[test]
    fn test_merge_configurations() {
        let global = json!({ "a": { "nested": true }, "b": ["an", "array"] });
        let local = json!({ "a": { "not-nested": false }, "c": 1 });

        assert_eq!(
            merge_configurations(Some(global.clone()), Some(local.clone())),
            Some(json!({ "a": { "not-nested": false }, "b": ["an", "array"], "c": 1 }))
        );

        assert_eq!(merge_configurations(None, Some(local.clone())), Some(local));
        assert_eq!(
            merge_configurations(Some(global.clone()), None),
            Some(global)
        );
    }
}

/// The unit of duration used in different descriptors.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DurationUnit {
    #[serde(alias = "s")]
    #[serde(alias = "second")]
    #[serde(alias = "seconds")]
    Second,
    #[serde(alias = "ms")]
    #[serde(alias = "millisecond")]
    #[serde(alias = "milliseconds")]
    Millisecond,
    #[serde(alias = "us")]
    #[serde(alias = "µs")]
    #[serde(alias = "microsecond")]
    #[serde(alias = "microseconds")]
    Microsecond,
}

/// The descriptor for a duration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DurationDescriptor {
    #[serde(alias = "duration")]
    pub(crate) length: u64,
    pub(crate) unit: DurationUnit,
}

impl DurationDescriptor {
    /// Converts the [`DurationDescriptor`](`DurationDescriptor`) to a [`Duration`](`Duration`).
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Second => Duration::from_secs(self.length),
            DurationUnit::Millisecond => Duration::from_millis(self.length),
            DurationUnit::Microsecond => Duration::from_micros(self.length),
        }
    }
}
