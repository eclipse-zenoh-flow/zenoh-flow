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

pub(crate) mod io;
pub use io::*;
pub(crate) mod message;
pub use message::*;
pub(crate) mod context;
pub use context::*;
pub(crate) mod configuration;
pub use configuration::Configuration;

use crate::traits::ZFData;
use crate::types::io::{CallbackInput, CallbackOutput};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::sync::Arc;

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
/// Special port type that matches any other port type.
pub(crate) const PORT_TYPE_ANY: &str = "_any_";

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
    pub fn try_as_bytes(&self) -> Result<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|e| zferror!(ErrorKind::SerializationError, "{:?}", e))?;
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
    pub fn try_get<Typed>(&mut self) -> Result<&Typed>
    where
        Typed: ZFData + crate::traits::Deserializable + 'static,
    {
        *self = (match &self {
            Self::Bytes(bytes) => {
                let data: Arc<dyn ZFData> = Arc::new(
                    Typed::try_deserialize(bytes.as_slice())
                        .map_err(|e| zferror!(ErrorKind::DeseralizationError, "{:?}", e))?,
                );
                Ok(Self::Typed(data.clone()))
            }
            Self::Typed(typed) => Ok(Self::Typed(typed.clone())),
        } as Result<Self>)?;

        match self {
            Self::Typed(typed) => Ok(typed
                .as_any()
                .downcast_ref::<Typed>()
                .ok_or_else(|| zferror!(ErrorKind::InvalidData, "Could not downcast"))?),
            _ => Err(zferror!(ErrorKind::InvalidData, "Should be deserialized first").into()),
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
