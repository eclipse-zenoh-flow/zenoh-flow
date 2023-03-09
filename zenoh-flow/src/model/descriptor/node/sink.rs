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

use crate::prelude::PortId;
use crate::types::{Configuration, NodeId};
use crate::zferror;
use crate::zfresult::{ErrorKind, ZFResult as Result};
use serde::{Deserialize, Serialize};

/// Describes a sink.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libgeneric_sink.so
/// configuration:
///   file: /tmp/generic-sink.txt
/// inputs: [Data]
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortId>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for SinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl SinkDescriptor {
    /// Creates a new `SinkDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SinkDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}
