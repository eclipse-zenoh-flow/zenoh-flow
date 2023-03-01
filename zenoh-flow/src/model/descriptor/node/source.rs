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

use crate::model::descriptor::{
    CompositeInputDescriptor, CompositeOutputDescriptor, LinkDescriptor, LoadedNode, PortDescriptor,
};
use crate::types::{Configuration, NodeId};
use crate::zfresult::{ErrorKind, ZFResult as Result};
use crate::{bail, zferror};
use serde::{Deserialize, Serialize};

use super::NodeDescriptor;

/// Describes a source.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libcounter_source.so
/// configuration:
///   start: 10
/// output:
///   id: Counter
///   type: usize
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub id: NodeId,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for SourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

impl LoadedNode for SourceDescriptor {
    fn from_parameters(
        id: NodeId,
        configuration: Option<Configuration>,
        uri: Option<String>,
        _inputs: Option<Vec<PortDescriptor>>,
        outputs: Option<Vec<PortDescriptor>>,
        _operators: Option<Vec<NodeDescriptor>>,
        _links: Option<Vec<LinkDescriptor>>,
        _composite_inputs: Option<Vec<CompositeInputDescriptor>>,
        _compisite_outpus: Option<Vec<CompositeOutputDescriptor>>,
    ) -> Result<Self> {
        match outputs {
            Some(outputs) =>{
                Ok(Self{
                    id,
                    configuration,
                    uri,
                    outputs,
                })
            },
            _ => bail!(ErrorKind::InvalidData, "Creating a SourceDescriptor requires: id, configuration, uri, and outputs. Maybe some parameters are set as None?")
        }
    }

    fn get_id(&self) -> &NodeId {
        &self.id
    }

    fn set_id(&mut self, id: NodeId) {
        self.id = id
    }

    fn get_configuration(&self) -> &Option<Configuration> {
        &self.configuration
    }

    fn set_configuration(&mut self, configuration: Option<Configuration>) {
        self.configuration = configuration
    }

    /// Creates a new `SourceDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SourceDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SourceDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SourceDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}
